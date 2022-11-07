// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "pipeline_fragment_context.h"

#include "exec/data_sink.h"
#include "exec/scan_node.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/result_sink_operator.h"
#include "pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "task_scheduler.h"
#include "util/container_util.hpp"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/exec/scan/new_olap_scan_node.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/vexchange_node.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/sink/vdata_stream_sender.h"
#include "vec/sink/vresult_sink.h"

namespace doris::pipeline {

PipelineFragmentContext::PipelineFragmentContext(const TUniqueId& query_id,
                                                 const TUniqueId& instance_id,
                                                 std::shared_ptr<QueryFragmentsCtx> query_ctx,
                                                 ExecEnv* exec_env)
        : _query_id(query_id),
          _fragment_instance_id(instance_id),
          _exec_env(exec_env),
          _cancel_reason(PPlanFragmentCancelReason::INTERNAL_ERROR),
          _closed_pipeline_cnt(0),
          _query_ctx(std::move(query_ctx)) {}

PipelineFragmentContext::~PipelineFragmentContext() {}

void PipelineFragmentContext::cancel(const PPlanFragmentCancelReason& reason,
                                     const std::string& msg) {
    if (!_cancelled) {
        std::lock_guard<std::mutex> l(_status_lock);
        if (_cancelled) {
            return;
        }
        _cancelled = true;
        _runtime_state->set_is_cancelled(true);
        _cancel_reason = reason;
        _cancel_msg = msg;
        _runtime_state->set_is_cancelled(true);
        // To notify wait_for_start()
        _runtime_state->get_query_fragments_ctx()->set_ready_to_execute(true);

        // must close stream_mgr to avoid dead lock in Exchange Node
        auto env = _runtime_state->exec_env();
        auto id = _runtime_state->fragment_instance_id();
        env->vstream_mgr()->cancel(id);
    }
}

PipelinePtr PipelineFragmentContext::add_pipeline() {
    // _prepared、_submitted或_canceled都不能再添加pipeline
    PipelineId id = _next_pipeline_id++;
    auto pipeline = std::make_shared<Pipeline>(id, shared_from_this());
    _pipelines.emplace_back(pipeline);
    return pipeline;
}

Status PipelineFragmentContext::prepare(const doris::TExecPlanFragmentParams& request) {
    if (_prepared) {
        return Status::InternalError("Already prepared");
    }
    auto* fragment_context = this;

    OpentelemetryTracer tracer = telemetry::get_noop_tracer();
    if (opentelemetry::trace::Tracer::GetCurrentSpan()->GetContext().IsValid()) {
        tracer = telemetry::get_tracer(print_id(_query_id));
    }
    START_AND_SCOPE_SPAN(tracer, span, "PipelineFragmentExecutor::prepare");

    const TPlanFragmentExecParams& params = request.params;

    LOG_INFO("PipelineFragmentContext::prepare")
            .tag("query_id", _query_id)
            .tag("instance_id", params.fragment_instance_id)
            .tag("backend_num", request.backend_num)
            .tag("pthread_id", (uintptr_t)pthread_self());

    // 基于量量化的算子做POC
    if (!request.query_options.__isset.enable_vectorized_engine ||
        !request.query_options.enable_vectorized_engine) {
        return Status::InternalError("should set enable_vectorized_engine to true");
    }

    // 1. 创建初始化RuntimeState
    _runtime_state = std::make_unique<RuntimeState>(params, request.query_options,
                                                    _query_ctx->query_globals, _exec_env);
    _runtime_state->set_query_fragments_ctx(_query_ctx.get());
    _runtime_state->set_tracer(std::move(tracer));

    // TODO 可以和plan_fragment_executor中的prepare部分合并
    RETURN_IF_ERROR(_runtime_state->init_mem_trackers(request.params.query_id));
    RETURN_IF_ERROR(_runtime_state->runtime_filter_mgr()->init());
    _runtime_state->set_be_number(request.backend_num);

    if (request.__isset.backend_id) {
        _runtime_state->set_backend_id(request.backend_id);
    }
    if (request.__isset.import_label) {
        _runtime_state->set_import_label(request.import_label);
    }
    if (request.__isset.db_name) {
        _runtime_state->set_db_name(request.db_name);
    }
    if (request.__isset.load_job_id) {
        _runtime_state->set_load_job_id(request.load_job_id);
    }
    if (request.__isset.load_error_hub_info) {
        _runtime_state->set_load_error_hub_info(request.load_error_hub_info);
    }

    if (request.query_options.__isset.is_report_success) {
        fragment_context->set_is_report_success(request.query_options.is_report_success);
    }

    RETURN_IF_ERROR(_runtime_state->create_block_mgr());
    auto* desc_tbl = _query_ctx->desc_tbl;
    _runtime_state->set_desc_tbl(desc_tbl);

    // 组建pipeline
    // 2. 创建ExecNode，并创建pipeline，放入PipelineFragmentContext
    RETURN_IF_ERROR(ExecNode::create_tree(_runtime_state.get(), _runtime_state->obj_pool(),
                                          request.fragment.plan, *desc_tbl, &_root_plan));
    _runtime_state->set_fragment_root_id(_root_plan->id());

    // Set senders of exchange nodes before pipeline build
    std::vector<ExecNode*> exch_nodes;
    _root_plan->collect_nodes(TPlanNodeType::EXCHANGE_NODE, &exch_nodes);
    for (ExecNode* exch_node : exch_nodes) {
        DCHECK_EQ(exch_node->type(), TPlanNodeType::EXCHANGE_NODE);
        int num_senders = find_with_default(params.per_exch_num_senders, exch_node->id(), 0);
        DCHECK_GT(num_senders, 0);
        static_cast<vectorized::VExchangeNode*>(exch_node)->set_num_senders(num_senders);
    }
    // 这里不对plan进行prepare
    // RETURN_IF_ERROR(_plan->prepare(_runtime_state.get()));

    // set scan ranges
    std::vector<ExecNode*> scan_nodes;
    std::vector<TScanRangeParams> no_scan_ranges;
    _root_plan->collect_scan_nodes(&scan_nodes);
    VLOG_CRITICAL << "scan_nodes.size()=" << scan_nodes.size();
    VLOG_CRITICAL << "params.per_node_scan_ranges.size()=" << params.per_node_scan_ranges.size();

    // 先执行，在Operator内部也许用得到
    _root_plan->try_do_aggregate_serde_improve();
    // 暂时将scan range放在ScanNode上
    for (int i = 0; i < scan_nodes.size(); ++i) {
        // TODO(cmy): this "if...else" should be removed once all ScanNode are derived from VScanNode.
        ExecNode* node = scan_nodes[i];
        if (typeid(*node) == typeid(vectorized::NewOlapScanNode) ||
            typeid(*node) == typeid(vectorized::NewFileScanNode) // ||
//            typeid(*node) == typeid(vectorized::NewOdbcScanNode) ||
//            typeid(*node) == typeid(vectorized::NewEsScanNode)
#ifdef LIBJVM
//            || typeid(*node) == typeid(vectorized::NewJdbcScanNode)
#endif
        ) {
            auto* scan_node = static_cast<vectorized::VScanNode*>(scan_nodes[i]);
            const std::vector<TScanRangeParams>& scan_ranges =
                    find_with_default(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
            scan_node->set_scan_ranges(scan_ranges);
        } else {
            ScanNode* scan_node = static_cast<ScanNode*>(scan_nodes[i]);
            const std::vector<TScanRangeParams>& scan_ranges =
                    find_with_default(params.per_node_scan_ranges, scan_node->id(), no_scan_ranges);
            scan_node->set_scan_ranges(scan_ranges);
            VLOG_CRITICAL << "scan_node_Id=" << scan_node->id() << " size=" << scan_ranges.size();
        }
    }

    // sr range转换成morsel queue,最简单的，一个morsel就是一个scan range，每个scan node对应一个mosel queue
    _runtime_state->set_per_fragment_instance_idx(params.sender_id);
    _runtime_state->set_num_per_fragment_instances(params.num_senders);

    _root_pipeline = fragment_context->add_pipeline();
    RETURN_IF_ERROR(_root_plan->constr_pipeline(fragment_context, _root_pipeline.get()));

    // 3. 如果存在sink，则将其转化为operator，并放入pipeline
    if (request.fragment.__isset.output_sink) {
        RETURN_IF_ERROR(
                DataSink::create_data_sink(_runtime_state->obj_pool(), request.fragment.output_sink,
                                           request.fragment.output_exprs, params,
                                           _root_plan->row_desc(), true, &_sink, *desc_tbl));

        // 不需要prepare
        //        RETURN_IF_ERROR(sink->prepare(_runtime_state));
        // 没有prepare，是没有profile的
        //        RuntimeProfile* sink_profile = sink->profile();
        //        if (sink_profile != nullptr) {
        //            _runtime_state->runtime_profile()->add_child(sink_profile, true, nullptr);
        //        }
        RETURN_IF_ERROR(_create_sink(request.fragment.output_sink));
    }

    for (auto& pipeline : _pipelines) {
        RETURN_IF_ERROR(pipeline->prepare(_runtime_state.get()));
    }

    for (PipelinePtr& pipeline : _pipelines) {
        // if sink
        auto sink = pipeline->sink()->build_operator();
        RETURN_IF_ERROR(sink->init(nullptr, _runtime_state.get()));
        // TODO pipeline 1 这个设计要重新考虑，把Operator和exec_node解绑，或者增加新的接口
        auto& thrift_sink = request.fragment.output_sink;
        switch (thrift_sink.type) {
        case TDataSinkType::DATA_STREAM_SINK: {
            auto* exchange_sink = dynamic_cast<ExchangeSinkOperator*>(sink.get());
            RETURN_IF_ERROR(exchange_sink->init(thrift_sink));
            break;
        }
        case TDataSinkType::RESULT_SINK: {
            auto* result_sink = dynamic_cast<ResultSinkOperator*>(sink.get());
            RETURN_IF_ERROR(result_sink->init(thrift_sink));
            break;
        }
        default:
            return Status::InternalError("Unsuported sink type in pipeline: {}", thrift_sink.type);
        }

        auto operators = pipeline->build_operators();
        _tasks.emplace_back(std::make_unique<PipelineTask>(pipeline, 0, _runtime_state.get(),
                                                           operators, sink, this));
    }

    for (auto& task : _tasks) {
        RETURN_IF_ERROR(task->prepare(_runtime_state.get()));
    }

    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::submit() {
    if (_submitted) {
        return Status::InternalError("submittd");
    }

    for (auto& task : _tasks) {
        RETURN_IF_ERROR(_exec_env->pipeline_task_scheduler()->schedule_task(task.get()));
    }
    _submitted = true;
    return Status::OK();
}

// 构造sink operator
Status PipelineFragmentContext::_create_sink(const TDataSink& thrift_sink) {
    OperatorTemplatePtr sink_;
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        auto* exchange_sink = dynamic_cast<doris::vectorized::VDataStreamSender*>(_sink.get());
        sink_ = std::make_shared<ExchangeSinkOperatorTemplate>(next_operator_template_id(), "",
                                                               nullptr, exchange_sink);
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        auto* result_sink = dynamic_cast<doris::vectorized::VResultSink*>(_sink.get());
        sink_ = std::make_shared<ResultSinkOperatorTemplate>(next_operator_template_id(), "",
                                                             nullptr, result_sink);
        break;
    }
    default:
        return Status::InternalError("Unsuported sink type in pipeline: {}", thrift_sink.type);
    }
    return _root_pipeline->set_sink(sink_);
}

void PipelineFragmentContext::close_a_pipeline() {
    ++_closed_pipeline_cnt;
    if (_closed_pipeline_cnt == _pipelines.size()) {
        _exec_env->fragment_mgr()->remove_pipeline_context(shared_from_this());
    }
}
} // namespace doris::pipeline