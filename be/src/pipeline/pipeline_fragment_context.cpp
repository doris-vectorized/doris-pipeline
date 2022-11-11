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

#include "exec/agg_context.h"
#include "exec/aggregation_sink_operator.h"
#include "exec/data_sink.h"
#include "exec/exchange_sink_operator.h"
#include "exec/exchange_source_operator.h"
#include "exec/final_aggregation_source_operator.h"
#include "exec/olap_scan_operator.h"
#include "exec/pre_aggregation_source_operator.h"
#include "exec/result_sink_operator.h"
#include "exec/scan_node.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/FrontendService.h"
#include "pipeline_task.h"
#include "runtime/client_cache.h"
#include "runtime/fragment_mgr.h"
#include "task_scheduler.h"
#include "util/container_util.hpp"
#include "util/thrift_util.h"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/exec/scan/new_olap_scan_node.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vexchange_node.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/sink/vdata_stream_sender.h"
#include "vec/sink/vresult_sink.h"

using apache::thrift::transport::TTransportException;
using apache::thrift::TException;

namespace doris::pipeline {

PipelineFragmentContext::PipelineFragmentContext(const TUniqueId& query_id,
                                                 const TUniqueId& instance_id, int backend_num,
                                                 std::shared_ptr<QueryFragmentsCtx> query_ctx,
                                                 ExecEnv* exec_env)
        : _query_id(query_id),
          _fragment_instance_id(instance_id),
          _backend_num(backend_num),
          _exec_env(exec_env),
          _cancel_reason(PPlanFragmentCancelReason::INTERNAL_ERROR),
          _closed_pipeline_cnt(0),
          _query_ctx(std::move(query_ctx)) {}

PipelineFragmentContext::~PipelineFragmentContext() = default;

void PipelineFragmentContext::cancel(const PPlanFragmentCancelReason& reason,
                                     const std::string& msg) {
    if (!_cancelled) {
        std::lock_guard<std::mutex> l(_status_lock);
        if (_cancelled) {
            return;
        }
        if (reason == PPlanFragmentCancelReason::LIMIT_REACH) {
            // TODO pipeline report on cancel
        } else {
            _exec_status = Status::Cancelled(msg);
        }
        _cancelled = true;
        _cancel_reason = reason;
        _cancel_msg = msg;
        _runtime_state->set_is_cancelled(true);
        // To notify wait_for_start()
        _runtime_state->get_query_fragments_ctx()->set_ready_to_execute(true);

        // must close stream_mgr to avoid dead lock in Exchange Node
        _exec_env->vstream_mgr()->cancel(_fragment_instance_id);
        // Cancel the result queue manager used by spark doris connector
        // TODO pipeline incomp
        // _exec_env->result_queue_mgr()->update_queue_status(id, Status::Aborted(msg));
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
    _runtime_profile.reset(new RuntimeProfile("PipelineContext"));

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

    if (request.fragment.__isset.output_sink) {
        RETURN_IF_ERROR(
                DataSink::create_data_sink(_runtime_state->obj_pool(), request.fragment.output_sink,
                                           request.fragment.output_exprs, params,
                                           _root_plan->row_desc(), true, &_sink, *desc_tbl));
    }

    _root_pipeline = fragment_context->add_pipeline();
    RETURN_IF_ERROR(_build_pipelines(_root_plan, _root_pipeline));
    if (_sink) {
        RETURN_IF_ERROR(_create_sink(request.fragment.output_sink));
    }
    RETURN_IF_ERROR(_build_pipeline_tasks(request));

    for(auto& pipeline : _pipelines) {
        _runtime_profile->add_child(pipeline->runtime_profile(), true, nullptr);
    }
    // TODO pipeline don't generate exec node's profiles
    _runtime_state->runtime_profile()->clear_children();
    _runtime_state->runtime_profile()->add_child(_runtime_profile.get(), true, nullptr);
    _prepared = true;
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipeline_tasks(
        const doris::TExecPlanFragmentParams& request) {
    for (auto& pipeline : _pipelines) {
        RETURN_IF_ERROR(pipeline->prepare(_runtime_state.get()));
    }

    for (PipelinePtr& pipeline : _pipelines) {
        // if sink
        auto sink = pipeline->sink()->build_operator();
        RETURN_IF_ERROR(sink->init(pipeline->sink()->exec_node(), _runtime_state.get()));
        // TODO pipeline 1 这个设计要重新考虑，把Operator和exec_node解绑，或者增加新的接口
        auto& sink_ = *(sink);
        if (typeid(sink_) == typeid(ExchangeSinkOperator)) {
            auto* exchange_sink = dynamic_cast<ExchangeSinkOperator*>(sink.get());
            RETURN_IF_ERROR(exchange_sink->init(request.fragment.output_sink));
        } else if (typeid(sink_) == typeid(ResultSinkOperator)) {
            auto* result_sink = dynamic_cast<ResultSinkOperator*>(sink.get());
            RETURN_IF_ERROR(result_sink->init(request.fragment.output_sink));
        }

        Operators operators;
        RETURN_IF_ERROR(pipeline->build_operators(operators));
        auto task = std::make_unique<PipelineTask>(pipeline, 0, _runtime_state.get(), operators,
                                                   sink, this, pipeline->runtime_profile());
        sink->set_child(task->get_root());
        _tasks.emplace_back(std::move(task));
    }

    for (auto& task : _tasks) {
        RETURN_IF_ERROR(task->prepare(_runtime_state.get()));
    }
    return Status::OK();
}

Status PipelineFragmentContext::_build_pipelines(ExecNode* node, PipelinePtr cur_pipe) {
    auto* fragment_context = this;
    auto node_type = node->type();
    switch (node_type) {
    // for source
    case TPlanNodeType::OLAP_SCAN_NODE: {
        // Starrocks中加了一个OlapScanPrepareOperatorFactory和NoopSinkOperatorFactory的pipeline
        // OlapScanPrepareOperatorFactory和OlapScanOperatorFactory之间靠OlapScanContextFactoryPtr联系在一起
        // OlapScanPrepareOperator和OlapScanOperator共享OlapScanContex只是为了使用OlapScanContex的标注依赖和管理声明周期的作用
        // OlapScanPrepareOperator主要用来做open tablet
        // 但中间还有一个NoopSinkOperatorFactory算子，可能是为了适配pipeline的调度而生成的
        auto* new_olap_scan_node = dynamic_cast<vectorized::NewOlapScanNode*>(node);
        if (!new_olap_scan_node) {
            return Status::InternalError("Just suppourt NewOlapScanNode in pipeline");
        }
        OperatorTemplatePtr operator_t = std::make_shared<OlapScanOperatorTemplate>(
                fragment_context->next_operator_template_id(), "OlapScanOperator",
                new_olap_scan_node);
        RETURN_IF_ERROR(cur_pipe->set_source(operator_t));
        break;
    }
    case TPlanNodeType::EXCHANGE_NODE: {
        OperatorTemplatePtr operator_t = std::make_shared<ExchangeSourceOperatorTemplate>(
                next_operator_template_id(), "ExchangeSourceOperator", node);
        RETURN_IF_ERROR(cur_pipe->set_source(operator_t));
        break;
    }
    case TPlanNodeType::AGGREGATION_NODE: {
        auto* agg_node = dynamic_cast<vectorized::AggregationNode*>(node);
        auto agg_ctx = std::make_shared<AggContext>();
        auto new_pipe = add_pipeline();
        RETURN_IF_ERROR(_build_pipelines(node->child(0), new_pipe));
        OperatorTemplatePtr agg_sink = std::make_shared<AggSinkOperatorTemplate>(
                next_operator_template_id(), "AggSinkOperator", agg_node, agg_ctx);
        RETURN_IF_ERROR(new_pipe->set_sink(agg_sink));
        if (agg_node->is_streaming_preagg()) {
            OperatorTemplatePtr agg_source = std::make_shared<PreAggSourceOperatorTemplate>(
                    next_operator_template_id(), "PAggSourceOperator", agg_node, agg_ctx);
            RETURN_IF_ERROR(cur_pipe->set_source(agg_source));
        } else {
            // 如果这里去掉依赖，要修改agg sink的can_read方法
            cur_pipe->add_dependency(new_pipe);
            OperatorTemplatePtr agg_source = std::make_shared<FinalAggSourceOperatorTemplate>(
                    next_operator_template_id(), "FinalAggSourceOperator", agg_node);
            RETURN_IF_ERROR(cur_pipe->set_source(agg_source));
        }
        break;
    }
    default:
        return Status::InternalError("Unsupported exec type in pipeline: {}",
                                     print_plan_node_type(node_type));
    }
    return Status::OK();
}

Status PipelineFragmentContext::submit() {
    if (_submitted) {
        return Status::InternalError("submitted");
    }

    for (auto& task : _tasks) {
        RETURN_IF_ERROR(_exec_env->pipeline_task_scheduler()->schedule_task(task.get()));
    }
    _submitted = true;
    return Status::OK();
}

Status PipelineFragmentContext::_create_sink(const TDataSink& thrift_sink) {
    OperatorTemplatePtr sink_;
    switch (thrift_sink.type) {
    case TDataSinkType::DATA_STREAM_SINK: {
        auto* exchange_sink = dynamic_cast<doris::vectorized::VDataStreamSender*>(_sink.get());
        sink_ = std::make_shared<ExchangeSinkOperatorTemplate>(
                next_operator_template_id(), "ExchangeSinkOperator", nullptr, exchange_sink);
        break;
    }
    case TDataSinkType::RESULT_SINK: {
        auto* result_sink = dynamic_cast<doris::vectorized::VResultSink*>(_sink.get());
        sink_ = std::make_shared<ResultSinkOperatorTemplate>(
                next_operator_template_id(), "ResultSinkOperator", nullptr, result_sink);
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
        send_report(true);
        _exec_env->fragment_mgr()->remove_pipeline_context(shared_from_this());
    }
}

// TODO pipeline dump copy from FragmentExecState::to_http_path
std::string PipelineFragmentContext::to_http_path(const std::string& file_name) {
    std::stringstream url;
    url << "http://" << BackendOptions::get_localhost() << ":" << config::webserver_port
        << "/api/_download_load?"
        << "token=" << _exec_env->token() << "&file=" << file_name;
    return url.str();
}

// TODO pipeline dump copy from FragmentExecState::coordinator_callback
// TODO pipeline this callback should be placed in a thread pool
void PipelineFragmentContext::send_report(bool done) {
    DCHECK(_closed_pipeline_cnt == _pipelines.size());
    if (!_is_report_success) {
        return;
    }

    Status exec_status = Status::OK();
    {
        std::lock_guard<std::mutex> l(_status_lock);
        if (!_exec_status.ok()) {
            exec_status = _exec_status;
        }
    }

    Status coord_status;
    auto coord_addr = _query_ctx->coord_addr;
    FrontendServiceConnection coord(_exec_env->frontend_client_cache(), coord_addr, &coord_status);
    if (!coord_status.ok()) {
        std::stringstream ss;
        ss << "couldn't get a client for " << coord_addr << ", reason: " << coord_status;
        LOG(WARNING) << "query_id: " << print_id(_query_id) << ", " << ss.str();
        {
            std::lock_guard<std::mutex> l(_status_lock);
            if (_exec_status.ok()) {
                _exec_status = Status::InternalError(ss.str());
            }
        }
        return;
    }
    auto* profile = _runtime_state->runtime_profile();

    TReportExecStatusParams params;
    params.protocol_version = FrontendServiceVersion::V1;
    params.__set_query_id(_query_id);
    params.__set_backend_num(_backend_num);
    params.__set_fragment_instance_id(_fragment_instance_id);
    exec_status.set_t_status(&params);
    params.__set_done(true);

    auto* runtime_state = _runtime_state.get();
    DCHECK(runtime_state != nullptr);
    if (runtime_state->query_type() == TQueryType::LOAD && !done && exec_status.ok()) {
        // this is a load plan, and load is not finished, just make a brief report
        params.__set_loaded_rows(runtime_state->num_rows_load_total());
        params.__set_loaded_bytes(runtime_state->num_bytes_load_total());
    } else {
        if (runtime_state->query_type() == TQueryType::LOAD) {
            params.__set_loaded_rows(runtime_state->num_rows_load_total());
            params.__set_loaded_bytes(runtime_state->num_bytes_load_total());
        }
        if (profile == nullptr) {
            params.__isset.profile = false;
        } else {
            profile->to_thrift(&params.profile);
            params.__isset.profile = true;
        }

        if (!runtime_state->output_files().empty()) {
            params.__isset.delta_urls = true;
            for (auto& it : runtime_state->output_files()) {
                params.delta_urls.push_back(to_http_path(it));
            }
        }
        if (runtime_state->num_rows_load_total() > 0 ||
            runtime_state->num_rows_load_filtered() > 0) {
            params.__isset.load_counters = true;

            static std::string s_dpp_normal_all = "dpp.norm.ALL";
            static std::string s_dpp_abnormal_all = "dpp.abnorm.ALL";
            static std::string s_unselected_rows = "unselected.rows";

            params.load_counters.emplace(s_dpp_normal_all,
                                         std::to_string(runtime_state->num_rows_load_success()));
            params.load_counters.emplace(s_dpp_abnormal_all,
                                         std::to_string(runtime_state->num_rows_load_filtered()));
            params.load_counters.emplace(s_unselected_rows,
                                         std::to_string(runtime_state->num_rows_load_unselected()));
        }
        if (!runtime_state->get_error_log_file_path().empty()) {
            params.__set_tracking_url(
                    to_load_error_http_path(runtime_state->get_error_log_file_path()));
        }
        if (!runtime_state->export_output_files().empty()) {
            params.__isset.export_files = true;
            params.export_files = runtime_state->export_output_files();
        }
        if (!runtime_state->tablet_commit_infos().empty()) {
            params.__isset.commitInfos = true;
            params.commitInfos.reserve(runtime_state->tablet_commit_infos().size());
            for (auto& info : runtime_state->tablet_commit_infos()) {
                params.commitInfos.push_back(info);
            }
        }
        if (!runtime_state->error_tablet_infos().empty()) {
            params.__isset.errorTabletInfos = true;
            params.errorTabletInfos.reserve(runtime_state->error_tablet_infos().size());
            for (auto& info : runtime_state->error_tablet_infos()) {
                params.errorTabletInfos.push_back(info);
            }
        }

        // Send new errors to coordinator
        runtime_state->get_unreported_errors(&(params.error_log));
        params.__isset.error_log = (params.error_log.size() > 0);
    }

    if (_exec_env->master_info()->__isset.backend_id) {
        params.__set_backend_id(_exec_env->master_info()->backend_id);
    }

    TReportExecStatusResult res;
    Status rpc_status;

    VLOG_DEBUG << "reportExecStatus params is "
               << apache::thrift::ThriftDebugString(params).c_str();
    try {
        try {
            coord->reportExecStatus(res, params);
        } catch (TTransportException& e) {
            LOG(WARNING) << "Retrying ReportExecStatus. query id: " << print_id(_query_id)
                         << ", instance id: " << print_id(_fragment_instance_id) << " to "
                         << coord_addr << ", err: " << e.what();
            rpc_status = coord.reopen();

            if (!rpc_status.ok()) {
                // we need to cancel the execution of this fragment
                cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "rpc fail");
                return;
            }
            coord->reportExecStatus(res, params);
        }

        rpc_status = Status(res.status);
    } catch (TException& e) {
        std::stringstream msg;
        msg << "ReportExecStatus() to " << coord_addr << " failed:\n" << e.what();
        LOG(WARNING) << msg.str();
        rpc_status = Status::InternalError(msg.str());
    }

    if (!rpc_status.ok()) {
        // we need to cancel the execution of this fragment
        cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "rpc fail 2");
    }
}

} // namespace doris::pipeline