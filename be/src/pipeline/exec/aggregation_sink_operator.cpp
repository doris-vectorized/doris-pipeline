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

#include "aggregation_sink_operator.h"

#include "vec/exec/vaggregation_node.h"

namespace doris::pipeline {

AggSinkOperator::AggSinkOperator(AggSinkOperatorTemplate* operator_template,
                                 vectorized::AggregationNode* agg_node,
                                 std::shared_ptr<AggContext> agg_context)
        : Operator(operator_template), _agg_node(agg_node), _agg_context(std::move(agg_context)) {}

Status AggSinkOperator::init(const ExecNode* exec_node, RuntimeState* state) {
    RETURN_IF_ERROR(Operator::init(exec_node, state));
    return Status::OK();
}

Status AggSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_agg_node->prepare_profile(state));
    _agg_node->_child_return_rows =
            std::bind<int64_t>(&AggSinkOperator::get_child_return_rows, this);
    return Status::OK();
}

Status AggSinkOperator::open(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::open(state));
    // open agg node
    RETURN_IF_ERROR(vectorized::VExpr::open(_agg_node->_probe_expr_ctxs, state));
    for (int i = 0; i < _agg_node->_aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_agg_node->_aggregate_evaluators[i]->open(state));
    }
    if (!_agg_node->is_streaming_preagg()) {
        if (_agg_node->_probe_expr_ctxs.empty()) {
            _agg_node->_create_agg_status(_agg_node->_agg_data.without_key);
            _agg_node->_agg_data_created_without_key = true;
        }
    }
    return Status::OK();
}

bool AggSinkOperator::can_write() {
    if (!_agg_node->is_streaming_preagg()) {
        return true;
    } else {
        // sink and source in diff threads
        return _agg_context->has_enough_space_to_push();
    }
}

Status AggSinkOperator::sink(RuntimeState* state, vectorized::Block* in_block, bool eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if ((!in_block || in_block->rows() == 0) && !eos) {
        return Status::OK();
    }
    if (!_agg_node->is_streaming_preagg()) {
        RETURN_IF_ERROR(_agg_node->_executor.execute(in_block));
        _num_rows_returned += in_block->rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    } else {
        // TODO pipeline 不能这里是返回block

        Status ret = Status::OK();

        if (in_block && in_block->rows() > 0) {
            auto* bock_from_ctx = _agg_context->get_free_block();
            ret = _agg_node->_executor.pre_agg(in_block, bock_from_ctx);
            if (!ret.ok()) {
                _agg_context->push_block(bock_from_ctx);
                return ret;
            }
            if (bock_from_ctx->rows() == 0) {
                _agg_context->return_free_block(bock_from_ctx);
            } else {
                _num_rows_returned += bock_from_ctx->rows();
                _agg_node->_make_nullable_output_key(bock_from_ctx);
                _agg_context->push_block(bock_from_ctx);
                COUNTER_SET(_agg_node->_rows_returned_counter, _num_rows_returned);
                COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            }
        }

        if (UNLIKELY(eos)) {
            // 必须全部取出放到队列中
            bool get_eos = false;
            while (!get_eos) {
                auto* bock_from_ctx = _agg_context->get_free_block();
                ret = _agg_node->_executor.get_result(state, bock_from_ctx, &get_eos);
                if (!ret.ok()) {
                    _agg_context->push_block(bock_from_ctx);
                    return ret;
                }
                if (bock_from_ctx->rows() == 0) {
                    _agg_context->return_free_block(bock_from_ctx);
                } else {
                    _num_rows_returned += bock_from_ctx->rows();
                    _agg_node->_make_nullable_output_key(bock_from_ctx);
                    _agg_context->push_block(bock_from_ctx);
                    COUNTER_SET(_agg_node->_rows_returned_counter, _num_rows_returned);
                    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
                }
            }
            // 尽快出发下游结束，这里不在finalize中做。
            // 但是要在每个循环中都对eos做判断。可以权衡考虑放到finalize中。
            _agg_context->set_finish();
        }
    }
    _agg_node->_executor.update_memusage();
    return Status::OK();
}

Status AggSinkOperator::close(RuntimeState* state) {
    // for poc
    if (_agg_context && !_agg_context->is_finish()) {
        // finish应该已经在结束时候设置了，如果没有设置说明出错了。
        _agg_context->set_canceled();
    }
    return Status::OK();
}

///////////////////////////////  operator template  ////////////////////////////////

AggSinkOperatorTemplate::AggSinkOperatorTemplate(int32_t id, const std::string& name,
                                                 vectorized::AggregationNode* exec_node,
                                                 std::shared_ptr<AggContext> agg_context)
        : OperatorTemplate(id, name, dynamic_cast<ExecNode*>(exec_node)),
          _agg_node(exec_node),
          _agg_context(std::move(agg_context)) {}

OperatorPtr AggSinkOperatorTemplate::build_operator() {
    return std::make_shared<AggSinkOperator>(this, _agg_node, _agg_context);
}

// use final aggregation source operator
bool AggSinkOperatorTemplate::is_sink() const {
    return true;
}

bool AggSinkOperatorTemplate::is_source() const {
    return false;
}
} // namespace doris::pipeline