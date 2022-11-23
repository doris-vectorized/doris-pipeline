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

AggSinkOperator::AggSinkOperator(AggSinkOperatorBuilder* operator_template,
                                 vectorized::AggregationNode* agg_node,
                                 std::shared_ptr<AggContext> agg_context)
        : Operator(operator_template), _agg_node(agg_node), _agg_context(std::move(agg_context)) {}

Status AggSinkOperator::init(ExecNode* exec_node, RuntimeState* state) {
    RETURN_IF_ERROR(Operator::init(exec_node, state));
    return Status::OK();
}

Status AggSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _agg_node->_child_return_rows =
            std::bind<int64_t>(&AggSinkOperator::get_child_return_rows, this);
    return Status::OK();
}

Status AggSinkOperator::open(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::open(state));
    _agg_node->alloc_resource(state);
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
    if (!_agg_node->is_streaming_preagg()) {
        RETURN_IF_ERROR(_agg_node->sink(state, in_block, eos));
    } else {
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
            // must push all result block to queue
            bool get_eos = false;
            while (!get_eos) {
                auto* bock_from_ctx = _agg_context->get_free_block();
                // TODO: better call the method in agg source
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
            _agg_context->set_finish();
        }
    }
    // TODO: remove it after we split the stream agg and normal agg
    _agg_node->_executor.update_memusage();
    return Status::OK();
}

Status AggSinkOperator::close(RuntimeState* state) {
    // for poc
    if (_agg_context && !_agg_context->is_finish()) {
        // finish should be set, if not set here means error.
        _agg_context->set_canceled();
    }
    return Status::OK();
}

///////////////////////////////  operator template  ////////////////////////////////

AggSinkOperatorBuilder::AggSinkOperatorBuilder(int32_t id, const std::string& name,
                                               vectorized::AggregationNode* exec_node,
                                               std::shared_ptr<AggContext> agg_context)
        : OperatorBuilder(id, name, exec_node),
          _agg_node(exec_node),
          _agg_context(std::move(agg_context)) {}

OperatorPtr AggSinkOperatorBuilder::build_operator() {
    return std::make_shared<AggSinkOperator>(this, _agg_node, _agg_context);
}

// use final aggregation source operator
bool AggSinkOperatorBuilder::is_sink() const {
    return true;
}

bool AggSinkOperatorBuilder::is_source() const {
    return false;
}
} // namespace doris::pipeline