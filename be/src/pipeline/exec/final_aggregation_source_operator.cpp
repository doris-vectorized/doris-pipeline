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

#include "final_aggregation_source_operator.h"

#include "vec/exec/vaggregation_node.h"

namespace doris {
namespace pipeline {

FinalAggSourceOperator::FinalAggSourceOperator(OperatorTemplate* templ,
                                               vectorized::AggregationNode* node)
        : Operator(templ), _agg_node(node) {}
Status FinalAggSourceOperator::init(const ExecNode* exec_node, RuntimeState* state) {
    RETURN_IF_ERROR(Operator::init(exec_node, state));
    return Status::OK();
}

// for poc
Status FinalAggSourceOperator::prepare(RuntimeState* state) {
    _mem_tracker = std::make_unique<MemTracker>(
            "FinalAggSourceOperator:" + _runtime_profile->name(), _runtime_profile.get());
    return Status::OK();
}

// for poc
Status FinalAggSourceOperator::open(RuntimeState* state) {
    return Status::OK();
}

bool FinalAggSourceOperator::can_read() {
    // TODO: Rethink use can_read() to replace the dependency
    return true;
}

Status FinalAggSourceOperator::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(_agg_node->_executor.get_result(state, block, eos));
    _agg_node->_make_nullable_output_key(block);
    // dispose the having clause, should not be execute in prestreaming agg
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_agg_node->_vconjunct_ctx_ptr, block,
                                                           block->columns()));
    reached_limit(block, eos);
    _agg_node->_executor.update_memusage();
    return Status::OK();
}

Status FinalAggSourceOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::close(state));
    for (auto* aggregate_evaluator : _agg_node->_aggregate_evaluators) {
        aggregate_evaluator->close(state);
    }
    vectorized::VExpr::close(_agg_node->_probe_expr_ctxs, state);

    if (_agg_node->_executor.close) {
        _agg_node->_executor.close();
    }

    /// _hash_table_size_counter may be null if prepare failed.
    if (_agg_node->_hash_table_size_counter) {
        std::visit(
                [&](auto&& agg_method) {
                    COUNTER_SET(_agg_node->_hash_table_size_counter,
                                int64_t(agg_method.data.size()));
                },
                _agg_node->_agg_data._aggregated_method_variant);
    }
    return Status::OK();
}

///////////////////////////////  operator template  ////////////////////////////////

FinalAggSourceOperatorTemplate::FinalAggSourceOperatorTemplate(
        int32_t id, const std::string& name, vectorized::AggregationNode* exec_node)
        : OperatorTemplate(id, name, dynamic_cast<ExecNode*>(exec_node)) {}

OperatorPtr FinalAggSourceOperatorTemplate::build_operator() {
    return std::make_shared<FinalAggSourceOperator>(
            this, dynamic_cast<vectorized::AggregationNode*>(_related_exec_node));
}

} // namespace pipeline
} // namespace doris