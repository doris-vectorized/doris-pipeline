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

#include "pre_aggregation_source_operator.h"

#include "vec/exec/vaggregation_node.h"

namespace doris {
namespace pipeline {
PreAggSourceOperator::PreAggSourceOperator(OperatorTemplate* templ,
                                           vectorized::AggregationNode* node,
                                           std::shared_ptr<AggContext> agg_context)
        : Operator(templ), _agg_node(node), _agg_context(std::move(agg_context)) {}

// for poc
Status PreAggSourceOperator::prepare(RuntimeState* state) {
    _mem_tracker = std::make_unique<MemTracker>("PreAggSourceOperator:" + _runtime_profile->name(),
                                                _runtime_profile.get());
    return Status::OK();
}

// for poc
Status PreAggSourceOperator::open(RuntimeState* state) {
    return Status::OK();
}

bool PreAggSourceOperator::can_read() {
    return _agg_context->has_data_or_finished();
}

Status PreAggSourceOperator::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    vectorized::Block* agg_block = nullptr;
    RETURN_IF_ERROR(_agg_context->get_block(&agg_block, eos));

    if (*eos) {
        DCHECK(agg_block == nullptr);
        return Status::OK();
    }
    DCHECK(agg_block != nullptr);

    block->swap(*agg_block);
    _num_rows_returned += block->rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    _agg_context->return_free_block(agg_block);

    return Status::OK();
}

Status PreAggSourceOperator::close(RuntimeState* state) {
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

PreAggSourceOperatorTemplate::PreAggSourceOperatorTemplate(int32_t id, const std::string& name,
                                                           vectorized::AggregationNode* exec_node,
                                                           std::shared_ptr<AggContext> agg_context)
        : OperatorTemplate(id, name, dynamic_cast<ExecNode*>(exec_node)),
          _agg_context(std::move(agg_context)) {}

OperatorPtr PreAggSourceOperatorTemplate::build_operator() {
    return std::make_shared<PreAggSourceOperator>(
            this, dynamic_cast<vectorized::AggregationNode*>(_related_exec_node), _agg_context);
}

} // namespace pipeline
} // namespace doris