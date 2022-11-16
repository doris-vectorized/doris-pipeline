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

#include "exchange_source_operator.h"

#include "common/status.h"
#include "vec/exec/vexchange_node.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris::pipeline {

ExchangeSourceOperator::ExchangeSourceOperator(OperatorTemplate* operator_template)
        : Operator(operator_template) {}

Status ExchangeSourceOperator::init(const ExecNode* exec_node, RuntimeState* state) {
    RETURN_IF_ERROR(Operator::init(exec_node, state));
    _exchange_node =
            dynamic_cast<doris::vectorized::VExchangeNode*>(const_cast<ExecNode*>(exec_node));
    return Status::OK();
}

Status ExchangeSourceOperator::prepare(RuntimeState* state) {
    _exchange_node->_rows_returned_counter = _rows_returned_counter;
    return Status::OK();
}

Status ExchangeSourceOperator::open(RuntimeState* state) {
    return _exchange_node->open(state);
}

bool ExchangeSourceOperator::can_read() {
    return _exchange_node->_stream_recvr->has_data(0);
}

Status ExchangeSourceOperator::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    auto st = _exchange_node->get_next(state, block, eos);
    if (block) {
        _num_rows_returned += block->rows();
    }
    return st;
}

Status ExchangeSourceOperator::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    if (_exchange_node && _exchange_node->_stream_recvr) {
        _exchange_node->_stream_recvr->close();
    }

    return Operator::close(state);
}

} // namespace doris::pipeline
