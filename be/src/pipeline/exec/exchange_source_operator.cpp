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
    _exchange_node->_sub_plan_query_statistics_recvr.reset(new QueryStatisticsRecvr());
    _exchange_node->_stream_recvr = state->exec_env()->vstream_mgr()->create_recvr(
            state, _exchange_node->_input_row_desc, state->fragment_instance_id(),
            _exchange_node->_id, _exchange_node->_num_senders, config::exchg_node_buffer_size_bytes,
            _exchange_node->_runtime_profile.get(), _exchange_node->_is_merging,
            _exchange_node->_sub_plan_query_statistics_recvr);
    _exchange_node->_rows_returned_counter =
            ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);

    // TODO pipeline 支持merge读
    if (_exchange_node->_is_merging) {
        return Status::NotSupported("Now pipeline shuffle not support merging.");
    }
    if (_exchange_node->_is_merging) {
        return Status::NotSupported("Not Implemented merging exchange source operator");
    }
    return Status::OK();
}

Status ExchangeSourceOperator::open(RuntimeState* state) {
    // TODO pipeline 1
    // 如果是_is_merging的情况
    // VDataStreamRecvr::create_merger
    //   VSortedRunMerger::prepare
    //     ReceiveQueueSortCursorImpl::ReceiveQueueSortCursorImpl
    //       ReceiveQueueSortCursorImpl::has_next_block
    //         VDataStreamRecvr::SenderQueue::get_batch 阻塞（eof、出错、下一个block到来）
    return Operator::open(state);
}

bool ExchangeSourceOperator::can_read() {
    // VDataStreamRecvr中所有SenderQueue都可读
    // TODO pipeline 1
    return _exchange_node->_stream_recvr->has_data(0);
}

Status ExchangeSourceOperator::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    return _exchange_node->get_next(state, block, eos);
    // TODO pipeline 支持merge读
    // VDataStreamRecvr::get_next
    //   VSortedRunMerger::get_next
    //     ReceiveQueueSortCursorImpl::has_next_block
    //       VDataStreamRecvr::SenderQueue::get_batch阻塞
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
