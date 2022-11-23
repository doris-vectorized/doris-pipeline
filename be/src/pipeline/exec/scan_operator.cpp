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

#include "scan_operator.h"

#include "vec/exec/scan/scanner_context.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::pipeline {

ScanOperator::ScanOperator(OperatorBuilder* operator_template,
                           doris::vectorized::VScanNode* scan_node)
        : Operator(operator_template), _scan_node(scan_node), _eos(false) {}

Status ScanOperator::init(ExecNode* exec_node, RuntimeState* state) {
    RETURN_IF_ERROR(Operator::init(exec_node, state));
    //    RETURN_IF_ERROR(_scan_node->_init_profile()); // subclass should call _init_profile，eg: OlapScanOperator
    return Status::OK();
}

// VScanNode::init call the VScanNode::_register_runtime_filter, gen the generate VScanNode::_runtime_filter_ctxs
Status ScanOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    // init profile for runtime filter
    for (auto& rf_ctx : _scan_node->_runtime_filter_ctxs) {
        rf_ctx.runtime_filter->init_profile(_scan_node->runtime_profile());
    }

    // Scan node set the two in ctor
    _scan_node->_input_tuple_desc =
            state->desc_tbl().get_tuple_descriptor(_scan_node->_input_tuple_id);
    _scan_node->_output_tuple_desc =
            state->desc_tbl().get_tuple_descriptor(_scan_node->_output_tuple_id);
    return Status::OK();
}

Status ScanOperator::open(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::open(state));
    RETURN_IF_ERROR(_scan_node->_acquire_runtime_filter(false));
    RETURN_IF_ERROR(_scan_node->_process_conjuncts());

    std::list<doris::vectorized::VScanner*> scanners;
    RETURN_IF_ERROR(_scan_node->_init_scanners(&scanners));
    if (scanners.empty()) {
        _eos = true;
    } else {
        RETURN_IF_ERROR(_scan_node->_start_scanners(scanners));
        _scanner_ctx = _scan_node->_scanner_ctx;
    }

    return Status::OK();
}

bool ScanOperator::can_read() {
    if (_eos || !_scanner_ctx || _scanner_ctx->done() || _scanner_ctx->can_finish()) {
        // _eos: need eos
        // !_scanner_ctx: need call open
        // _scanner_ctx->done(): need finish
        // _scanner_ctx->can_finish(): should be scheduled
        return true;
    } else {
        return !_scanner_ctx->empty_in_queue(); // have block to process
    }
}

Status ScanOperator::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    // 参考vscan node
    if (state->is_cancelled()) {
        _scanner_ctx->set_status_on_error(Status::Cancelled("query cancelled"));
        return _scanner_ctx->status();
    }

    if (_eos) {
        *eos = true;
        return Status::OK();
    }

    vectorized::Block* scan_block = nullptr;
    RETURN_IF_ERROR(_scanner_ctx->get_block_from_queue(&scan_block, eos, false));
    if (*eos) {
        DCHECK(scan_block == nullptr);
        _eos = true;
        return Status::OK();
    }
    if (!scan_block) { // no data
        return Status::OK();
    }

    // get scanner's block memory
    block->swap(*scan_block);
    _scanner_ctx->return_free_block(scan_block);

    reached_limit(block, eos);
    if (*eos) {
        // reach limit, stop the scanners.
        _scanner_ctx->set_should_stop();
    }
    return Status::OK();
}

bool ScanOperator::is_pending_finish() {
    return _scanner_ctx && !_scanner_ctx->can_finish();
}

Status ScanOperator::close(RuntimeState* state) {
    // TODO pipeline scan get profile from scan node
    if (is_closed()) {
        return Status::OK();
    }
    if (_scanner_ctx) {
        DCHECK(_scanner_ctx->can_finish());
        // stop and wait the scanner scheduler to be done
        // _scanner_ctx may not be created for some short circuit case.
        _scanner_ctx->set_should_stop();
        _scanner_ctx->clear_and_join();
    }

    for (auto& ctx : _scan_node->_runtime_filter_ctxs) {
        auto* runtime_filter = ctx.runtime_filter;
        runtime_filter->consumer_close();
    }

    for (auto& ctx : _scan_node->_stale_vexpr_ctxs) {
        (*ctx)->close(state);
    }

    return Operator::close(state);
}

} // namespace doris::pipeline