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

#include "operator.h"

namespace doris::pipeline {

Operator::Operator(OperatorTemplate* operator_template)
        : _operator_template(operator_template), _limit(-1), _is_closed(false) {}

bool Operator::is_sink() const {
    return _operator_template->is_sink();
}

bool Operator::is_source() const {
    return _operator_template->is_source();
}

Status Operator::init(const ExecNode* exec_node, RuntimeState* state) {
    _runtime_profile.reset(new RuntimeProfile("Operator"));
    _runtime_profile->set_metadata(_operator_template->id());
    if (exec_node && exec_node->limit() >= 0) {
        _limit = exec_node->limit();
    }
    return Status::OK();
}

Status Operator::prepare(RuntimeState* state) {
    _mem_tracker = std::make_unique<MemTracker>("Operator:" + _runtime_profile->name(),
                                                _runtime_profile.get());
    // for poc
    if (_operator_template->exec_node()) {
        RETURN_IF_ERROR(_operator_template->exec_node()->prepare_self(state));
    }
    return Status::OK();
}

Status Operator::open(RuntimeState* state) {
    // for poc
    if (_operator_template->exec_node()) {
        RETURN_IF_ERROR(_operator_template->exec_node()->open_self(state));
    }
    return Status::OK();
}

// 释放资源
Status Operator::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    _is_closed = true;
    if (_operator_template->exec_node()) {
        _operator_template->exec_node()->close_self(state);
    }
    return Status::OK();
}

void Operator::reached_limit(vectorized::Block* block, bool* eos) {
    if (_limit != -1 and _num_rows_returned + block->rows() >= _limit) {
        block->set_num_rows(_limit - _num_rows_returned);
        *eos = true;
    }
    _num_rows_returned += block->rows();
}

const RowDescriptor& Operator::row_desc() {
    return _operator_template->row_desc();
}

/////////////////////////////////////// OperatorTemplate ////////////////////////////////////////////////////////////

Status OperatorTemplate::prepare(doris::RuntimeState* state) {
    _state = state;
    // runtime filter ,目前已经由VScanNode处理
    return Status::OK();
}

void OperatorTemplate::close(doris::RuntimeState* state) {
    if (_is_closed) {
        return;
    }
    _is_closed = true;
}

} // namespace doris::pipeline
