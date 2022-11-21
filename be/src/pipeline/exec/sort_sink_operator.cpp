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

#include "sort_sink_operator.h"

#include "vec/exec/vsort_node.h"

namespace doris::pipeline {

SortSinkOperatorTemplate::SortSinkOperatorTemplate(int32_t id, const string& name,
                                                   vectorized::VSortNode* sort_node)
        : OperatorTemplate(id, name, sort_node), _sort_node(sort_node) {}

SortSinkOperator::SortSinkOperator(SortSinkOperatorTemplate* operator_template,
                                   vectorized::VSortNode* sort_node)
        : Operator(operator_template), _sort_node(sort_node) {}

Status SortSinkOperator::init(doris::ExecNode* node, doris::RuntimeState* state) {
    RETURN_IF_ERROR(Operator::init(node, state));
    return Status::OK();
}

Status SortSinkOperator::open(doris::RuntimeState* state) {
    RETURN_IF_ERROR(Operator::open(state));
    RETURN_IF_ERROR(_sort_node->alloc_resource(state));
    return Status::OK();
}

Status SortSinkOperator::close(doris::RuntimeState* state) {
    return Status::OK();
}

Status SortSinkOperator::sink(doris::RuntimeState* state, vectorized::Block* block, bool eos) {
    RETURN_IF_ERROR(_sort_node->sink(state, block, eos));
    return Status::OK();
}
} // namespace doris::pipeline
