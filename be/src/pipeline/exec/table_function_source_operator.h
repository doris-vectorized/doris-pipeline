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

#pragma once

#include "operator.h"
#include "vec/exec/vtable_function_node.h"

namespace doris {

namespace pipeline {

class TableFunctionSourceOperatorBuilder;

class TableFunctionSourceOperator : public Operator {
public:
    TableFunctionSourceOperator(TableFunctionSourceOperatorBuilder* operator_builder,
                                vectorized::VTableFunctionNode* sort_node);

    Status close(RuntimeState* state) override {
        RETURN_IF_ERROR(_node->close(state));
        return Operator::close(state);
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        return _node->get_next(state, block, eos);
    }

    bool can_read() override { return _node->can_read(); }

private:
    vectorized::VTableFunctionNode* _node;
};

class TableFunctionSourceOperatorBuilder : public OperatorBuilder {
public:
    TableFunctionSourceOperatorBuilder(int32_t id, const std::string& name,
                                       vectorized::VTableFunctionNode* node)
            : OperatorBuilder(id, name, node), _node(node) {}

    bool is_sink() const override { return false; }

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override {
        return std::make_shared<TableFunctionSourceOperator>(this, _node);
    }

private:
    vectorized::VTableFunctionNode* _node;
};

} // namespace pipeline
} // namespace doris