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

#include <utility>

#include "operator.h"
#include "sort_context.h"

namespace doris {

namespace vectorized {
class VSortNode;
}

namespace pipeline {

class SortSinkOperatorTemplate;

class SortSinkOperator : public Operator {
public:
    SortSinkOperator(SortSinkOperatorTemplate* operator_template, vectorized::VSortNode* sort_node,
                     std::shared_ptr<SortContext> sort_context);
    Status init(const ExecNode*, RuntimeState*) override;

    Status open(RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    // return can write continue
    Status sink(RuntimeState* state, vectorized::Block* block, bool eos) override;

    Status finalize(RuntimeState* state) override { return _sort_context->finalize(state); }

    bool can_write() override { return !_sort_context->finalized; };

private:
    vectorized::VSortNode* _sort_node;
    std::shared_ptr<SortContext> _sort_context;
};

class SortSinkOperatorTemplate : public OperatorTemplate {
public:
    SortSinkOperatorTemplate(int32_t id, const std::string& name, vectorized::VSortNode* sort_node,
                             std::shared_ptr<SortContext> sort_context);

    bool is_sink() const override { return true; }

    bool is_source() const override { return false; }

    OperatorPtr build_operator() override {
        return std::make_shared<SortSinkOperator>(this, _sort_node, _sort_context);
    }

private:
    vectorized::VSortNode* _sort_node;
    std::shared_ptr<SortContext> _sort_context;
};

} // namespace pipeline
} // namespace doris