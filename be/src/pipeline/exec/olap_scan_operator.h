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

#include "scan_operator.h"

namespace doris::vectorized {
class NewOlapScanNode;
}

namespace doris::pipeline {

class OlapScanOperatorTemplate;
class OlapScanOperator : public ScanOperator {
public:
    OlapScanOperator(OperatorTemplate* operator_template,
                     doris::vectorized::NewOlapScanNode* scan_node);
    Status prepare(RuntimeState* state) override;
    Status init(ExecNode* exec_node, RuntimeState* state) override;
    //    Status open(RuntimeState* state) override; // NewOlapScanNode::_build_key_ranges_and_filters
};

class OlapScanOperatorTemplate : public ScanOperatorTemplate {
public:
    OlapScanOperatorTemplate(uint32_t id, const std::string& name,
                             // TupleId o_tuple_id, int64_t limit_per_scanner,
                             vectorized::NewOlapScanNode* new_olap_scan_node);
    //            : ScanOperatorTemplate(id, name, dynamic_cast<ExecNode*>(new_olap_scan_node)),
    //            _output_tuple_id(o_tuple_id), _limit_per_scanner(limit_per_scanner),
    //            _new_olap_scan_node(new_olap_scan_node) {}

    OperatorPtr build_operator() override {
        return std::make_shared<OlapScanOperator>(this, _new_olap_scan_node);
    }

private:
    //    TupleId _output_tuple_id = -1;
    //    // If sort info is set, push limit to each scanner;
    //    int64_t _limit_per_scanner = -1;
    vectorized::NewOlapScanNode* _new_olap_scan_node;
};

} // namespace doris::pipeline