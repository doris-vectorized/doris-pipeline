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

#include "olap_scan_operator.h"

#include "vec/exec/scan/new_olap_scan_node.h"

namespace doris::pipeline {

OlapScanOperator::OlapScanOperator(OperatorTemplate* operator_template,
                                   doris::vectorized::NewOlapScanNode* scan_node)
        : ScanOperator(operator_template, dynamic_cast<doris::vectorized::VScanNode*>(scan_node)) {}

Status OlapScanOperator::init(const ExecNode* exec_node, RuntimeState* state) {
    RETURN_IF_ERROR(ScanOperator::init(exec_node, state));
    return Status::OK();
}

Status OlapScanOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanOperator::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    return Status::OK();
}

OlapScanOperatorTemplate::OlapScanOperatorTemplate(uint32_t id, const std::string& name,
                                                   // TupleId o_tuple_id, int64_t limit_per_scanner,
                                                   vectorized::NewOlapScanNode* new_olap_scan_node)
        : ScanOperatorTemplate(id, name, dynamic_cast<ExecNode*>(new_olap_scan_node)),
          _new_olap_scan_node(new_olap_scan_node) {}

} // namespace doris::pipeline