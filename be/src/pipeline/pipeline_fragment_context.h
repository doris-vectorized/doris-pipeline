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

#include "pipeline.h"
#include "runtime/runtime_state.h"

namespace doris {
class ExecNode;
class DataSink;

namespace pipeline {

class PipelineTask;

class PipelineFragmentContext : public std::enable_shared_from_this<PipelineFragmentContext> {
public:
    PipelineFragmentContext(const TUniqueId& query_id, const TUniqueId& instance_id,
                            std::shared_ptr<QueryFragmentsCtx> query_ctx, ExecEnv* exec_env);

    virtual ~PipelineFragmentContext();

    PipelinePtr add_pipeline();

    TUniqueId get_fragment_id() { return _fragment_id; }

    RuntimeState* get_runtime_state() { return _runtime_state.get(); }

    bool is_canceled() { return _runtime_state->is_cancelled(); }

    int32_t next_operator_template_id() { return _next_operator_template_id++; }

    Status prepare(const doris::TExecPlanFragmentParams& request);

    Status submit();

    void set_is_report_success(bool is_report_success) { _is_report_success = is_report_success; }

    ExecNode*& plan() { return _root_plan; }

    void set_need_wait_execution_trigger() { _need_wait_execution_trigger = true; }

    void cancel(const PPlanFragmentCancelReason& reason, const std::string& msg = "");

    // TODO pipeline runtime filter
    //    void set_merge_controller_handler(
    //            std::shared_ptr<RuntimeFilterMergeControllerEntity>& handler) {
    //        _merge_controller_handler = handler;
    //    }

    QueryFragmentsCtx* get_query_context() { return _query_ctx.get(); }

    TUniqueId get_query_id() const { return _query_id; }

    void close_a_pipeline();

private:
    // Id of this query
    TUniqueId _query_id;
    // Id of this instance
    TUniqueId _fragment_instance_id;
    ExecEnv* _exec_env;
    TUniqueId _fragment_id;

    bool _prepared = false;
    bool _submitted = false;

    std::mutex _status_lock;
    Status _exec_status;
    bool _cancelled = false;
    PPlanFragmentCancelReason _cancel_reason;
    std::string _cancel_msg;

    Pipelines _pipelines;
    PipelineId _next_pipeline_id = 0;
    std::atomic<int> _closed_pipeline_cnt;

    int32_t _next_operator_template_id = 10000;

    std::vector<std::unique_ptr<PipelineTask>> _tasks;

    PipelinePtr _root_pipeline;

    // TODO pipeline profile
    bool _is_report_success = false;

    std::unique_ptr<RuntimeState> _runtime_state;

    ExecNode* _root_plan = nullptr; // lives in _runtime_state->obj_pool()
    std::unique_ptr<DataSink> _sink;

    std::shared_ptr<QueryFragmentsCtx> _query_ctx;
    // TODO pipeline runtime filter
    //    std::shared_ptr<RuntimeFilterMergeControllerEntity> _merge_controller_handler;

    // If set the true, this plan fragment will be executed only after FE send execution start rpc.
    bool _need_wait_execution_trigger = false;

    Status _create_sink(const TDataSink& t_data_sink);
};
} // namespace pipeline
} // namespace doris