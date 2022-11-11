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

#include "exec/operator.h"
#include "pipeline.h"
#include "util/stopwatch.hpp"

namespace doris::pipeline {

enum PipelineTaskState : uint8_t {
    NOT_READY = 0, // 没有prepare
    BLOCKED = 1,   // have some dependencies not finished or some conditions not met
    BLOCKED_FOR_DEPENDENCY = 2,
    BLOCKED_FOR_SOURCE = 3,
    BLOCKED_FOR_SINK = 4,

    RUNNABLE = 5,  // 可执行
    PENDING_FINISH = 6, // 计算任务已结束，但资源还不能释放，例如有scan任务或sink任务
    FINISHED = 7, // 正常结束
    CANCELED = 8  // 异常结束
};

// 非多线程安全
class PipelineTask {
public:
    PipelineTask(PipelinePtr& pipeline, uint32_t index, RuntimeState* state, Operators& operators,
                 OperatorPtr& sink, PipelineFragmentContext* fragment_context,
                 RuntimeProfile* parent_profile)
            : _index(index),
              _pipeline(pipeline),
              _operators(operators),
              _source(_operators.front()),
              _root(_operators.back()),
              _sink(sink),
              _prepared(false),
              _opened(false),
              _state(state),
              _cur_state(NOT_READY),
              _fragment_context(fragment_context),
              _parent_profile(parent_profile) {}

    Status prepare(RuntimeState* state);

    // 执行pipeline，以时间片为调度
    // 以sink和source的状态返回pipeline task的状态
    Status execute(bool* eos);

    // 释放资源,可能是cancel
    Status close();
    PipelineTaskState get_state() { return _cur_state; }

    void set_state(PipelineTaskState state) {
        // DCHECK state 状态是否正常
        _cur_state = state;
    }

    bool is_pending_finish() { return _source->is_pending_finish() || _sink->is_pending_finish(); }

    // 当该方法返回true时，表示该task应该被BlockedTaskScheduler check
    bool is_blocking() { return has_dependency() || !_source->can_read() || !_sink->can_write(); }

    // task执行完成
    Status finalize();

    void finish_p_dependency() {
        for (const auto& p : _pipeline->_parents) {
            p->finish_one_dependency();
        }
    }

    PipelineFragmentContext* fragment_context() { return _fragment_context; }

    QueryFragmentsCtx* query_fragments_context();

    int get_previous_core_id() { return _previous_schedule_id; }

    void set_previous_core_id(int id) { _previous_schedule_id = id; }

    bool has_dependency();

    uint32_t index() { return _index; }

    OperatorPtr get_root() { return _root; }

private:
    Status open();
    void _init_profile();

private:
    uint32_t _index;
    PipelinePtr _pipeline;
    bool _dependency_finish = false;
    Operators _operators; // left is _source, right is _root
    OperatorPtr _source;
    OperatorPtr _root;
    OperatorPtr _sink;

    bool _prepared;
    bool _opened;
    RuntimeState* _state;
    int _previous_schedule_id = -1;
    PipelineTaskState _cur_state;
    std::unique_ptr<doris::vectorized::Block> _block;
    PipelineFragmentContext* _fragment_context;

    RuntimeProfile* _parent_profile;
    std::unique_ptr<RuntimeProfile> _task_profile;
    RuntimeProfile::Counter* _sink_timer;
    RuntimeProfile::Counter* _get_block_timer;
};
} // namespace doris::pipeline