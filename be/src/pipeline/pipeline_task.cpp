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

#include "pipeline_task.h"

#include "pipeline/pipeline_fragment_context.h"

namespace doris::pipeline {

Status PipelineTask::prepare(RuntimeState* state) {
    if (_sink) {
        RETURN_IF_ERROR(_sink->prepare(state));
    }
    for (auto& o : _operators) {
        RETURN_IF_ERROR(o->prepare(state));
    }
    _block.reset(new doris::vectorized::Block());
    _prepared = true;
    return Status::OK();
}

bool PipelineTask::has_dependency() {
    if (_dependency_finish) {
        return false;
    }
    if (_fragment_context->is_canceled()) {
        _dependency_finish = true;
        return false;
    }
    if (_pipeline->has_dependency()) {
        return true;
    }
    // fe还未开启查询
    if (!_state->get_query_fragments_ctx()
                 ->is_ready_to_execute()) { // TODO pipeline config::s_ready_to_execute
        return true;
    }

    // 其他前置条件，如runtime filter
    // ...

    _dependency_finish = true;
    return false;
}

Status PipelineTask::open() {
    if (_sink) {
        RETURN_IF_ERROR(_sink->open(_state));
    }
    for (auto& o : _operators) {
        RETURN_IF_ERROR(o->open(_state));
    }
    _opened = true;
    return Status::OK();
}

Status PipelineTask::execute(bool* eos) {
    int64_t time_spent = 0;
    // 检查状态一定要是runnable
    *eos = false;
    if (!_opened) {
        SCOPED_RAW_TIMER(&time_spent);
        RETURN_IF_ERROR(open());
    }
    LOG(INFO) << "start while " << this;
    while (_source->can_read() && _sink->can_write() && !_fragment_context->is_canceled()) {
        // TODO sr
        if (time_spent > 100'000'000L) {
            break;
        }
        SCOPED_RAW_TIMER(&time_spent); // pipeline TODO 这个方法后会清空数据么？
        _block->clear_column_data(_root->row_desc().num_materialized_slots());
        auto* block = _block.get();
        LOG(INFO) << "llj log get block " << this;
        RETURN_IF_ERROR(_root->get_block(_state, block, eos));
        if (_block->rows() != 0 || *eos) {
            LOG(INFO) << "llj log get block end " << *eos << " rows:" << _block->rows() << " "
                      << this;
            RETURN_IF_ERROR(_sink->sink(_state, block, *eos));
            if (*eos) { // 直接返回，scheduler会处理finish
                break;
            }
        }
    }

    if (!*eos && (!_source->can_read() || !_sink->can_write())) {
        set_state(BLOCKED);
    }

    return Status::OK();
}

Status PipelineTask::finalize() {
    return _sink->finalize(_state);
}

Status PipelineTask::close() {
    auto s = _sink->close(_state);
    for (auto op : _operators) {
        auto tem = op->close(_state);
        if (!tem.ok() && s.ok()) {
            s = tem;
        }
    }
    // 如果pipeline task是并发执行，则多个task执行结束才能close pipeline
    _pipeline->close(_state);
    return s;
}

QueryFragmentsCtx* PipelineTask::query_fragments_context() {
    return _fragment_context->get_query_context();
}
} // namespace doris::pipeline