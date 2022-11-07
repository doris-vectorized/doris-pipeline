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

#include <queue>

#include "common/status.h"
#include "pipeline.h"
#include "pipeline_task.h"
#include "util/threadpool.h"

namespace doris::pipeline {

class SubWorkTaskQueue {
    friend class WorkTaskQueue;

public:
    void add_total_time(const uint64_t duration) { _total_consume_time.fetch_add(duration); }

    void push_back(PipelineTask* task) { _queue.emplace(task); }

    PipelineTask* try_take() {
        if (_queue.empty()) {
            return nullptr;
        }
        auto task = _queue.front();
        _queue.pop();
        return task;
    }

    void set_factor_for_normal(double factor_for_normal) { _factor_for_normal = factor_for_normal; }

    // TODO pipeline 1 有没有可能会溢出？？？
    double total_consume_time() { return _total_consume_time.load() / _factor_for_normal; }

    bool empty() { return _queue.empty(); }

private:
    std::queue<PipelineTask*> _queue;
    // factor for normalization
    double _factor_for_normal = 1;
    // TODO pipeline 需不需要一个归0的操作 1
    std::atomic<uint64_t> _total_consume_time = 0;
};

// 每个线程有自己的多级反馈队列
class WorkTaskQueue {
public:
    explicit WorkTaskQueue() : _closed(false) {
        double factor = 1;
        for (int i = SUB_QUEUE_LEVEL - 1; i >= 0; --i) {
            _sub_queues[i].set_factor_for_normal(factor);
            factor *= 1.2;
        }
    }

    void close() {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        _closed = true;
        _wait_task.notify_all();
    }

    PipelineTask* try_take_unprotected() {
        if (_total_task_size == 0 || _closed) {
            return nullptr;
        }
        double min_consume_time = _sub_queues[0].total_consume_time();
        int idx = 0;
        for (int i = 1; i < SUB_QUEUE_LEVEL; ++i) {
            if (!_sub_queues[i].empty()) {
                double consume_time = _sub_queues[i].total_consume_time();
                if (idx == -1 || consume_time < min_consume_time) {
                    idx = i;
                    min_consume_time = consume_time;
                }
            }
        }
        auto task = _sub_queues[idx].try_take();
        if (task) {
            _total_task_size--;
        }
        return task;
    }

    PipelineTask* try_take() {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        return try_take_unprotected();
    }

    PipelineTask* take() {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        while (!_closed) {
            auto task = try_take_unprotected();
            if (task) {
                return task;
            } else {
                _wait_task.wait(lock);
            }
        }
        DCHECK(_closed);
        return nullptr;
    }

    void push(PipelineTask* task) {
        size_t level = _compute_level(task);
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        _sub_queues[level].push_back(task);
        _total_task_size++;
        _wait_task.notify_one();
    }

    // TODO pipeline 1 根据size作为每个核心的负载不准确
    size_t size() {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        return _total_task_size;
    }

private:
    static const size_t SUB_QUEUE_LEVEL = 5;
    SubWorkTaskQueue _sub_queues[SUB_QUEUE_LEVEL];
    std::mutex _work_size_mutex;
    std::condition_variable _wait_task;
    size_t _total_task_size = 0;
    bool _closed;

private:
    // TODO pipeline 1
    size_t _compute_level(PipelineTask* task) { return 0; }
};

// 应该考虑核心之间的内存距离
class TaskQueue {
public:
    explicit TaskQueue(size_t core_size) : _core_size(core_size) {
        _async_queue = new WorkTaskQueue[core_size];
    }

    ~TaskQueue() { delete[] _async_queue; }

    void close() {
        for (int i = 0; i < _core_size; ++i) {
            _async_queue[i].close();
        }
    }

    // 尝试从自己队列中拿task执行
    PipelineTask* try_take(size_t core_id) { return _async_queue[core_id].try_take(); }

    // not block，尝试从其他核心偷任务执行
    PipelineTask* steal_take(size_t core_id) {
        DCHECK(core_id < _core_size);
        size_t next_id = core_id;
        for (size_t i = 1; i < _core_size; ++i) {
            ++next_id;
            if (next_id == _core_size) {
                next_id = 0;
            }
            DCHECK(next_id < _core_size);
            auto task = try_take(next_id);
            if (task) {
                return task;
            }
        }
        return nullptr;
    }

    // TODO pipeline 1 增加超时接口
    PipelineTask* take(size_t core_id) { return _async_queue[core_id].take(); }

    void push_back(PipelineTask* task) {
        int core_id = task->get_previous_core_id();
        if (core_id < 0) {
            core_id = _next_core.fetch_add(1) % _core_size;
        }
        DCHECK(core_id < _core_size);
        _async_queue[core_id].push(task);
    }

    void push_back(PipelineTask* task, size_t core_id) {
        DCHECK(core_id < _core_size);
        _async_queue[core_id].push(task);
    }

    int cores() const { return _core_size; }

private:
    WorkTaskQueue* _async_queue = nullptr;
    size_t _core_size; // 有没有办法
    std::atomic<size_t> _next_core = 0;
};

class BlockedTaskScheduler {
public:
    explicit BlockedTaskScheduler(std::shared_ptr<TaskQueue> task_queue)
            : _task_queue(std::move(task_queue)), _started(false), _shutdown(false) {}

    ~BlockedTaskScheduler();

    Status start();
    void shutdown();
    void add_blocked_task(PipelineTask* task);

private:
    std::shared_ptr<TaskQueue> _task_queue;

    std::mutex _task_mutex;
    std::condition_variable _task_cond;
    std::list<PipelineTask*> _blocked_tasks;

    scoped_refptr<Thread> _thread;
    std::atomic<bool> _started;
    std::atomic<bool> _shutdown;

private:
    void _schedule();
    void _make_task_run(std::list<PipelineTask*>& local_tasks,
                        std::list<PipelineTask*>::iterator& task_itr,
                        std::vector<PipelineTask*>& ready_tasks);
};

class TaskScheduler {
public:
    TaskScheduler(ExecEnv* exec_env, std::shared_ptr<BlockedTaskScheduler> b_scheduler,
                  std::shared_ptr<TaskQueue> task_queue)
            : _task_queue(std::move(task_queue)),
              _exec_env(exec_env),
              _blocked_task_scheduler(std::move(b_scheduler)),
              _shutdown(false) {}

    ~TaskScheduler();

    Status schedule_task(PipelineTask* task);

    // 应该是cpu核心的整倍数
    Status start();

    void shutdown();

    BlockedTaskScheduler* blocked_task_scheduler() const { return _blocked_task_scheduler.get(); }

    ExecEnv* exec_env() { return _exec_env; }

private:
    std::unique_ptr<ThreadPool> _fix_thread_pool;
    std::shared_ptr<TaskQueue> _task_queue;
    std::vector<std::shared_ptr<std::atomic<bool>>> _markers;
    ExecEnv* _exec_env;
    std::shared_ptr<BlockedTaskScheduler> _blocked_task_scheduler;
    std::atomic<bool> _shutdown;

private:
    void _do_work(size_t index);
    void _try_close_task(PipelineTask* task, PipelineTaskState state);
};
} // namespace doris::pipeline