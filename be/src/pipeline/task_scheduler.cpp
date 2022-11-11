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

#include "task_scheduler.h"

#include "pipeline_fragment_context.h"
#include "util/thread.h"

namespace doris::pipeline {

Status BlockedTaskScheduler::start() {
    LOG(INFO) << "BlockedTaskScheduler start";
    RETURN_IF_ERROR(Thread::create(
            "BlockedTaskScheduler", "schedule_blocked_pipeline", [this]() { this->_schedule(); },
            &_thread));
    while (!this->_started.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return Status::OK();
}

BlockedTaskScheduler::~BlockedTaskScheduler() = default;

void BlockedTaskScheduler::shutdown() {
    if (!this->_shutdown.load()) {
        this->_shutdown.store(true);
        if (_thread) {
            _task_cond.notify_one();
            _thread->join();
        }
    }
}

void BlockedTaskScheduler::add_blocked_task(PipelineTask* task) {
    std::unique_lock<std::mutex> lock(_task_mutex);
    _blocked_tasks.push_back(task);
    _task_cond.notify_one();
}

void BlockedTaskScheduler::_schedule() {
    LOG(INFO) << "BlockedTaskScheduler schedule thread start";
    _started.store(true);
    std::list<PipelineTask*> local_blocked_tasks;
    int spin_count = 0;
    std::vector<PipelineTask*> ready_tasks;

    while (!_shutdown.load()) {
        std::unique_lock<std::mutex> lock(this->_task_mutex);
        local_blocked_tasks.splice(local_blocked_tasks.end(), _blocked_tasks);
        if (local_blocked_tasks.empty()) {
            //            if (!_shutdown.load()) {
            //                // shutdown不被锁保护，这里可能已经shutdown且_task_cond已经被通知过了
            //                // 非超时等待可能永久卡死
            //                _task_cond.wait(lock);
            //            }

            while (!_shutdown.load() && _blocked_tasks.empty()) {
                _task_cond.wait_for(lock, std::chrono::milliseconds(10));
            }

            if (_shutdown.load()) {
                break;
            }
            // _blocked_tasks一定不为空
            local_blocked_tasks.splice(local_blocked_tasks.end(), _blocked_tasks);

            //            sr
            //            std::cv_status cv_status = std::cv_status::no_timeout;
            //            while (!_shutdown.load() && _blocked_tasks.empty()) {
            //                cv_status = _task_cond.wait_for(lock, std::chrono::milliseconds(10));
            //            }
            //            if (cv_status == std::cv_status::timeout) {
            //                continue;
            //            }
            //            if (_shutdown.load()) {
            //                break;
            //            }
            //            local_blocked_tasks.splice(local_blocked_tasks.end(), _blocked_tasks);
        }

        auto iter = local_blocked_tasks.begin();
        DateTimeValue now = DateTimeValue::local_time();
        while (iter != local_blocked_tasks.end()) {
            auto* task = *iter;
            auto state = task->get_state();
            if (state == PENDING_FINISH || task->fragment_context()->is_canceled()) {
                // should cancel or should finish
                if (task->is_pending_finish()) {
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                }
            } else if (task->query_fragments_context()->is_timeout(now)) {
                // there are not any drivers belonging to a query context can make progress for an expiration period
                // indicates that some fragments are missing because of failed exec_plan_fragment invocation. in
                // this situation, query is failed finally, so drivers are marked PENDING_FINISH/FINISH.
                //
                // If the fragment is expired when the source operator is already pending i/o task,
                // The state of driver shouldn't be changed.
                LOG(WARNING) << "[Driver] Timeout, query_id="
                             << print_id(task->query_fragments_context()->query_id)
                             << ", instance_id="
                             << print_id(task->fragment_context()->get_fragment_id());

                task->fragment_context()->cancel(PPlanFragmentCancelReason::TIMEOUT);

                // 交由task runner去做
                //                driver->cancel_operators(driver->fragment_ctx()->runtime_state());
                if (task->is_pending_finish()) {
                    iter++;
                } else {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                }
            } else if (state == BLOCKED) {
                if (!task->is_blocking()) {
                    _make_task_run(local_blocked_tasks, iter, ready_tasks);
                } else {
                    iter++;
                }
            } else {
                _make_task_run(local_blocked_tasks, iter, ready_tasks);
            }
        }

        if (ready_tasks.empty()) {
            spin_count += 1;
        } else {
            spin_count = 0;
            for (auto& task : ready_tasks) {
                _task_queue->push_back(task);
            }
            ready_tasks.clear();
        }

        if (spin_count != 0 && spin_count % 64 == 0) {
#ifdef __x86_64__
            _mm_pause();
#else
            // TODO: Maybe there's a better intrinsic like _mm_pause on non-x86_64 architecture.
            sched_yield();
#endif
        }
        if (spin_count == 640) {
            spin_count = 0;
            sched_yield();
        }
    }
    LOG(INFO) << "BlockedTaskScheduler schedule thread stop";
}

void BlockedTaskScheduler::_make_task_run(std::list<PipelineTask*>& local_tasks,
                                          std::list<PipelineTask*>::iterator& task_itr,
                                          std::vector<PipelineTask*>& ready_tasks) {
    auto& task = *task_itr;
    //    driver->_pending_timer->update(driver->_pending_timer_sw->elapsed_time());
    local_tasks.erase(task_itr++);
    ready_tasks.emplace_back(task);
}

/////////////////////////  TaskScheduler  ///////////////////////////////////////////////////////////////////////////

TaskScheduler::~TaskScheduler() {
    shutdown();
}

Status TaskScheduler::start() {
    int cores = _task_queue->cores();
    ThreadPoolBuilder("TaskSchedulerThreadPool")
            .set_min_threads(cores)
            .set_max_threads(cores)
            .set_max_queue_size(0)
            .build(&_fix_thread_pool);
    _markers.reserve(cores);
    for (size_t i = 0; i < cores; ++i) {
        LOG(INFO) << "Start TaskScheduler thread " << i;
        _markers.push_back(std::make_shared<std::atomic<bool>>(true));
        RETURN_IF_ERROR(
                _fix_thread_pool->submit_func(std::bind(&TaskScheduler::_do_work, this, i)));
    }
    LOG(INFO) << "Start TaskScheduler threads end.";
    return _blocked_task_scheduler->start();
}

Status TaskScheduler::schedule_task(PipelineTask* task) {
    if (task->has_dependency()) {
        task->set_state(BLOCKED);
        _blocked_task_scheduler->add_blocked_task(task);
    } else {
        task->set_state(RUNNABLE);
        _task_queue->push_back(task);
    }
    // TODO 控制task的数量
    return Status::OK();
}

void TaskScheduler::_do_work(size_t index) {
    LOG(INFO) << "Start TaskScheduler worker " << index;
    auto queue = _task_queue;
    auto marker = _markers[index];
    while (*marker) {
        auto task = queue->try_take(index);
        LOG(INFO) << "llj log TaskScheduler worker " << index << " get task " << task;
        if (!task) {
            task = queue->steal_take(index);
            LOG(INFO) << "llj log TaskScheduler worker " << index << " steal task " << task;
            if (!task) {
                task = queue->take(index);
                LOG(INFO) << "llj log TaskScheduler worker " << index << " take task " << task;
                if (!task) {
                    continue;
                }
            }
        }
        auto* fragment_ctx = task->fragment_context();

        auto check_state = task->get_state();
        if (check_state == PENDING_FINISH) { // 从blocked task scheduler传过来
            bool is_pending = task->is_pending_finish();
            LOG(INFO) << "llj log do task end now: is pendding " << is_pending;
            DCHECK(!is_pending) << "must not pending close " << task;
            _try_close_task(task, fragment_ctx->is_canceled() ? CANCELED : FINISHED);
            continue;
        }
        DCHECK(check_state != FINISHED && check_state != CANCELED) << "task already finish";

        if (fragment_ctx->is_canceled()) {
            LOG(INFO) << "llj log fragment_ctx->is_canceled() " << task;
            // 可能由pending finish状态转换而来，应该可以立即cancel
            // 也肯能由block状态转换而来，由其他task通知cancel
            _try_close_task(task, CANCELED);
            continue;
        }

        // task 执行
        bool eos = false;
        LOG(INFO) << "llj log execute task " << task;
        auto status = task->execute(&eos);
        task->set_previous_core_id(index);
        if (!status.ok()) {
            LOG(INFO) << "llj log execute task fail " << task;
            // 执行失败，cancel整个fragment，sr是cancel整个query context。
            fragment_ctx->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "execute fail");
            _try_close_task(task, CANCELED);
            continue;
        }

        if (eos) {
            LOG(INFO) << "llj log execute task end " << task;
            // TODO pipeline 并发情况下，要最后一个task执行完成才执行finalize，并消除父节点的依赖
            status = task->finalize();
            if (!status.ok()) {
                // 执行失败，cancel整个fragment，sr是cancel整个query context。
                fragment_ctx->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, "finalize fail");
                _try_close_task(task, CANCELED);
            } else {
                task->finish_p_dependency(); // 成功，让下游尽快调度，不等close
                _try_close_task(task, FINISHED);
            }
            continue;
        }

        auto pipeline_state = task->get_state();
        LOG(INFO) << "llj log get_state task " << task << " " << pipeline_state;
        switch (pipeline_state) {
        case BLOCKED:
        case PENDING_FINISH:
            _blocked_task_scheduler->add_blocked_task(task);
            break;
        case RUNNABLE:
            queue->push_back(task, index);
            break;
        case FINISHED:
        case CANCELED:
            break;
        default:
            DCHECK(false);
            break;
        }
    }
    LOG(INFO) << "Stop TaskScheduler worker " << index;
}

Status TaskScheduler::_try_close_task(PipelineTask* task, PipelineTaskState state) {
    LOG(INFO) << "llj log _try_close_task " << task;
    // state 只能是CANCELED或FINISHED两种
    if (task->is_pending_finish()) {
        LOG(INFO) << "llj log is_pending_finish " << task;
        //            task->set_previous_core_id(-1);
        task->set_state(PENDING_FINISH);
        _blocked_task_scheduler->add_blocked_task(task);
        return Status::OK();
    } else {
        LOG(INFO) << "llj log is_pending_finish else " << task;
        RETURN_IF_ERROR(task->close());
        task->set_state(state);
        if (state == CANCELED) {
            task->finish_p_dependency();
        }
        RETURN_IF_ERROR(task->fragment_context()->close_a_pipeline());
    }
}

void TaskScheduler::shutdown() {
    if (!this->_shutdown.load()) {
        this->_shutdown.store(true);
        _blocked_task_scheduler->shutdown();
        if (_task_queue) {
            _task_queue->close();
            _task_queue.reset();
        }
        if (_fix_thread_pool) {
            for (const auto& marker : _markers) {
                marker->store(false);
            }
            _fix_thread_pool->shutdown();
            _fix_thread_pool->wait();
            _fix_thread_pool.reset();
        }
    }
}

} // namespace doris::pipeline