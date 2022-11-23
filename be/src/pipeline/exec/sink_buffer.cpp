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

#include "sink_buffer.h"

#include <google/protobuf/stubs/common.h>

#include <atomic>

#include "service/brpc.h"
#include "util/time.h"

namespace doris::pipeline {
// Disposable call back, it must be created on the heap.
// It will destroy itself after call back
// copy from sr
template <typename T, typename C = void>
class DisposableClosure : public google::protobuf::Closure {
public:
    using FailedFunc = std::function<void(const C&)>;
    using SuccessFunc = std::function<void(const C&, const T&)>;

    DisposableClosure(const C& ctx) : _ctx(ctx) {}
    ~DisposableClosure() override = default;
    // Disallow copy and assignment.
    DisposableClosure(const DisposableClosure& other) = delete;
    DisposableClosure& operator=(const DisposableClosure& other) = delete;
    void addFailedHandler(FailedFunc fn) { _failed_handler = std::move(fn); }
    void addSuccessHandler(SuccessFunc fn) { _success_handler = fn; }

    void Run() noexcept override {
        std::unique_ptr<DisposableClosure> self_guard(this);
        try {
            if (cntl.Failed()) {
                LOG(WARNING) << "brpc failed, error=" << berror(cntl.ErrorCode())
                             << ", error_text=" << cntl.ErrorText();
                _failed_handler(_ctx);
            } else {
                _success_handler(_ctx, result);
            }
        } catch (const std::exception& exp) {
            LOG(FATAL) << "[ExchangeSinkOperator] Callback error: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "[ExchangeSinkOperator] Callback error: Unknown";
        }
    }

public:
    brpc::Controller cntl;
    T result;

private:
    const C _ctx;
    FailedFunc _failed_handler;
    SuccessFunc _success_handler;
};

SinkBuffer::SinkBuffer(PUniqueId query_id, PlanNodeId dest_node_id, int send_id,
                       RuntimeState* state)
        : _full_time(0),
          _is_finishing(false),
          _finished_sink(0),
          _query_id(query_id),
          _dest_node_id(dest_node_id),
          _sender_id(send_id),
          _be_number(state->be_number()),
          _state(state) {}

void SinkBuffer::close() {
    for (const auto& pair : _instance_to_request) {
        if (pair.second) {
            pair.second->release_finst_id();
            pair.second->release_query_id();
        }
    }
}

bool SinkBuffer::is_full() const {
    // std::queue' read is concurrent safe without mutex
    // Judgement may not that accurate because we do not known in advance which
    // instance the data to be sent corresponds to
    size_t max_package_size = 64 * _instance_to_package_queue.size();
    size_t total_package_size = 0;
    for (auto& [_, q] : _instance_to_package_queue) {
        total_package_size += q.size();
    }
    const bool is_full = total_package_size > max_package_size;

    int64_t last_full_timestamp = _last_full_timestamp;
    int64_t full_time = _full_time;

    if (is_full && last_full_timestamp == -1) {
        _last_full_timestamp.compare_exchange_weak(last_full_timestamp, MonotonicNanos());
    }
    if (!is_full && last_full_timestamp != -1) {
        // The following two update operations cannot guarantee atomicity as a whole without lock
        // But we can accept bias in estimatation
        _full_time.compare_exchange_weak(full_time,
                                         full_time + (MonotonicNanos() - last_full_timestamp));
        _last_full_timestamp.compare_exchange_weak(last_full_timestamp, -1);
    }

    return is_full;
}

void SinkBuffer::set_finishing() {
    _is_finishing = true;
}

bool SinkBuffer::is_pending_finish() const {
    for (auto& pair : _instance_to_package_queue_mutex) {
        std::lock_guard<std::mutex> lock(*(pair.second));
        auto& id = pair.first;
        if (!_instance_to_sending_by_pipeline.at(id)) {
            return true;
        }
    }
    return false;
}

void SinkBuffer::register_sink(TUniqueId fragment_instance_id) {
    if (_is_finishing) {
        return;
    }
    auto low_id = fragment_instance_id.lo;
    if (_instance_to_package_queue_mutex.count(low_id)) {
        return;
    }
    _instance_to_package_queue_mutex[low_id] = std::make_shared<std::mutex>();
    _instance_to_seq[low_id] = 0;
    _instance_to_package_queue[low_id] = std::queue<TransmitInfo, std::list<TransmitInfo>>();
    PUniqueId finst_id;
    finst_id.set_hi(fragment_instance_id.hi);
    finst_id.set_lo(fragment_instance_id.lo);
    _instance_to_finst_id[low_id] = finst_id;
    _instance_to_sending_by_pipeline[low_id] = true;
}

void SinkBuffer::add_block(TransmitInfo&& request) {
    if (_is_finishing) {
        return;
    }
    TUniqueId ins_id = request.fragment_instance_id;
    bool send_now = false;
    {
        std::lock_guard<std::mutex> lock(*_instance_to_package_queue_mutex[ins_id.lo]);
        // Do not have in process rpc, directly send
        if (_instance_to_sending_by_pipeline[ins_id.lo]) {
            send_now = true;
            _instance_to_sending_by_pipeline[ins_id.lo] = false;
        }
        _instance_to_package_queue[ins_id.lo].emplace(std::move(request));
    }
    if (send_now) {
        _send_rpc(ins_id.lo);
    }
}

// 该方法执行时，某InstanceLoId对应的队列不会存在正在发送的数据
// 要在执行链退出时，将_instance_to_sending_by_pipeline[ins_id.lo] 设置为true
void SinkBuffer::_send_rpc(InstanceLoId id) {
    std::lock_guard<std::mutex> lock(*_instance_to_package_queue_mutex[id]);
    std::queue<TransmitInfo, std::list<TransmitInfo>>& q = _instance_to_package_queue[id];
    if (q.empty() || _is_finishing) {
        // rpc 的链退出了，因此需要让pipeline线程执行_send_rpc方法
        _instance_to_sending_by_pipeline[id] = true;
        return;
    }
    if (!_instance_to_request[id]) {
        // 为啥这一句如果放在q.pop后，request里的block就变成nullptr了？
        construct_request(id);
    }

    TransmitInfo& request = q.front();
    bool eos = request.eos;
    _instance_to_request[id]->set_eos(eos);
    auto& p_block = request.block;
    if (p_block) {
        _instance_to_request[id]->set_allocated_block(p_block.get());
    }
    _instance_to_request[id]->set_packet_seq(_instance_to_seq[id]++);
    auto* _closure = new DisposableClosure<PTransmitDataResult, ClosureContext>({id, eos});
    // 不能直接传入capture this?
    _closure->addFailedHandler([this](const ClosureContext& ctx) {
        _is_finishing = true;
        _state->set_is_cancelled(true);
        {
            std::lock_guard<std::mutex> lock(*_instance_to_package_queue_mutex[ctx.id]);
            _instance_to_sending_by_pipeline[ctx.id] = true;
        }
    });
    _closure->addSuccessHandler(
            [this](const ClosureContext& ctx, const PTransmitDataResult& result) {
                Status s = Status(result.status());
                if (!s.ok()) {
                    _is_finishing = true;
                    _state->set_is_cancelled(true); // cancel pipeline context
                    {
                        std::lock_guard<std::mutex> lock(*_instance_to_package_queue_mutex[ctx.id]);
                        _instance_to_sending_by_pipeline[ctx.id] = true;
                    }
                } else {
                    if (ctx.eos) {
                        std::lock_guard<std::mutex> lock(*_instance_to_package_queue_mutex[ctx.id]);
                        _instance_to_sending_by_pipeline[ctx.id] = true;
                    } else {
                        _send_rpc(ctx.id);
                    }
                }
            });

    //    if (request.is_transfer_chain && eos) {
    //        auto statistic = params->mutable_query_statistics();
    //        _parent->_query_statistics->to_pb(statistic);
    //    }

    _closure->cntl.set_timeout_ms(10000); // 10s

    if (false) {
        //TODO Support HTTP interface
        //    && _parent->_transfer_large_data_by_brpc
        //        _brpc_request.has_block() &&
        //        _brpc_request.block().has_column_values() &&
        //        _brpc_request.ByteSizeLong() > MIN_HTTP_BRPC_SIZE) {
        //        Status st = request_embed_attachment_contain_block<PTransmitDataParams,
        //        RefCountClosure<PTransmitDataResult>>(
        //                &_brpc_request, _closure);
        //        if (!st.ok()) {
        //            _is_finishing = true;
        //        }
        //        std::string brpc_url =
        //                fmt::format("http://{}:{}", _brpc_dest_addr.hostname, _brpc_dest_addr.port);
        //        std::shared_ptr<PBackendService_Stub> _brpc_http_stub =
        //                _state->exec_env()->brpc_internal_client_cache()->get_new_client_no_cache(brpc_url,
        //                                                                                          "http");
        //        _closure->cntl.http_request().uri() =
        //                brpc_url + "/PInternalServiceImpl/transmit_block_by_http";
        //        _closure->cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        //        _closure->cntl.http_request().set_content_type("application/json");
        //        _brpc_http_stub->transmit_block_by_http(&_closure->cntl, NULL, &_closure->result, _closure);
    } else {
        _closure->cntl.http_request().Clear();
        request.stub->transmit_block(&_closure->cntl, _instance_to_request[id].get(),
                                     &_closure->result, _closure);
    }
    if (p_block) {
        _instance_to_request[id]->release_block();
    }

    q.pop();
}

} // namespace doris::pipeline
