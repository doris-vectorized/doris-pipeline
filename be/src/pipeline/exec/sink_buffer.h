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

#include <parallel_hashmap/phmap.h>

#include <list>
#include <queue>
#include <shared_mutex>

#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace pipeline {
using InstanceLoId = int64_t;
struct TransmitInfo {
    TUniqueId fragment_instance_id;
    bool is_http; // TODO
    std::shared_ptr<PBackendService_Stub> stub;
    std::shared_ptr<PBlock> block;
    bool is_transfer_chain;
    bool eos = false;
    // TODO _query_statistics
};

struct ClosureContext {
    InstanceLoId id;
    bool eos;
};

// Each ExchangeSinkOperator have one SinkBuffer
class SinkBuffer {
public:
    SinkBuffer(PUniqueId, int, PlanNodeId, RuntimeState*);
    ~SinkBuffer() = default;
    void register_sink(TUniqueId);
    void add_block(const TransmitInfo& request);
    bool is_full() const;

    void set_finishing();
    bool is_pending_finish() const;

    void close();

private:
    // TODO: rethink here should use std::shared_ptr, seems useless
    phmap::flat_hash_map<InstanceLoId, std::shared_ptr<std::mutex>>
            _instance_to_package_queue_mutex;
    phmap::flat_hash_map<InstanceLoId, std::queue<TransmitInfo, std::list<TransmitInfo>>>
            _instance_to_package_queue;
    using PackageSeq = int64_t;
    // must init zero
    phmap::flat_hash_map<InstanceLoId, PackageSeq> _instance_to_seq;
    phmap::flat_hash_map<InstanceLoId, std::unique_ptr<PTransmitDataParams>> _instance_to_request;
    phmap::flat_hash_map<InstanceLoId, PUniqueId> _instance_to_finst_id;
    //    phmap::flat_hash_map<InstanceLoId, PlanNodeId> _instance_to_dest_node_id;
    phmap::flat_hash_map<InstanceLoId, bool> _instance_to_sending_by_pipeline;

    mutable std::atomic<int64_t> _last_full_timestamp = -1;
    mutable std::atomic<int64_t> _full_time = 0;

    std::atomic<bool> _is_finishing;
    std::atomic<int> _finished_sink;
    PUniqueId _query_id;
    PlanNodeId _dest_node_id;
    // Sender instance id, unique within a fragment. StreamSender save the variable
    int _sender_id;
    int _be_number;

    RuntimeState* _state;

private:
    void _send_rpc(InstanceLoId);
    // must hold the _instance_to_package_queue_mutex[id] mutex to opera
    void construct_request(InstanceLoId id) {
        _instance_to_request[id] = std::make_unique<PTransmitDataParams>();
        _instance_to_request[id]->set_allocated_finst_id(&_instance_to_finst_id[id]);
        _instance_to_request[id]->set_allocated_query_id(&_query_id);

        _instance_to_request[id]->set_node_id(_dest_node_id);
        _instance_to_request[id]->set_sender_id(_sender_id);
        _instance_to_request[id]->set_be_number(_be_number);
    }
};

} // namespace pipeline
} // namespace doris