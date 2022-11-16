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

#include "operator.h"

namespace doris {
namespace vectorized {
class VDataStreamSender;
class VResultSink;
class VPartitionInfo;
} // namespace vectorized

namespace pipeline {
class SinkBuffer;

// Now local exchange is not supported since VDataStreamRecvr is considered as a pipeline broker.
class ExchangeSinkOperator : public Operator {
public:
    ExchangeSinkOperator(OperatorTemplate* operator_template, vectorized::VDataStreamSender* sink);
    ~ExchangeSinkOperator();
    Status init(const ExecNode* exec_node, RuntimeState* state = nullptr) override;
    Status init(const TDataSink& tsink) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    bool can_write() override;
    Status sink(RuntimeState* state, vectorized::Block* block, bool eos) override;
    bool is_pending_finish() override;
    Status finalize(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    RuntimeState* state() { return _state; }

    Status serialize_block(vectorized::Block* src, PBlock* dest, int num_receivers = 1);

private:
    class Channel;
    bool _new_shuffle_hash_method = false;
    TPartitionType::type _part_type;
    ObjectPool* _pool;
    std::vector<vectorized::VExprContext*> _partition_expr_ctxs;
    std::vector<vectorized::VPartitionInfo*> _partition_infos;
    std::unique_ptr<SinkBuffer> _sink_buffer;
    vectorized::VDataStreamSender* _sink;
    RuntimeState* _state = nullptr;

    std::vector<Channel*> _channels;
    std::vector<std::shared_ptr<Channel>> _channel_shared_ptrs;
    int _current_channel_idx; // index of current channel to send to if _random == true
    segment_v2::CompressionTypePB _compression_type;
    bool _transfer_large_data_by_brpc = false;

private:
    template <typename Channels>
    Status channel_add_rows(Channels& channels, int num_channels, const uint64_t* channel_ids,
                            int rows, vectorized::Block* block, bool eos);

    // copy from vdata_stream_sender.h
    Status get_partition_column_result(vectorized::Block* block, int* result) const {
        int counter = 0;
        for (auto ctx : _partition_expr_ctxs) {
            RETURN_IF_ERROR(ctx->execute(block, &result[counter++]));
        }
        return Status::OK();
    }
};

class ExchangeSinkOperator::Channel {
public:
    friend class ExchangeSinkOperator;
    Channel(ExchangeSinkOperator* parent, TUniqueId instance_id, const TNetworkAddress& brpc_dest);
    Status init(RuntimeState* state);

    // Asynchronously sends a block
    // Returns the status of the most recently finished transmit_data
    // rpc (or OK if there wasn't one that hasn't been reported yet).
    // if batch is nullptr, send the eof packet
    Status send_block(std::shared_ptr<PBlock> block, bool eos = false);

    // Add some rows of block to _mutable_block，
    Status add_rows(vectorized::Block*, const std::vector<int>&);

    // send _mutable_block
    Status flush_block(bool eos = false);

private:
    ExchangeSinkOperator* _parent;
    TUniqueId _fragment_instance_id;
    // 插入数据时，先插入该block，block满或eof时，将数据拷贝到sink buffer，并reset.
    // 用于 hash
    std::unique_ptr<vectorized::MutableBlock> _mutable_block;

    TNetworkAddress _brpc_dest_addr;
    std::shared_ptr<PBackendService_Stub> _brpc_stub;
    RuntimeState* _state;
    bool _eos_send = false;
};

class ExchangeSinkOperatorTemplate : public OperatorTemplate {
public:
    ExchangeSinkOperatorTemplate(int32_t id, const std::string& name, ExecNode* exec_node,
                                 vectorized::VDataStreamSender* sink)
            : OperatorTemplate(id, name, exec_node), _sink(sink) {}

    bool is_sink() const override { return true; }

    OperatorPtr build_operator() override {
        return std::make_shared<ExchangeSinkOperator>(this, _sink);
    }

private:
    vectorized::VDataStreamSender* _sink;
};

} // namespace pipeline
} // namespace doris
