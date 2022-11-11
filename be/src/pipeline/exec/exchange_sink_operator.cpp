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

#include "exchange_sink_operator.h"

#include "gen_cpp/internal_service.pb.h"
#include "sink_buffer.h"
#include "util/brpc_client_cache.h"
#include "vec/exprs/vexpr.h"
#include "vec/runtime/vpartition_info.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::pipeline {

ExchangeSinkOperator::ExchangeSinkOperator(OperatorTemplate* operator_template,
                                           vectorized::VDataStreamSender* sink)
        : Operator(operator_template),
          _part_type(sink->_part_type),
          _pool(sink->_pool),
          _sink(sink),
          _current_channel_idx(0),
          _transfer_large_data_by_brpc(sink->_transfer_large_data_by_brpc) {}
ExchangeSinkOperator::~ExchangeSinkOperator() = default;

Status ExchangeSinkOperator::init(const ExecNode* exec_node, RuntimeState* state) {
    RETURN_IF_ERROR(Operator::init(exec_node, state));
    _state = state;
    return Status::OK();
}

Status ExchangeSinkOperator::init(const TDataSink& tsink) {
    // partition infos
    const TDataStreamSink& t_stream_sink = tsink.stream_sink;
    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                _pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
    } else if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        // Range partition
        // Partition Exprs
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                _pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
        // Partition infos
        int num_parts = t_stream_sink.output_partition.partition_infos.size();
        if (num_parts == 0) {
            return Status::InternalError("Empty partition info.");
        }
        for (int i = 0; i < num_parts; ++i) {
            vectorized::VPartitionInfo* info = _pool->add(new vectorized::VPartitionInfo());
            RETURN_IF_ERROR(vectorized::VPartitionInfo::from_thrift(
                    _pool, t_stream_sink.output_partition.partition_infos[i], info));
            _partition_infos.push_back(info);
        }
        // partitions should be in ascending order
        std::sort(_partition_infos.begin(), _partition_infos.end(),
                  [](const vectorized::VPartitionInfo* v1, const vectorized::VPartitionInfo* v2) {
                      return v1->range() < v2->range();
                  });
    } else {
        // UNPARTITIONED
    }

    // sink buffer and Channel PUniqueId query_id, int send_id, RuntimeState* state
    PUniqueId query_id;
    query_id.set_hi(_state->query_id().hi);
    query_id.set_lo(_state->query_id().lo);
    _sink_buffer = std::make_unique<SinkBuffer>(query_id, t_stream_sink.dest_node_id,
                                                _sink->_sender_id, _state);

    std::map<int64_t, int64_t> fragment_id_to_channel_index;
    for (auto iter = _sink->_channel_shared_ptrs.begin(); iter != _sink->_channel_shared_ptrs.end();
         iter++) {
        auto fragment_instance_id = (*iter)->_fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) ==
            fragment_id_to_channel_index.end()) {
            _channel_shared_ptrs.emplace_back(
                    new Channel(this, fragment_instance_id, (*iter)->_brpc_dest_addr));
            fragment_id_to_channel_index.emplace(fragment_instance_id.lo,
                                                 _channel_shared_ptrs.size() - 1);
            _channels.push_back(_channel_shared_ptrs.back().get());
        } else {
            _channel_shared_ptrs.emplace_back(
                    _channel_shared_ptrs[fragment_id_to_channel_index[fragment_instance_id.lo]]);
        }
    }
    return Status::OK();
}

Status ExchangeSinkOperator::prepare(RuntimeState* state) {
    std::vector<std::string> instances;
    for (const auto& channel : _sink->_channels) {
        instances.emplace_back(channel->get_fragment_instance_id_str());
    }
    _mem_tracker = std::make_unique<MemTracker>(
            "ExchangeSinkOperator:" + print_id(state->fragment_instance_id()),
            _runtime_profile.get());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    if (_part_type == TPartitionType::UNPARTITIONED || _part_type == TPartitionType::RANDOM) {
        // sink 已经shuffle了，这里按照sink的channels初始化的
        //        std::random_device rd;
        //        std::mt19937 g(rd());
        //        shuffle(_channels.begin(), _channels.end(), g);
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        if (_state->query_options().__isset.enable_new_shuffle_hash_method) {
            _new_shuffle_hash_method = _state->query_options().enable_new_shuffle_hash_method;
        }

        RETURN_IF_ERROR(vectorized::VExpr::prepare(_partition_expr_ctxs, state, _sink->_row_desc));
    } else {
        RETURN_IF_ERROR(vectorized::VExpr::prepare(_partition_expr_ctxs, state, _sink->_row_desc));
        for (auto iter : _partition_infos) {
            RETURN_IF_ERROR(iter->prepare(state, _sink->_row_desc));
        }
    }

    for (auto* channel : _channels) {
        RETURN_IF_ERROR(channel->init(state));
        _sink_buffer->register_sink(channel->_fragment_instance_id);
    }
    return Status::OK();
}

Status ExchangeSinkOperator::open(RuntimeState* state) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    RETURN_IF_ERROR(vectorized::VExpr::open(_partition_expr_ctxs, state));
    for (auto iter : _partition_infos) {
        RETURN_IF_ERROR(iter->open(state));
    }
    _compression_type = state->fragement_transmission_compression_type();
    return Status::OK();
}

bool ExchangeSinkOperator::can_write() {
    return !_sink_buffer->is_full();
}

Status ExchangeSinkOperator::serialize_block(vectorized::Block* src, PBlock* dest,
                                             int num_receivers) {
    dest->Clear();
    size_t uncompressed_bytes = 0, compressed_bytes = 0;
    RETURN_IF_ERROR(src->serialize(_state->be_exec_version(), dest, &uncompressed_bytes,
                                   &compressed_bytes, _compression_type,
                                   _transfer_large_data_by_brpc));
    return Status::OK();
}

Status ExchangeSinkOperator::finalize(RuntimeState* state) {
    int num_channels = _channels.size();
    for (int i = 0; i < num_channels; ++i) {
        RETURN_IF_ERROR(_channels[i]->flush_block(true));
    }
    return Status::OK();
}

Status ExchangeSinkOperator::sink(RuntimeState* state, vectorized::Block* block, bool eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    if (_part_type == TPartitionType::UNPARTITIONED || _channels.size() == 1) {
        // 1. serialize depends on it is not local exchange
        // 2. send block
        // 3. rollover block
        //        int local_size = 0;
        //        for (auto channel : _channels) {
        //            if (channel->is_local()) local_size++;
        //        }
        //        if (local_size == _channels.size()) {
        // TODO  local exchange
        //            for (auto channel : _channels) {
        //                RETURN_IF_ERROR(channel->send_local_block(block));
        //            }
        //        } else {

        std::shared_ptr<PBlock> p_block;
        if (block && block->rows() > 0) {
            p_block.reset(new PBlock());
            RETURN_IF_ERROR(serialize_block(block, p_block.get(), _channels.size()));
        }
        for (auto channel : _channels) {
            RETURN_IF_ERROR(channel->send_block(p_block, eos));
        }
        //        }
    } else if (_part_type == TPartitionType::RANDOM) {
        std::shared_ptr<PBlock> p_block;
        if (block && block->rows() > 0) {
            p_block.reset(new PBlock());
            RETURN_IF_ERROR(serialize_block(block, p_block.get(), _channels.size()));
        }
        // 1. select channel
        auto* current_channel = _channels[_current_channel_idx];
        RETURN_IF_ERROR(current_channel->send_block(p_block, eos));
        _current_channel_idx = (_current_channel_idx + 1) % _channels.size();
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        // will only copy schema
        // we don't want send temp columns
        auto column_to_keep = block->columns();

        int result_size = _partition_expr_ctxs.size();
        int result[result_size];
        RETURN_IF_ERROR(get_partition_column_result(block, result));

        // vectorized calculate hash
        int rows = block->rows();
        auto element_size = _channels.size();
        std::vector<uint64_t> hash_vals(rows);
        auto* __restrict hashes = hash_vals.data();

        // TODO: after we support new shuffle hash method, should simple the code
        if (_part_type == TPartitionType::HASH_PARTITIONED) {
            if (!_new_shuffle_hash_method) {
                // for each row, we have a siphash val
                std::vector<SipHash> siphashs(rows);
                // result[j] means column index, i means rows index
                for (int j = 0; j < result_size; ++j) {
                    block->get_by_position(result[j]).column->update_hashes_with_value(siphashs);
                }
                for (int i = 0; i < rows; i++) {
                    hashes[i] = siphashs[i].get64() % element_size;
                }
            } else {
                // result[j] means column index, i means rows index, here to calculate the xxhash value
                for (int j = 0; j < result_size; ++j) {
                    block->get_by_position(result[j]).column->update_hashes_with_value(hashes);
                }

                for (int i = 0; i < rows; i++) {
                    hashes[i] = hashes[i] % element_size;
                }
            }

            vectorized::Block::erase_useless_column(block, column_to_keep);
            RETURN_IF_ERROR(channel_add_rows(_channels, element_size, hashes, rows, block, eos));
        } else {
            for (int j = 0; j < result_size; ++j) {
                block->get_by_position(result[j]).column->update_crcs_with_value(
                        hash_vals, _partition_expr_ctxs[j]->root()->type().type);
            }
            element_size = _channel_shared_ptrs.size();
            for (int i = 0; i < rows; i++) {
                hashes[i] = hashes[i] % element_size;
            }

            vectorized::Block::erase_useless_column(block, column_to_keep);
            RETURN_IF_ERROR(
                    channel_add_rows(_channel_shared_ptrs, element_size, hashes, rows, block, eos));
        }
    } else {
        // Range partition
        // 1. calculate range
        // 2. dispatch rows to channel
    }
    if(block) {
        _num_rows_returned += block->rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
    return Status::OK();
}

bool ExchangeSinkOperator::is_pending_finish() {
    return _sink_buffer->is_pending_finish();
}

Status ExchangeSinkOperator::close(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::close(state));
    _sink_buffer->close();
    return Status::OK();
}

template <typename Channels>
Status ExchangeSinkOperator::channel_add_rows(Channels& channels, int num_channels,
                                              uint64_t* __restrict channel_ids, int rows,
                                              vectorized::Block* block, bool eos) {
    std::vector<int> channel2rows[num_channels];
    for (int i = 0; i < rows; ++i) {
        channel2rows[channel_ids[i]].emplace_back(i);
    }
    for (int i = 0; i < num_channels; ++i) {
        if (!channel2rows[i].empty()) {
            RETURN_IF_ERROR(channels[i]->add_rows(block, channel2rows[i]));
        }
    }
    return Status::OK();
}

///////////////

ExchangeSinkOperator::Channel::Channel(ExchangeSinkOperator* parent, TUniqueId instance_id,
                                       const TNetworkAddress& brpc_dest)
        : _parent(parent), _fragment_instance_id(instance_id), _brpc_dest_addr(brpc_dest) {}

Status ExchangeSinkOperator::Channel::init(RuntimeState* state) {
    if (_brpc_dest_addr.hostname == BackendOptions::get_localhost()) {
        _brpc_stub = state->exec_env()->brpc_internal_client_cache()->get_client(
                "127.0.0.1", _brpc_dest_addr.port);
    } else {
        _brpc_stub = state->exec_env()->brpc_internal_client_cache()->get_client(_brpc_dest_addr);
    }

    if (!_brpc_stub) {
        std::string msg = fmt::format(
                "ExchangeSinkOperator::Channel::init Get rpc stub failed, dest_addr={}:{}",
                _brpc_dest_addr.hostname, _brpc_dest_addr.port);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    _state = state;
    return Status::OK();
}

Status ExchangeSinkOperator::Channel::send_block(std::shared_ptr<PBlock> block, bool eos) {
    if (eos) { // 非频繁判断
        if (_eos_send) {
            LOG(INFO) << "eos send ";
            return Status::OK();
        } else {
            _eos_send = true;
        }
    }
    // 这里block已经拿到了所有权
    _parent->_sink_buffer->add_block({_fragment_instance_id, // 不用每次都拷贝
                                      false, _brpc_stub, std::move(block), false, eos});
    return Status::OK();
}

Status ExchangeSinkOperator::Channel::add_rows(vectorized::Block* block,
                                               const std::vector<int>& rows) {
    int row_wait_add = rows.size();
    int batch_size = _parent->state()->batch_size();
    const int* begin = &rows[0];

    while (row_wait_add > 0) {
        if (!_mutable_block) {
            _mutable_block = std::make_unique<vectorized::MutableBlock>(block->clone_empty());
        }
        int row_add = 0;
        int max_add = batch_size - _mutable_block->rows();
        if (row_wait_add >= max_add) {
            row_add = max_add;
        } else {
            row_add = row_wait_add;
        }

        _mutable_block->add_rows(block, begin, begin + row_add);

        row_wait_add -= row_add;
        begin += row_add;

        if (row_add == max_add) {
            RETURN_IF_ERROR(flush_block());
        }
    }
    return Status::OK();
}

Status ExchangeSinkOperator::Channel::flush_block(bool eos) {
    std::shared_ptr<PBlock> block_ptr;
    if (_mutable_block) {
        block_ptr.reset(new PBlock()); // 对象池？
        auto block = _mutable_block->to_block();
        RETURN_IF_ERROR(_parent->serialize_block(&block, block_ptr.get()));
        block.clear_column_data();
        _mutable_block->set_muatable_columns(block.mutate_columns());
    }
    RETURN_IF_ERROR(send_block(std::move(block_ptr), eos));
    return Status::OK();
}

} // namespace doris::pipeline
