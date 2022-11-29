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

#include "common/status.h"

namespace doris {
class TupleDescriptor;
namespace vectorized {
class Block;
}
namespace pipeline {

class AggContext {
public:
    AggContext() = default;
    ~AggContext() { DCHECK(_is_finished); }

    std::unique_ptr<vectorized::Block> get_free_block();

    void return_free_block(std::unique_ptr<vectorized::Block>);

    bool has_data_or_finished();
    Status get_block(std::unique_ptr<vectorized::Block>* block);

    bool has_enough_space_to_push();
    void push_block(std::unique_ptr<vectorized::Block>);

    void set_finish();
    void set_canceled(); // should set before finish
    bool is_finish();

    bool data_exhausted() { return _data_exhausted; }

private:
    std::mutex _free_blocks_lock;
    std::vector<std::unique_ptr<vectorized::Block>> _free_blocks;

    std::mutex _transfer_lock;
    std::list<std::unique_ptr<vectorized::Block>> _blocks_queue;

    bool _data_exhausted = false;
    bool _is_finished = false;
    bool _is_canceled = false;
    int64_t _cur_bytes_in_queue = 0;
    static constexpr int64_t MAX_BYTE_OF_QUEUE = 1024l * 1024 * 1024 / 10;
};

} // namespace pipeline
} // namespace doris