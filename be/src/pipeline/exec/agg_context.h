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
    ~AggContext() { close(); }
    vectorized::Block* get_free_block();

    void return_free_block(vectorized::Block*);

    bool has_data_or_finished();
    Status get_block(vectorized::Block** block, bool* eos);

    bool has_enough_space_to_push();
    void push_block(vectorized::Block*);

    void set_finish();
    void set_canceled(); // should set before finish
    bool is_finish();
    void close();

private:
    std::mutex _free_blocks_lock;
    std::vector<vectorized::Block*> _free_blocks;

    std::mutex _transfer_lock;
    std::list<vectorized::Block*> _blocks_queue;

    bool _is_finished = false;
    bool _is_canceled = false;
    int64_t _cur_bytes_in_queue = 0;
    int64_t _max_bytes_in_queue = 1024 * 1024 * 1024 / 10; // TODO pipeline
};

} // namespace pipeline
} // namespace doris