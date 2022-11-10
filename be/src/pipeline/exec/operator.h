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

#include <utility>

#include "common/status.h"
#include "exec/exec_node.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "vec/core/block.h"

namespace doris::pipeline {

// 1. 每次scan结束和sink结束后去测试pipeline是否可以调度
//    scan线程、sink线程、执行线程都会对调度数据结构(全局调度队列)进行修改，冲突线程数为scan线程数+sink线程数+woker线程数，由于sink
//    线程数较多，因此sink间并发可能受影响
// 2. pipeline从scan拿数据时，如果拿到的结果是NO_MORE_DATA，则注册scan回调
//            向sink发送数据时，如果拿到的结果是SINK_BUSY，则注册sink回调
//    还没有类似实现
//    回调方式的坏处？？
// 3. 每个pipeline的scan和sink都由一个transform线程阻塞控制，scan transform拿到数据后测试pipeline是否可以调度，sink transform线程
//    发送数据后测试pipeline是否可以调度
//    好处：scan和sink不会被阻塞
//    坏处：冲突线程数变成了pipeline个数 * 2 + worker个数；会产生大量的阻塞线程
// 4. 参考Starrocks，由单独的线程负责调度（浪费一个核心频繁探测）【暂定方案】
//    好处：有既定实现可以参考
//    坏处：限制思路；调度线程可能成为性能瓶颈
// 5. pipeline的task负责部分调度（处理pipeline间依赖关系，如最后一个pipeline task负责将它依赖的pipeline task加入调度队列）
//    + 方法4

// 从source获取数据后的执行结果，初始为NO_MORE_DATA，如果
enum class SourceState : uint8_t {
    NO_MORE_DATA = 0, // 没有数据
    MORE_DATA = 1,    // 还可以继续拉数据
    FINISHED = 2      // 表示source的数据已经全部读取完毕
};

//
enum class SinkState : uint8_t {
    SINK_IDLE = 0, // 可以向sink中继续发送数据
    SINK_BUSY = 1, // sink已经满了，需要等sink将数据发送一部分
    FINISHED = 2   //表示sink不需要更多数据进入
};
////////////////        暂时用不到上面        ////////////////

class OperatorTemplate;
class Operator;

using OperatorPtr = std::shared_ptr<Operator>;
using Operators = std::vector<OperatorPtr>;

class Operator {
public:
    explicit Operator(OperatorTemplate* operator_template);
    virtual ~Operator() = default; // shared ptr要求析构父类时，能够调用正确的子类析构函数

    // sink 和 source都要感知cancel的情况，再cancel时及时终止
    bool is_sink() const;

    bool is_source() const;

    // 初始化一些对象，在构造函数中并能失败，在这里可以失败
    // init profile todo
    // 被ExecNode构造出来后就调用该方法
    // 子类应该先执行Operator::init
    virtual Status init(const ExecNode* exec_node, RuntimeState* state = nullptr);

    // 参照ExecNode，准备需要的数据结构，比如profile
    // 在rpc中调用
    virtual Status prepare(RuntimeState* state);

    // 参照ExecNode，在pipeline task首次执行时调用，不能阻塞，不能中断
    // 在pipeline前置依赖都结束时执行open
    // a 依赖 c & b 依赖 c，则a、b都执行完成后，在worker中执行c的open
    // 第一版pipeline，doris的pipeline中只有一个task，因此这样操作不会是性能瓶颈，但是如果一个pipeline是多个
    // 并发task，则要考虑
    // sr 将一些open的行为放到了prepare中
    virtual Status open(RuntimeState* state);

    // 释放资源，不能阻塞，不能中断，
    virtual Status close(RuntimeState* state);

    Status set_child(OperatorPtr child) {
        if (is_source()) {
            return Status::InternalError("source can not has child.");
        }
        _child = std::move(child);
        return Status::OK();
    }

    // 没有scanner运行或有未读取的block
    virtual bool can_read() { return false; } // for source

    virtual bool can_write() { return false; } // for sink

    // for pipeline
    virtual Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
        std::stringstream error_msg;
        error_msg << " has not implements get_block";
        return Status::NotSupported(error_msg.str());
    }

    // return can write continue
    virtual Status sink(RuntimeState* state, vectorized::Block* block, bool eos) {
        std::stringstream error_msg;
        error_msg << " not a sink ";
        return Status::NotSupported(error_msg.str());
    }

    virtual Status finalize(RuntimeState* state) {
        std::stringstream error_msg;
        error_msg << " not a sink, can not finalize";
        return Status::NotSupported(error_msg.str());
    }

    // close被调用且
    // - 对于source来说，scan线程未全部退出
    // - 对于rpc sink来说，rpc未全部处理完成
    // - 否则返回false
    virtual bool is_pending_finish() { return false; }

    // close且非pending_finish
    // virtual bool is_finished() = 0;

    bool is_closed() const { return _is_closed; }

    MemTracker* mem_tracker() const { return _mem_tracker.get(); }

    /// Only use in vectorized exec engine to check whether reach limit and cut num row for block
    // and add block rows for profile
    // 参考ExecNode的reached_limit
    void reached_limit(vectorized::Block* block, bool* eos);

    const OperatorTemplate* operator_template() const { return _operator_template; }

    const RowDescriptor& row_desc();

    int64_t rows_returned() const { return _num_rows_returned; }

protected:
    std::unique_ptr<MemTracker> _mem_tracker;

    OperatorTemplate* _operator_template;
    // source has no child
    // if an operator is not source, it will get data from its child.
    OperatorPtr _child;

    std::unique_ptr<RuntimeProfile> _runtime_profile;
    int64_t _num_rows_returned;
    int64_t _limit;

private:
    bool _is_closed = false;
};

class OperatorTemplate {
public:
    OperatorTemplate(int32_t id, const std::string& name, ExecNode* exec_node = nullptr)
            : _id(id), _name(name), _related_exec_node(exec_node) {}

    virtual ~OperatorTemplate() = default;

    virtual OperatorPtr build_operator() = 0;

    virtual bool is_sink() const { return false; }
    virtual bool is_source() const { return false; }

    // 处理所有operator共有的对象
    virtual Status prepare(RuntimeState* state);
    // 关闭所有operator共有的对象
    virtual void close(RuntimeState* state);

    std::string get_name() const { return _name; }

    RuntimeState* runtime_state() { return _state; }

    const RowDescriptor& row_desc() { return _related_exec_node->row_desc(); }

    ExecNode* exec_node() const { return _related_exec_node; }

    int32_t id() const { return _id; }

protected:
    const int32_t _id;
    const std::string _name;
    ExecNode* _related_exec_node;
    std::shared_ptr<RuntimeProfile> _runtime_profile;

    RuntimeState* _state = nullptr;
    bool _is_closed = false;
};

using OperatorTemplatePtr = std::shared_ptr<OperatorTemplate>;
using OperatorTemplates = std::vector<OperatorTemplatePtr>;

} // namespace doris::pipeline
