/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class ProjectionExecutor : public AbstractExecutor
{
private:
    std::unique_ptr<AbstractExecutor> prev_; // 投影节点的儿子节点
    /*
    在查询执行计划中，投影操作通常是位于查询计划的顶层，它的作用是选择和投影出特定的列或字段。
    子节点是指在查询计划中位于投影操作之前的其他操作节点，它们的输出结果将作为投影操作的输入。
    子节点的作用是提供数据源或进行数据转换，为投影操作提供所需的数据。
    子节点可以是扫描操作（如表扫描或索引扫描）、过滤操作（如谓词过滤）或其他转换操作（如连接、聚合等）。
    子节点的输出将作为投影操作的输入，投影操作将从子节点的输出中选择指定的列，并进行投影操作，生成最终的结果。
    */
    std::vector<ColMeta> cols_; // 需要投影的字段
    size_t len_;                // 字段总长度
    std::vector<size_t> sel_idxs_;
    //std::unique_ptr<RmRecord> curr_record_;

public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols)
    {
        prev_ = std::move(prev);
        //std::unique_ptr<SeqScanExecutor> prev_after_choose=prev_;
        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols)
        {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    void beginTuple() override
    {
        prev_->beginTuple();
    }

    void nextTuple() override
    {
        prev_->nextTuple();
        _abstract_rid=prev_->_abstract_rid;
        //curr_record_ = Next();
    }

    std::unique_ptr<RmRecord> Next() override
    {

        /*if (curr_record_)
        {
            return std::move(curr_record_);
        }*/

        std::unique_ptr<RmRecord> record = prev_->Next();

        if (record)
        {
            // 创建一个向量来存储投影后的记录数据
            char *data = new char[len_]; // 分配内存

            // 遍历选定的列，并复制对应的数据
            for (size_t i = 0; i < sel_idxs_.size(); ++i)
            {
                size_t sel_idx = sel_idxs_[i];
                size_t offset = cols_[i].offset;
                size_t origin_offset=prev_->cols()[sel_idx].offset;
                char* dest=data+offset;
                char* src=record->data + origin_offset;
                size_t length=cols_[i].len;
                std::memcpy(dest, src, length);
            }

            // 使用投影后的数据创建一个新的 RmRecord，并返回它
            auto temp=std::make_unique<RmRecord>(len_, data);
            return temp;
        }
        return nullptr;
    }

    bool is_end() const override{
        return prev_->_abstract_rid.slot_no==-1;
    }

    const std::vector<ColMeta> &cols() const override {
    // 提供适当的实现，返回具体的 ColMeta 对象或者 std::vector<ColMeta>
        return cols_;
    }

    Rid &rid() override { return _abstract_rid; }
};