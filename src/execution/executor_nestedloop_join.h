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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    const std::vector<ColMeta> &cols() const override {
    // 提供适当的实现，返回具体的 ColMeta 对象或者 std::vector<ColMeta>
        return cols_;
    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
        isend = (left_->is_end() && right_->is_end());  // 支持与空表做join
    }

    void nextTuple() override {
        //对每个左节点遍历右节点,
        if (! right_->is_end()){   // 右边不是最后一个节点，直接取下一个即可
            right_ -> nextTuple();

            // 判断当前是不是最后一个
            if (right_->is_end()){
                right_->beginTuple();
                left_->nextTuple();
            }
        }
        if(left_ -> is_end()){
            isend = true;           
        }
    }

    std::unique_ptr<RmRecord> Next() override {

        if (isend){return nullptr;}     // 虽然这句话不可能执行
            
        std::unique_ptr<RmRecord> left_record = left_->Next();
        std::unique_ptr<RmRecord> right_record = right_->Next();

        if (left_record == nullptr || right_record == nullptr){
            return nullptr;
        }
        // 构造新的记录并返回
        

        char* left_data = left_record->data;
        char* right_data = right_record->data;
        // 将左右记录的数据拷贝到新的记录中

        char* data = new char[len_];

        memcpy(data, left_data, left_->tupleLen());
        memcpy(data + left_->tupleLen(), right_data, right_->tupleLen());
        std::unique_ptr<RmRecord> result_record = std::make_unique<RmRecord>(len_, data);

        return result_record;
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override{
        return isend;
    }
    
    //自己加的

    // size_t tupleLen() const override {
    //     return 0;
    // }
    
    // std::vector<ColMeta> &cols() const override {
    //     std::vector<ColMeta> *_cols = nullptr;
    //     return *_cols;
    // };
};