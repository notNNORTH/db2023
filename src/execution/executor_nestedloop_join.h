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

    std::vector<ColMeta> cols_check_;   // 条件语句中所有用到列的列的元数据信息--by 星穹铁道高手

    int block_size;         // 内存缓冲区的大小     --by 星穹铁道高手
    std::vector<RmRecord> buffer;    // 内存缓冲区 --by 星穹铁道高手
    int left_tuple_index;   // 现在在buffer中的位置 --by 星穹铁道高手
    bool is_last_block;     // 是否为最后一个块     --by 星穹铁道高手
    std::unique_ptr<RmRecord> right_record; // 当前右边的块         --by 星穹铁道高手

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


                /****************by 星穹铁道高手***************/
        // fed_conds_ = conds_;    //后续查询优化可以从fedcond入手!!!!!!!!!!!!!!!!!!!
        //初始化条件语句中所有用到列的列的元数据信息
   
        int con_size = fed_conds_.size();   // 判断条件的个数

        block_size = 1000;      // 初始化为最大1000个元组的缓冲区
        right_record = nullptr;

        auto my_left_cols = left_->cols();
        int left_size = my_left_cols.size();       // 左子树元组总数
        auto my_right_cols = right_->cols();
        int right_size = my_right_cols.size();       // 左子树元组总数

        for (int loop = 0; loop < con_size; loop++) {
            auto temp_con = fed_conds_[loop];
            // 检查左操作数是否为列操作数
            if (!temp_con.lhs_col.tab_name.empty() && !temp_con.lhs_col.col_name.empty()) {
                // 查找colmeta
                for(int i = 0; i < left_size; i++){     //后续join也可能改tab，加入多表内容
                    if((temp_con.lhs_col.tab_name == my_left_cols[i].tab_name) 
                                && (temp_con.lhs_col.col_name == my_left_cols[i].name)){
                        auto temp = my_left_cols[i];
                        cols_check_.push_back(temp);
                    }
                }
                for(int i = 0; i < right_size; i++){     //后续join也可能改tab，加入多表内容
                    if((temp_con.lhs_col.tab_name == my_right_cols[i].tab_name) 
                                && (temp_con.lhs_col.col_name == my_right_cols[i].name)){
                        auto temp = my_right_cols[i];
                        cols_check_.push_back(temp);
                    }
                }
            }
            // 检查右操作数是否为列操作数
            if (!temp_con.is_rhs_val && !temp_con.rhs_col.tab_name.empty() && !temp_con.rhs_col.col_name.empty()) {
                // 查找right_colmeta
                for(int i = 0; i < left_size; i++){//后续join也可能改tab，加入多表内容
                    if((temp_con.rhs_col.tab_name == my_left_cols[i].tab_name) 
                                && (temp_con.rhs_col.col_name == my_left_cols[i].name)){
                        auto temp = my_left_cols[i];
                        cols_check_.push_back(temp);
                    }
                }
                for(int i = 0; i < right_size; i++){//后续join也可能改tab，加入多表内容
                    if((temp_con.rhs_col.tab_name == my_right_cols[i].tab_name) 
                                && (temp_con.rhs_col.col_name == my_right_cols[i].name)){
                        auto temp = my_right_cols[i];
                        cols_check_.push_back(temp);
                    }
                }
            }
        }   
    }

    const std::vector<ColMeta> &cols() const override {
    // 提供适当的实现，返回具体的 ColMeta 对象或者 std::vector<ColMeta>
        return cols_;
    }

    void beginTuple() override {
        /*  嵌套循环连接算法（Block Nested-Loop-Join） */
        set_buffer(true);
        right_->beginTuple();
        right_record = right_->Next();
        // left_tuple_index = 0;
        isend = (right_->is_end() || left_tuple_index == buffer.size());
    }

    void nextTuple() override {
        //对每个左节点遍历右节点,
        left_tuple_index++;

        if (left_tuple_index < buffer.size()){
            return;
        }
        
        right_->nextTuple();
        if ( !right_->is_end() ){
            right_record = right_->Next();
            if (!right_record){
                right_->beginTuple();
                right_record = right_->Next();

                if(is_last_block){
                    isend = true;
                    return;
                }
                
                set_buffer(false);
                right_->beginTuple();
                right_record = right_->Next();
                return;
            }
            left_tuple_index = 0;
            return;
        }
        
        
        if(is_last_block){
            isend = true;
            return;
        }
        
        set_buffer(false);
        right_->beginTuple();
        right_record = right_->Next();
    }

    std::unique_ptr<RmRecord> Next() override {

        if (isend){return nullptr;}     // 虽然这句话不可能执行
        if (buffer.size() == 0){ return nullptr; }
        
        // 构造新的记录并返回
        for(; !isend; nextTuple()){
            auto left_record = buffer[left_tuple_index];
            
            if (&left_record == nullptr || right_record == nullptr){
                return nullptr;
            }
            
            // 判断是否返回该记录
            bool ret = true;
            for (Condition &cond : fed_conds_){
                ConditionEvaluator Cal;
                bool do_join = Cal.evaluate(cond, cols_check_, left_record, *right_record);
                if (!do_join){
                    ret = false;
                    break;
                }
            }

            if(ret){
                std::unique_ptr<RmRecord> result_record = std::make_unique<RmRecord>(len_);

                memcpy(result_record->data, left_record.data, left_->tupleLen());
                memcpy(result_record->data + left_->tupleLen(), right_record->data, right_->tupleLen());

                return result_record;
            }
        }
        return nullptr;
    }

    void set_buffer(bool first_time){
        // 1.清空缓冲区
        buffer.clear();

        // 2.向缓冲区插入元素
        if (first_time){
            left_->beginTuple();
        }else{
            // left_->nextTuple();
        }

        for (; !left_->is_end() && buffer.size() < block_size; left_->nextTuple()) {
            auto left_record = left_->Next();
            if (left_record) {
                buffer.emplace_back(std::move(*left_record));
            } else {
                break;
            }
        }

        // 3.置位左边的index
        left_tuple_index = 0;
        is_last_block = left_->is_end();
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override{
        return isend;
    }
    
    ColMeta get_col_offset(const TabCol& target) override {
        const std::vector<ColMeta>& rec_cols = cols();
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta& col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });

        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }

        return *pos;
    }

    ~NestedLoopJoinExecutor() override {
        buffer.clear();
    }
};