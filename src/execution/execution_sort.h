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

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;    // scan执行器
    ColMeta cols_;          // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;       // 处理的元组数量
    bool is_desc_;          // true降序, false升序
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;

    std::vector<RmRecord> all_records;  // 存储所有找到的元组

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->get_col_offset(sel_cols);
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
        current_tuple = nullptr;    // by 星穹铁道高手

        /***************by 星穹铁道高手**************/
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = prev_->Next();
            all_records.push_back(*record);
            used_tuple.push_back(0);    // 0为没用过，1为用过
        }
        tuple_num = all_records.size();
    }

    // 取第一个满足条件的record置为current_tuple
    void beginTuple() override {
        if (is_desc_){      // 降序寻找
            search_DESC();
        }else{
            search_ASC();
        }
    }

    // 取下一个满足条件的record置为current_tuple
    void nextTuple() override {
        if (is_desc_){      // 降序寻找
            search_DESC();
        }else{
            search_ASC();
        }
    }

    // 返回current_tuple
    std::unique_ptr<RmRecord> Next() override {
        std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(*current_tuple);
        return record;
    }

    // 判断是否搜索结束
    bool is_end() const override{
        return current_tuple == nullptr;
    }

    // 降序搜索下一个元组
    void search_DESC(){
        // 找到第一个没用过的元组
        int i = 0;
        int index = -1;
        for(; i < tuple_num; i++){
            if(!used_tuple[i]){
                index = i;
                break;
            }
        }

        for(; i < tuple_num; i++){
            if(used_tuple[i]){ continue; }
            
            Value value_index = get_col_value(all_records[index], cols_);
            Value value_i = get_col_value(all_records[i], cols_);

            if (ConditionEvaluator::isGreaterThanOrEqual(value_i, value_index)){
                index = i;
            }
        }
        if (index == -1){
            current_tuple = nullptr;
        }else{
            used_tuple[index] = 1;
            current_tuple = std::make_unique<RmRecord>(all_records[index]);
        }
    }
    

    // 升序搜索下一个元组
    void search_ASC(){
        // 找到第一个没用过的元组
        int i = 0;
        int index = -1;
        for(; i < tuple_num; i++){
            if(!used_tuple[i]){
                index = i;
                break;
            }
        }

        for(; i < tuple_num; i++){
            if(used_tuple[i]){ continue; }
            
            Value value_index = get_col_value(all_records[index], cols_);
            Value value_i = get_col_value(all_records[i], cols_);

            if (ConditionEvaluator::isLessThanOrEqual(value_i, value_index)){
                index = i;
            }
        }
        if (index == -1){
            current_tuple = nullptr;
        }else{
            used_tuple[index] = 1;
            current_tuple = std::make_unique<RmRecord>(all_records[index]);
        }
    }

    // 给定record和元组的属性，返回属性值
    Value get_col_value(RmRecord& record, ColMeta& col) {
        int offset = col.offset;
        Value value;

        auto type = col.type;
        if(type==TYPE_INT){
            char* charPointer1 = reinterpret_cast<char*>(record.data + col.offset);  
            int int_val = *reinterpret_cast<int*>(charPointer1);
            value.set_int(int_val);
            value.init_raw(col.len);
            return value;
        }else if(type==TYPE_FLOAT){
            char* charPointer2 = reinterpret_cast<char*>(record.data + col.offset);
            double float_val = *reinterpret_cast<double*>(charPointer2);
            value.set_float(float_val);
            value.init_raw(col.len);
            return value;
        }else if(type==TYPE_STRING){
            char* charPointer3 = reinterpret_cast<char*>(record.data + col.offset); 
            std::string str(charPointer3, charPointer3+col.len); 
            value.set_str(str);
            value.init_raw(col.len);
            return value;
        }else if(type==TYPE_BIGINT){
            char* charPointer4 = reinterpret_cast<char*>(record.data + col.offset);
            BigInt bigint_val = *reinterpret_cast<BigInt*>(charPointer4);
            value.set_bigint(bigint_val);
            value.init_raw(col.len);
            return value;
        }else if(type == TYPE_DATETIME){
            char* charPointer5 = reinterpret_cast<char*>(record.data + col.offset);
            DateTime datetime_val = *reinterpret_cast<DateTime*>(charPointer5);
            value.set_datetime(datetime_val);
            value.init_raw(col.len);
            return value;
        }

        return value;
    }

    const std::vector<ColMeta> &cols() const override {
    // 提供适当的实现，返回具体的 ColMeta 对象或者 std::vector<ColMeta>
        return prev_->cols();
    }

    Rid &rid() override { return _abstract_rid; }
};