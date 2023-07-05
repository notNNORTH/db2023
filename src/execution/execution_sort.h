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
    std::vector<ColMeta> cols_;          // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;       // 处理的元组数量
    std::vector<bool> is_desc_;          // true降序, false升序
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;

    std::vector<RmRecord> all_records;  // 存储所有找到的元组
    int limit_;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_cols, std::vector<bool> is_desc, int limit) {
        prev_ = std::move(prev);

        for (auto &sel_col : sel_cols){
            cols_.push_back(prev_->get_col_offset(sel_col));
        }
        
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
        limit_ = limit;
    }

    // 取第一个满足条件的record置为current_tuple
    void beginTuple() override {
        set_current_tuple();
    }

    // 取下一个满足条件的record置为current_tuple
    void nextTuple() override {
        set_current_tuple();
    }

    // 返回current_tuple
    std::unique_ptr<RmRecord> Next() override {
        limit_--;
        std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(*current_tuple);
        return record;
    }

    // 判断是否搜索结束
    bool is_end() const override{
        return (current_tuple == nullptr) || (limit_ == 0);
    }

    // 获取满足条件的tuple
    void set_current_tuple(){
        // 1.找到第一个未被使用的元组(index)
        int i = 0;
        int index = -1;
        for(; i < tuple_num; i++){
            if(!used_tuple[i]){
                index = i;
                break;
            }
        }

        // 2.依次与所有未被使用的元组对比
        //      2.1 记录对比属性
        int order_col_size = cols_.size();  // 参与对比的属性数量
        int order_col_index = 0;            // 当前对比属性位置

        for(; i < tuple_num; i++){
            if(used_tuple[i]){ continue; }

            //  2.2 逐属性对比
            bool better = true;
            for (int j = 0; j < order_col_size; j++){
                Value value_index = get_col_value(all_records[index], cols_[j]);
                Value value_i = get_col_value(all_records[i], cols_[j]);

                if (is_desc_[j]){
                    if (ConditionEvaluator::isGreaterThan(value_i, value_index)){ break; }
                    else if (ConditionEvaluator::isLessThan(value_i, value_index)){
                        better = false;
                        break;
                    }
                    continue;
                }else{
                    if (ConditionEvaluator::isLessThan(value_i, value_index)){ break; }
                    else if (ConditionEvaluator::isGreaterThan(value_i, value_index)){
                        better = false;
                        break;
                    }
                    continue;
                }
            }
            //  2.3 元组i的值更好：替换index
            if (better){
                index = i;
            }
        }

        // 3.置current_tuple的值
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