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
    // std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;
    int index;

    std::vector<RmRecord> all_records;  // 存储所有找到的元组
    std::vector<std::vector<Value>> order_by_cols;  // 存储元组order_by所用到的属性值
    int limit_;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_cols, std::vector<bool> is_desc, int limit) {
        prev_ = std::move(prev);

        for (auto &sel_col : sel_cols){
            cols_.push_back(prev_->get_col_offset(sel_col));
        }
        
        is_desc_ = is_desc;
        tuple_num = 0;
        // used_tuple.clear();
        current_tuple = nullptr;    // by 星穹铁道高手

        /***************by 星穹铁道高手**************/
        int order_col_size = cols_.size();  // 参与对比的属性数量
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = prev_->Next();
            if (!record){ break; }
            all_records.push_back(*record);
            // used_tuple.push_back(0);    // 0为没用过，1为用过

            // 初始化order_by所用到的属性值
            std::vector<Value> order_by_col;
            for (int i = 0; i < order_col_size; i++){
                Value value = get_col_value(*record, cols_[i]);
                order_by_col.push_back(value);
            }
            order_by_cols.push_back(order_by_col);
        }

        tuple_num = all_records.size();
        limit_ = limit;


        /**************快速排序**************/
        quicksort(all_records, order_by_cols, 0, tuple_num - 1);
    }

    // 取第一个满足条件的record置为current_tuple
    void beginTuple() override {
        index = 0;
    }

    // 取下一个满足条件的record置为current_tuple
    void nextTuple() override {
        index++;
    }

    // 返回current_tuple
    std::unique_ptr<RmRecord> Next() override {
        limit_--;
        std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(all_records[index]);
        return record;
    }

    // 判断是否搜索结束
    bool is_end() const override{
        return (index >= tuple_num) || (limit_ == 0);
    }

    void quicksort(std::vector<RmRecord>& records, std::vector<std::vector<Value>>& order_cols,
                int low, int high) {
        if (low < high) {
            int pivot = partition(records, order_cols, low, high);
            quicksort(records, order_cols, low, pivot - 1);
            quicksort(records, order_cols, pivot + 1, high);
        }
    }

    int partition(std::vector<RmRecord>& records, std::vector<std::vector<Value>>& order_cols,
                int low, int high) {
        int i = low - 1;
        const std::vector<Value>& pivot_cols = order_cols[high]; // 使用最后一个元组作为枢轴
        for (int j = low; j < high; j++) {
            if (is_tuple_less(records[j], records[high], order_cols[j], pivot_cols)) {
                i++;
                std::swap(records[i], records[j]);
                std::swap(order_cols[i], order_cols[j]);
            }
        }
        std::swap(records[i + 1], records[high]);
        std::swap(order_cols[i + 1], order_cols[high]);
        return i + 1;
    }

    bool is_tuple_less(const RmRecord& record1, const RmRecord& record2,
                    const std::vector<Value>& order_cols1, const std::vector<Value>& order_cols2) {
        for (size_t i = 0; i < order_cols1.size(); i++) {
            const Value& value1 = order_cols1[i];
            const Value& value2 = order_cols2[i];
            if (is_desc_[i]) {
                if (ConditionEvaluator::isGreaterThan(value1, value2))
                    return true;
                if (ConditionEvaluator::isLessThan(value1, value2))
                    return false;
            } else {
                if (ConditionEvaluator::isLessThan(value1, value2))
                    return true;
                if (ConditionEvaluator::isGreaterThan(value1, value2))
                    return false;
            }
        }
        return false;
    }


    // 给定record和元组的属性，返回属性值
    Value get_col_value(RmRecord& record, ColMeta& col) {
        int offset = col.offset;
        Value value;

        auto type = col.type;
        if(type == TYPE_INT){
            char* charPointer1 = reinterpret_cast<char*>(record.data + col.offset);  
            int int_val = *reinterpret_cast<int*>(charPointer1);
            value.set_int(int_val);
            value.init_raw(col.len);
            return value;
        }else if(type == TYPE_FLOAT){
            char* charPointer2 = reinterpret_cast<char*>(record.data + col.offset);
            double float_val = *reinterpret_cast<double*>(charPointer2);
            value.set_float(float_val);
            value.init_raw(col.len);
            return value;
        }else if(type == TYPE_STRING){
            char* charPointer3 = reinterpret_cast<char*>(record.data + col.offset); 
            std::string str(charPointer3, charPointer3+col.len); 
            value.set_str(str);
            value.init_raw(col.len);
            return value;
        }else if(type == TYPE_BIGINT){
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