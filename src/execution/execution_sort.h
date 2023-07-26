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

#include "LoserTree.h"

class SortExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;    // scan执行器
    std::vector<ColMeta> cols_;          // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;       // 处理的元组数量
    std::vector<bool> is_desc_;         // true降序, false升序
    int limit_;

    std::string temp_file_name_;        // 临时文件名
    std::vector<std::string> temp_files_;   // 存储所有临时文件的名称

    int buffer_max_size_;   // 缓冲区最大大小
    int buffer_size_;       // 临时变量: 缓冲区中元组的数量
    std::vector<RmRecord> buffers_;        // 缓冲区

    std::vector<int> output_file_counts_; // 文件中剩余元组的数量
    std::vector<LoserTree> loser_trees_;    // 失败树
    int record_size;

public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<TabCol> sel_cols, std::vector<bool> is_desc, int limit) {
        prev_ = std::move(prev);

        for (auto &sel_col : sel_cols) {
            cols_.push_back(prev_->get_col_offset(sel_col));
        }
        is_desc_ = is_desc;
        tuple_num = 0;
        limit_ = limit;

        // 初始化缓冲区和输出缓冲区
        buffer_max_size_ = 2000;  // 缓冲区大小为5000
        buffers_.clear();
        buffer_size_ = 0;

        // 创建临时文件名
        temp_file_name_ = "temp_sort_file";
        std::remove(temp_file_name_.c_str());

        // 执行排序和分割操作
        executeSort();
        
        LoserTree loser_tree(temp_files_, cols_, is_desc, record_size, output_file_counts_);
        loser_trees_.push_back(loser_tree);
    }

    // 执行排序操作
    void executeSort() {
        // 初始化record_size
        bool set_record_size = true;

        // 读取元组并存储到缓冲区
        int buffer_index = 0;   // 文件标识符

        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto record = prev_->Next();
            if (!record) {
                break;
            }
            if (set_record_size){
                record_size = record->size;
                set_record_size = false;
            }
            buffers_.push_back(std::move(*record));
            buffer_size_++;

            if (buffer_size_ >= buffer_max_size_) {
                // 当前缓冲区已满，进行排序
                /*
                std::vector<std::vector<Value>> order_by_col;
                order_by_col.resize(buffer_max_size_);
                for (int i = 0; i < buffer_max_size_; i++) {
                    std::vector<Value> order_by_cols_row;
                    for (int j = 0; j < cols_.size(); j++) {
                        Value value = get_col_value(buffers_[i], cols_[j]);
                        order_by_cols_row.push_back(std::move(value));
                    }
                    order_by_col[i] = std::move(order_by_cols_row);
                }*/
                //quicksort(buffers_, order_by_col, 0, buffer_max_size_ - 1);
                //bubbleSort(buffers_, order_by_col);
                mySort(buffers_);

                // 将排序后的元组写入临时文件
                std::string temp_file = temp_file_name_ + std::to_string(buffer_index);
                writeToTxt(temp_file, buffers_);
                output_file_counts_.push_back(buffer_size_);

                // 清空缓冲区
                buffers_.clear();
                buffer_size_ = 0;

                // 记录临时文件名
                temp_files_.push_back(temp_file);

                // 切换到下一个缓冲区
                buffer_index++;
            }
        }

        // 处理最后一个缓冲区中的元组
        if (buffer_size_ > 0) {
            // 进行排序
            /*
            std::vector<std::vector<Value>> order_by_col;
            order_by_col.resize(buffer_size_);
            for (int i = 0; i < buffer_size_; i++) {
                std::vector<Value> order_by_cols_row;
                for (int j = 0; j < cols_.size(); j++) {
                    Value value = get_col_value(buffers_[i], cols_[j]);
                    order_by_cols_row.push_back(std::move(value));
                }
                order_by_col[i] = std::move(order_by_cols_row);
            }*/
            //quicksort(buffers_, order_by_col, 0, buffer_size_ - 1);
            //bubbleSort(buffers_, order_by_col);
            mySort(buffers_);

            // 将排序后的元组写入临时文件
            std::string temp_file = temp_file_name_ + std::to_string(buffer_index);
            writeToTxt(temp_file, buffers_);
            output_file_counts_.push_back(buffer_size_);

            // 记录临时文件名
            temp_files_.push_back(temp_file);

            // 清空缓冲区
            buffers_.clear();
        }

        // 更新处理的元组数量
        tuple_num = buffer_index * buffer_max_size_ + buffer_size_;
    }

    // 取第一个满足条件的record置为current_tuple
    void beginTuple() override {
        loser_trees_[0].set_tree();
    }

    // 取下一个满足条件的record置为current_tuple
    void nextTuple() override {
        loser_trees_[0].set_leaf(loser_trees_[0].treeNode[0]);  //输出一个，要更新对应的叶子结点的值
		loser_trees_[0].adjust(loser_trees_[0].treeNode[0]);  //调整
    }

    // 返回current_tuple
    std::unique_ptr<RmRecord> Next() override {
        limit_--;
        tuple_num--;
        std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(loser_trees_[0].get_tuple());
        return record;
        // return nullptr;
    }

    // 判断是否搜索结束
    bool is_end() const override {
        return (tuple_num == 0) || (limit_ == 0);
    }

    void mySort(std::vector<RmRecord>& records){
        std::sort(records.begin(), records.end(), [this](RmRecord& record1, RmRecord& record2) {
            return compare(record1, record2);
        });
    }

    bool compare(RmRecord& record1, RmRecord& record2){
        std::vector<Value> left_cols_row;
        std::vector<Value> right_cols_row;
        for (int j = 0; j < cols_.size(); j++) {
            Value value1 = get_col_value(record1, cols_[j]);
            left_cols_row.push_back(std::move(value1));

            Value value2 = get_col_value(record2, cols_[j]);
            right_cols_row.push_back(std::move(value2));
        }
        
        return is_tuple_less(record1, record2, left_cols_row, right_cols_row);
    }

    // 冒泡排序算法
    void bubbleSort(std::vector<RmRecord>& records, std::vector<std::vector<Value>>& order_cols) {
        int n = records.size();
        for (int i = 0; i < n - 1; i++) {
            for (int j = 0; j < n - i - 1; j++) {
                if (is_tuple_less(records[j], records[j + 1], order_cols[j], order_cols[j + 1])) {
                    // std::swap(records[j], records[j + 1]);
                    // std::swap(order_cols[j], order_cols[j + 1]);
                }
            }
        }
    }

    // 快速排序算法
    void quicksort(std::vector<RmRecord>& records, std::vector<std::vector<Value>>& order_cols, int low, int high) {
        if (low < high) {
            int pivot = partition(records, order_cols, low, high);
            quicksort(records, order_cols, low, pivot - 1);
            quicksort(records, order_cols, pivot + 1, high);
        }
    }

    int partition(std::vector<RmRecord>& records, std::vector<std::vector<Value>>& order_cols, int low, int high) {
        int i = low - 1;
        const std::vector<Value>& pivot_cols = order_cols[high]; // 使用最后一个元组作为枢轴
        for (int j = low; j < high; j++) {
            if (is_tuple_less(records[j], records[high], order_cols[j], pivot_cols)) {
                i++;
                // 手动交换 records[i] 和 records[j] 的值
/*                RmRecord* temp_record = new RmRecord(std::move(records[i]));
                records[i] = std::move(records[j]);
                records[j] = std::move(*temp_record);
                delete temp_record;

                // 手动交换 order_cols[i] 和 order_cols[j] 的值
                std::vector<Value>* temp_order_cols = new std::vector<Value>(std::move(order_cols[i]));
                order_cols[i] = std::move(order_cols[j]);
                order_cols[j] = std::move(*temp_order_cols);
                delete temp_order_cols;*/
            }
        }
        // 手动交换 records[i + 1] 和 records[high] 的值
/*        RmRecord* temp_record = new RmRecord(std::move(records[i + 1]));
        records[i + 1] = std::move(records[high]);
        records[high] = std::move(*temp_record);
        delete temp_record;

        // 手动交换 order_cols[i + 1] 和 order_cols[high] 的值
        std::vector<Value>* temp_order_cols = new std::vector<Value>(std::move(order_cols[i + 1]));
        order_cols[i + 1] = std::move(order_cols[high]);
        order_cols[high] = std::move(*temp_order_cols);
        delete temp_order_cols;*/
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
        if (type == TYPE_INT) {
            char* charPointer1 = reinterpret_cast<char*>(record.data + col.offset);
            int int_val = *reinterpret_cast<int*>(charPointer1);
            value.set_int(int_val);
            value.init_raw(col.len);
            return value;
        } else if (type == TYPE_FLOAT) {
            char* charPointer2 = reinterpret_cast<char*>(record.data + col.offset);
            double float_val = *reinterpret_cast<double*>(charPointer2);
            value.set_float(float_val);
            value.init_raw(col.len);
            return value;
        } else if (type == TYPE_STRING) {
            char* charPointer3 = reinterpret_cast<char*>(record.data + col.offset);
            std::string str(charPointer3, charPointer3 + col.len);
            value.set_str(str);
            value.init_raw(col.len);
            return value;
        } else if (type == TYPE_BIGINT) {
            char* charPointer4 = reinterpret_cast<char*>(record.data + col.offset);
            BigInt bigint_val = *reinterpret_cast<BigInt*>(charPointer4);
            value.set_bigint(bigint_val);
            value.init_raw(col.len);
            return value;
        } else if (type == TYPE_DATETIME) {
            char* charPointer5 = reinterpret_cast<char*>(record.data + col.offset);
            DateTime datetime_val = *reinterpret_cast<DateTime*>(charPointer5);
            value.set_datetime(datetime_val);
            value.init_raw(col.len);
            return value;
        }
        return value;
    }

    const std::vector<ColMeta> &cols() const override {
        return prev_->cols();
    }

    Rid &rid() override { return _abstract_rid; }

    ~SortExecutor() override {
        // 清空临时文件
        for (int i = 0; i < temp_files_.size(); i++) {
            std::remove(temp_files_[i].c_str());
        }
    }

    // 写入元组到文件
    void writeToTxt(const std::string& file_name, std::vector<RmRecord> records) {
        std::ofstream output_file(file_name, std::ios::binary | std::ios::app);
        for (auto record : records) {
            output_file.write(reinterpret_cast<char*>(record.data), record.size);
        }
        output_file.close();
    }
};