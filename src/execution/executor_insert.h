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

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据, Value_代表一个数据，如（“王二萌”，10），存在一个vector中
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        for(auto value:values){
            if (value.type == TYPE_BIGINT ){
                if(value.bigint_val.flag == 1) throw BigIntoverflow();
            }
            if (value.type == TYPE_DATETIME and value.datetime_val.flag == false){  // 如果是DateTime且该类型不合法
                throw DateTimeError();
            }
        }
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type) {     // 检查插入数据与该位置属性是否一致
                if(col.type == TYPE_BIGINT && val.type == TYPE_INT){
                    BigInt bigint(val.int_val);
                    val.set_bigint(bigint);
                }else if(col.type == TYPE_INT && val.type == TYPE_BIGINT){
                    int value = val.bigint_val.value;
                    val.set_int(value);
                }else if(col.type == TYPE_STRING && val.type == TYPE_DATETIME){
                    std::string str_val = val.datetime_val.get_datetime();
                    val.set_str(str_val);
                }else{
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_);
        
        // Insert into index
        try {
            // Insert into index
            for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char* key = new char[index.col_tot_len];
                int offset = 0;
                for(size_t j = 0; j < index.col_num; ++j) {
                    memcpy(key + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                ih->insert_entry(key, rid_, nullptr);
                delete []key;
            }
        }catch(InternalError &error) {
            fh_->delete_record(rid_, context_);
            throw InternalError("item already exits");
        }
        return nullptr;
    }
    Rid &rid() override { return rid_; }
};
