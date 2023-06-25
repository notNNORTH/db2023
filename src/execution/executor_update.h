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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {

        // 1.检查是否还有待更新的记录。如果没有，返回nullptr表示更新操作完成
        if (rids_.empty()) {
            return nullptr;
        }

        // 2.从rids_中取出最后一个记录的ID
        Rid rid = rids_.back();
        rids_.pop_back();

        // 3.通过记录ID使用文件处理器（fh_）获取该记录的内容（RmRecord对象）
        RmRecord rec = *fh_->get_record(rid, context_);

        // 4.根据set_clauses_中的设定，更新记录的对应字段
        for (const auto& set_clause : set_clauses_) {
            
            // 判断插入的属性是否存在
            const auto& it = tab_.ColName_to_ColMeta.find(set_clause.lhs.col_name);
            if (it == tab_.ColName_to_ColMeta.end()){
                throw ColumnNotFoundError(set_clause.lhs.col_name);
            }

            // 判断插入值和属性类型是否一致
            auto& col = it->second;
            auto& val = const_cast<Value&>(set_clause.rhs);
            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }

            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        // 5.使用文件处理器（fh_）将更新后的记录写回到文件中
        fh_->update_record(rid, rec.data, context_);

        // 6.针对表中的每个索引，更新相应的索引条目     //////////////////////////////////////////////////////////
        for (const auto& index : tab_.indexes) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char* key = new char[index.col_tot_len];
            int offset = 0;

            for (size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }

            // ih->update_entry(key, rid, context_->txn_); /////////////////////////////haven't done//////////////////////////
        }

        //////////////////////////////////////// return std::make_unique<RmRecord>(std::move(rec));
        return nullptr;
    }
        
    Rid &rid() override { return _abstract_rid; }
};