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


    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同--by 星穹铁道高手
    std::vector<ColMeta> cols_check_;   // 条件语句中所有用到列的列的元数据信息--by 星穹铁道高手

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

        /****************by 星穹铁道高手***************/
        fed_conds_ = conds_;    //后续查询优化可以从fedcond入手!!!!!!!!!!!!!!!!!!!
        //初始化条件语句中所有用到列的列的元数据信息
        int con_size = fed_conds_.size();
        for (int loop = 0; loop < con_size; loop++) {
            auto temp_con = fed_conds_[loop];

            // 检查左操作数是否为列操作数
            if (!temp_con.lhs_col.tab_name.empty() && !temp_con.lhs_col.col_name.empty()) {
                // 查找colmeta
                int size = tab_.cols.size();
                for(int i = 0; i < size; i++){//后续join也可能改tab，加入多表内容
                    if((temp_con.lhs_col.tab_name == tab_.cols[i].tab_name) 
                                && (temp_con.lhs_col.col_name == tab_.cols[i].name)){
                        auto temp = tab_.cols[i];
                        cols_check_.push_back(temp);
                    }
                }
            }
            // 检查右操作数是否为列操作数
            if (!temp_con.is_rhs_val && !temp_con.rhs_col.tab_name.empty() && !temp_con.rhs_col.col_name.empty()) {
                // 查找colmeta
                int size = tab_.cols.size();
                for(int i = 0; i < size; i++){//后续join也可能改tab，加入多表内容
                    if((temp_con.rhs_col.tab_name == tab_.cols[i].tab_name) 
                                && (temp_con.rhs_col.col_name == tab_.cols[i].name)){
                        auto temp = tab_.cols[i];
                        cols_check_.push_back(temp);
                    }
                }
            }

        }
    }

    std::unique_ptr<RmRecord> Next() override {
/*

        // 1.检查是否还有待更新的记录。如果没有，返回nullptr表示更新操作完成
        for (auto &rid : rids_){

            // 2.通过记录ID使用文件处理器（fh_）获取该记录的内容（RmRecord对象）
            RmRecord rec = *fh_->get_record(rid, context_);
            RmRecord old_rec=rec;

            // 3.判断 WHERE 后面的condition
            bool do_update = true;
            for (Condition &cond : conds_){
                ConditionEvaluator Cal;
                bool tmp = Cal.evaluate(cond, cols_check_, rec);
                if (!tmp){
                    do_update = false;
                    break;
                }
            }
            if(!do_update){continue;}

            for(auto& index:tab_.indexes) {
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char key[index.col_tot_len];
                int offset = 0;
                for(int i = 0; i < index.col_num; i++) {
                    memcpy(key + offset, old_rec.data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                ih->delete_entry(key, nullptr);
            }

            // 4.根据set_clauses_中的设定，更新记录的对应字段
            for (const auto& set_clause : set_clauses_) {
                
                // 判断插入的属性是否存在
                const auto& it = tab_.cols;
                bool found = false;
                int i=0;
                for (const auto& element : it) {
                    if (element.tab_name == set_clause.lhs.tab_name && element.name == set_clause.lhs.col_name) {
                        found = true;
                        break;
                    }
                    i++;   
                }
                if (!found){
                    throw ColumnNotFoundError(set_clause.lhs.col_name);
                }

                // 判断插入值和属性类型是否一致
                auto& col = it[i];
                auto& val = const_cast<Value&>(set_clause.rhs);
                if (col.type == TYPE_FLOAT && val.type == TYPE_INT || col.type == TYPE_INT && val.type == TYPE_FLOAT){
                    val.type = col.type;
                }
                else if (col.type == TYPE_BIGINT && val.type == TYPE_INT ){
                    BigInt bigint(val.int_val);
                    val.set_bigint(bigint);
                }
                else if (col.type == TYPE_INT && val.type == TYPE_BIGINT){
                    int value = val.bigint_val.value;
                    val.set_int(value);
                }
                else if (col.type != val.type) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }

                val.raw = nullptr;
                val.init_raw(col.len);
                memcpy(rec.data + col.offset, val.raw->data, col.len);
            }

            // 5.使用文件处理器（fh_）将更新后的记录写回到文件中
            fh_->update_record(rid, rec.data, context_);

            try {
                // 将rid插入在内存中更新后的新的record的cols对应key的index
                for(size_t i = 0; i < tab_.indexes.size(); ++i) {
                    auto& index = tab_.indexes[i];
                    auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                    char key[index.col_tot_len];
                    int offset = 0;
                    for(size_t i = 0; i < index.col_num; ++i) {
                        memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                        offset += index.cols[i].len;
                    }
                    ih->insert_entry(key,rid,nullptr);
                }
            }catch(InternalError &error) {
                // 1. 恢复record
                fh_->update_record(rid, old_rec.data, context_);
                // 恢复索引
                // 2. 恢复所有的index
                for(auto& index:tab_.indexes) {
                    auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                    char key[index.col_tot_len];
                    int offset = 0;
                    for(int i = 0; i < index.col_num; i++) {
                        memcpy(key + offset, old_rec.data + index.cols[i].offset, index.cols[i].len);
                        offset += index.cols[i].len;
                    }
                    ih->insert_entry(key, rid, nullptr);
                }

                // 3. 继续抛出异常
                throw InternalError("item already exits!");
            }


        }
        return nullptr;
*/
    }
        
    Rid &rid() override { return _abstract_rid; }
};