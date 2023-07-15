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

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同--by 星穹铁道高手
    std::vector<ColMeta> cols_check_;   // 条件语句中所有用到列的列的元数据信息--by 星穹铁道高手

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
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

        std::vector<Rid> rids_delete;

        for (auto &rid: rids_){
            // 2.通过记录ID使用文件处理器（fh_）获取该记录的内容（RmRecord对象）
            RmRecord rec = *fh_->get_record(rid, context_);

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
            // if(!do_update){continue;}


            if (do_update){
                rids_delete.push_back(rid);
            }
        }

        // 1.检查是否还有待删除的记录
        while (!rids_delete.empty())
        {
            // 2.获取最后一个待删除记录的位置
            Rid rid = rids_delete.back();
            RmRecord rec_to_del = *fh_->get_record(rid, context_);
            rids_delete.pop_back();

            for(int i = 0; i < tab_.indexes.size(); ++i) {
                auto& it_index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, it_index.cols)).get();
                char key[it_index.col_tot_len];
                int offset = 0;
                for(int i = 0; i < it_index.col_num; ++i) {
                    memcpy(key + offset, rec_to_del.data + it_index.cols[i].offset, it_index.cols[i].len);
                    offset += it_index.cols[i].len;
                }
                ih->delete_entry(key, nullptr);
            }
            
            // 3.删除记录
            fh_->delete_record(rid, context_);
        }
       
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};