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

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<IxScan> scan_;

    SmManager *sm_manager_;
    std::vector<ColMeta> cols_check_;         // 条件语句中所有用到列的列的元数据信息
    IxIndexHandle *ix_handle;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
        auto ix_name=sm_manager_->get_ix_manager()->get_index_name(tab_name_,index_col_names_);
        ix_handle=(sm_manager_->ihs_[ix_name]).get();

        //初始化条件语句中所有用到列的列的元数据信息
        int con_size=fed_conds_.size();
        for (int loop=0;loop<con_size;loop++) {
            auto temp_con=fed_conds_[loop];

            // 检查左操作数是否为列操作数
            if (!temp_con.lhs_col.tab_name.empty() && !temp_con.lhs_col.col_name.empty()) {
                // 查找colmeta
                auto tabname_in_con_left=temp_con.lhs_col.tab_name;
                auto colname_in_con_left=temp_con.lhs_col.col_name;
                int size=tab_.cols.size();
                for(int i=0;i<size;i++){//后续join也可能改tab，加入多表内容
                    if((tabname_in_con_left==tab_.cols[i].tab_name)&&(colname_in_con_left==tab_.cols[i].name)){
                        auto temp=tab_.cols[i];
                        cols_check_.push_back(temp);
                    }
                }
            }
            // 检查右操作数是否为列操作数
            if (!temp_con.is_rhs_val && !temp_con.rhs_col.tab_name.empty() && !temp_con.rhs_col.col_name.empty()) {
                // 查找colmeta
                auto tabname_in_con_right=temp_con.rhs_col.tab_name;
                auto colname_in_con_right=temp_con.rhs_col.col_name;
                int size=tab_.cols.size();
                for(int i=0;i<size;i++){//后续join也可能改tab，加入多表内容
                    if((tabname_in_con_right==tab_.cols[i].tab_name)&&(colname_in_con_right==tab_.cols[i].name)){
                        auto temp=tab_.cols[i];
                        cols_check_.push_back(temp);
                    }
                }
            }
        }

    }

    void beginTuple() override {

        std::vector<Condition> lower_bound_conds;
        std::vector<Condition> upper_bound_conds;

        int key_size=index_meta_.col_tot_len;
        char* key_lower = new char[key_size];
        char* key_upper = new char[key_size];

        int max_int=std::numeric_limits<int>::max();
        int min_int=std::numeric_limits<int>::min();
        double max_flt=std::numeric_limits<double>::max();
        double min_flt=std::numeric_limits<double>::min();

        //初始化最大与最小缓冲区
        int ofst=0;
        for(auto idx_col:index_meta_.cols){
            switch (idx_col.type)
            {
            case TYPE_INT:
                memcpy(key_lower + ofst, &min_int, idx_col.len);
                memcpy(key_upper + ofst, &max_int, idx_col.len);
                ofst+=idx_col.len;
                break;
            case TYPE_FLOAT:
                memcpy(key_lower + ofst, &min_flt, idx_col.len);
                memcpy(key_upper + ofst, &max_flt, idx_col.len);
                ofst+=idx_col.len;
                break;
            case TYPE_STRING:
                char* min_str = new char[idx_col.len];
                memset(min_str,0,idx_col.len);
                char* max_str = new char[idx_col.len];
                memset(max_str,255,idx_col.len);
                memcpy(key_lower + ofst, min_str, idx_col.len);
                memcpy(key_upper + ofst, max_str, idx_col.len);
                ofst+=idx_col.len;
                delete min_str;
                delete max_str;
                break;
            }
        }


        int key_offset=0;

        //分离出索引内列cond
        bool continue_to_find=true;
        int match_col_in_cond=0;
        int match_col_in_idx=0;
        std::vector<Condition> in_idx_conds_;
        
        while(continue_to_find){
            int match_count=0;
            while(fed_conds_[match_col_in_cond].lhs_col.col_name==index_col_names_[match_col_in_idx]){
                in_idx_conds_.push_back(fed_conds_[match_col_in_cond]);
                match_col_in_cond++;
                match_count++;
            }
            if(match_count>0){
                match_col_in_idx++;
                continue;
            }else{
                continue_to_find=false;
            }
        }

        for(int i=0;i<in_idx_conds_.size();i++){
            auto this_cond=in_idx_conds_[i];
            Condition next_cond;
            if(i!=in_idx_conds_.size()-1){
                next_cond=in_idx_conds_[i+1];
            }else{
                next_cond=this_cond;
            }
            const void* col_value_lower;
            const void* col_value_upper;

            if(this_cond.op==OP_EQ){
                if(this_cond.rhs_val.type==TYPE_INT){
                    col_value_lower = &(this_cond.rhs_val.int_val);
                    col_value_upper = &(this_cond.rhs_val.int_val);
                }else if(this_cond.rhs_val.type==TYPE_FLOAT){
                    col_value_lower = &(this_cond.rhs_val.float_val);
                    col_value_upper = &(this_cond.rhs_val.float_val);
                }else if(this_cond.rhs_val.type==TYPE_STRING){
                    col_value_lower = &(this_cond.rhs_val.str_val[0]);
                    col_value_upper = &(this_cond.rhs_val.str_val[0]);
                }

                auto it=std::find(index_col_names_.begin(),index_col_names_.end(),this_cond.lhs_col.col_name);
                int j=std::distance(index_col_names_.begin(),it);
                memcpy(key_lower + key_offset, col_value_lower, index_meta_.cols[j].len);
                memcpy(key_upper + key_offset, col_value_upper, index_meta_.cols[j].len);
                key_offset+=index_meta_.cols[j].len;
                continue;

            }else if(this_cond.op==OP_GE||this_cond.op==OP_GT){
                if((next_cond.lhs_col.col_name==this_cond.lhs_col.col_name)&&(next_cond.op==OP_LE||next_cond.op==OP_LT)){
                    auto it=std::find(index_col_names_.begin(),index_col_names_.end(),this_cond.lhs_col.col_name);
                    int j=std::distance(index_col_names_.begin(),it);

                    if(this_cond.rhs_val.type==TYPE_INT){
                        col_value_lower = &(this_cond.rhs_val.int_val);
                        col_value_upper = &(next_cond.rhs_val.int_val);
                    }else if(this_cond.rhs_val.type==TYPE_FLOAT){
                        col_value_lower = &(this_cond.rhs_val.float_val);
                        col_value_upper = &(next_cond.rhs_val.float_val);
                    }else if(this_cond.rhs_val.type==TYPE_STRING){
                        col_value_lower = &(this_cond.rhs_val.str_val[0]);
                        col_value_upper = &(next_cond.rhs_val.str_val[0]);
                    }

                    memcpy(key_lower + key_offset, col_value_lower, index_meta_.cols[j].len);
                    memcpy(key_upper + key_offset, col_value_upper, index_meta_.cols[j].len);
                    key_offset+=index_meta_.cols[j].len;
                    i++;
                    continue;
                }else{
                    auto it=std::find(index_col_names_.begin(),index_col_names_.end(),this_cond.lhs_col.col_name);
                    int j=std::distance(index_col_names_.begin(),it);
                    char* temp_str = new char[index_meta_.cols[j].len];

                    if(this_cond.rhs_val.type==TYPE_INT){
                        col_value_lower = &(this_cond.rhs_val.int_val);
                        col_value_upper = &(max_int);
                    }else if(this_cond.rhs_val.type==TYPE_FLOAT){
                        col_value_lower = &(this_cond.rhs_val.float_val);
                        col_value_upper = &(max_flt);
                    }else if(this_cond.rhs_val.type==TYPE_STRING){
                        col_value_lower = &(this_cond.rhs_val.str_val[0]);
                        memset(temp_str,255,index_meta_.cols[j].len);
                        col_value_upper = temp_str;
                    }

                    memcpy(key_lower + key_offset, col_value_lower, index_meta_.cols[j].len);
                    memcpy(key_upper + key_offset, col_value_upper, index_meta_.cols[j].len);
                    key_offset+=index_meta_.cols[j].len;
                    delete temp_str;
                    continue;
                }
            }else if(this_cond.op==OP_LE||this_cond.op==OP_LT){
                if((next_cond.lhs_col.col_name==this_cond.lhs_col.col_name)&&(next_cond.op==OP_GE||next_cond.op==OP_GT)){
                    auto it=std::find(index_col_names_.begin(),index_col_names_.end(),this_cond.lhs_col.col_name);
                    int j=std::distance(index_col_names_.begin(),it);

                    if(this_cond.rhs_val.type==TYPE_INT){
                        col_value_lower = &(next_cond.rhs_val.int_val);
                        col_value_upper = &(this_cond.rhs_val.int_val);
                    }else if(this_cond.rhs_val.type==TYPE_FLOAT){
                        col_value_lower = &(next_cond.rhs_val.float_val);
                        col_value_upper = &(this_cond.rhs_val.float_val);
                    }else if(this_cond.rhs_val.type==TYPE_STRING){
                        col_value_lower = &(next_cond.rhs_val.str_val[0]);
                        col_value_upper = &(this_cond.rhs_val.str_val[0]);
                    }

                    memcpy(key_lower + key_offset, col_value_lower, index_meta_.cols[j].len);
                    memcpy(key_upper + key_offset, col_value_upper, index_meta_.cols[j].len);
                    key_offset+=index_meta_.cols[j].len;
                    i++;
                    continue;
                }else{
                    auto it=std::find(index_col_names_.begin(),index_col_names_.end(),this_cond.lhs_col.col_name);
                    int j=std::distance(index_col_names_.begin(),it);
                    char* temp_str = new char[index_meta_.cols[j].len];

                    if(this_cond.rhs_val.type==TYPE_INT){
                        col_value_lower = &(min_int);
                        col_value_upper = &(this_cond.rhs_val.int_val);
                    }else if(this_cond.rhs_val.type==TYPE_FLOAT){
                        col_value_lower = &(min_flt);
                        col_value_upper = &(this_cond.rhs_val.float_val);
                    }else if(this_cond.rhs_val.type==TYPE_STRING){
                        memset(temp_str,0,index_meta_.cols[j].len);
                        col_value_lower = temp_str;
                        col_value_upper = &(this_cond.rhs_val.str_val[0]);
                    }

                    memcpy(key_lower + key_offset, col_value_lower, index_meta_.cols[j].len);
                    memcpy(key_upper + key_offset, col_value_upper, index_meta_.cols[j].len);
                    key_offset+=index_meta_.cols[j].len;
                    delete temp_str;
                    continue;
                }
            }
        }
        if(ix_handle->get_filehdr()->num_pages_>2){
            auto begin=(ix_handle->find_leaf_page(key_lower,Operation::FIND,nullptr)).first;
            auto end=(ix_handle->find_leaf_page(key_upper,Operation::FIND,nullptr)).first;

            if(ix_compare(key_lower,key_upper,begin->get_file_hdr()->col_types_,begin->get_file_hdr()->col_lens_)>0){
                sm_manager_->get_bpm()->unpin_page(end->get_page_id(),false);

                Iid lower=Iid();
                lower.page_no=static_cast<int>(begin->get_page_no());
                lower.slot_no=begin->lower_bound(key_lower);

                if (lower.slot_no == end->get_size()) {
                    // keep valid slot
                    lower.slot_no = end->get_size()-1;
                }

                sm_manager_->get_bpm()->unpin_page(begin->get_page_id(),false);
                scan_ = std::make_unique<IxScan>(ix_handle,lower,lower,sm_manager_->get_bpm());
                rid_=scan_->rid();

            }else{
                Iid lower=Iid();
                lower.page_no=static_cast<int>(begin->get_page_no());
                lower.slot_no=begin->lower_bound(key_lower);

                Iid upper=Iid();
                upper.page_no=static_cast<int>(end->get_page_no());
                upper.slot_no=end->upper_bound(key_upper);

                if (upper.page_no!= ix_handle->get_filehdr()->last_leaf_ && upper.slot_no == end->get_size()) {
                    // go to next leaf
                    upper.slot_no = 0;
                    upper.page_no = end->get_next_leaf();
                }

                if (lower.slot_no == end->get_size()) {
                    // keep valid slot
                    lower.slot_no = end->get_size()-1;
                }

                sm_manager_->get_bpm()->unpin_page(begin->get_page_id(),false);
                sm_manager_->get_bpm()->unpin_page(end->get_page_id(),false);
        
                scan_ = std::make_unique<IxScan>(ix_handle,lower,upper,sm_manager_->get_bpm());
                rid_=scan_->rid();
            }

        }else{
            Iid no_node=Iid{0,0};
            scan_ = std::make_unique<IxScan>(ix_handle,no_node,no_node,sm_manager_->get_bpm());
        }
        delete key_lower;
        delete key_upper;        
    }

    void nextTuple() override {
        scan_->next();
    }

    std::unique_ptr<RmRecord> Next() override {
        for(;!scan_->is_end();nextTuple()){
            rid_=scan_->rid();

            //取出当前记录
            auto record_for_check = fh_->get_record(rid_, context_);

            //依次判断所有condition
            int cond_num = fed_conds_.size();   // 条件表达式的个数
            bool add = true;        // 是否返回该记录

            for (int i = 0; i < cond_num; i++){
                ConditionEvaluator Cal;
                bool t_o_f=Cal.evaluate(conds_[i], cols_check_, *record_for_check);
                if(!t_o_f){
                    //一旦有一个condition为false就终止求条件表达式值, continue, 取下一条记录
                    add = false;
                    break;
                }
            }
            if (!add){continue;}
            return record_for_check;
        }
        return nullptr;
    }

    bool is_end() const override{
        bool res=scan_->is_end();
    	return res;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    Rid &rid() override { 
        rid_=scan_->rid();
        return  rid_;
    }


};