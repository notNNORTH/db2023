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
                delete []min_str;
                delete []max_str;
                break;
            }
        }

        //分离出索引内列cond并根据列名\op分类
        bool continue_to_find=true;
        int match_col_in_cond=0;
        int match_col_in_idx=0;
        std::vector<Condition> in_idx_conds_;
        std::unordered_map<std::string, std::unordered_map<CompOp, std::vector<Condition>>> Col_Op_Conds;
        
        while(continue_to_find){
            int match_count=0;
            while(match_col_in_cond<fed_conds_.size()&&match_col_in_idx<index_col_names_.size()
                &&(fed_conds_[match_col_in_cond].lhs_col.col_name==index_col_names_[match_col_in_idx])){
                auto colname=index_col_names_[match_col_in_idx];
                auto op=fed_conds_[match_col_in_cond].op;
                Col_Op_Conds[colname][op].push_back(fed_conds_[match_col_in_cond]);
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
        
        //根据条件来初始化上界下界key
        int key_offset=0;
        for(int i=0;i<Col_Op_Conds.size();i++){
            auto colname=index_col_names_[i];
            auto Op_Conds=Col_Op_Conds[colname];
            auto EQ_conds=Op_Conds[OP_EQ];
            auto GE_conds=Op_Conds[OP_GE];
            auto GT_conds=Op_Conds[OP_GT];
            auto LE_conds=Op_Conds[OP_LE];
            auto LT_conds=Op_Conds[OP_LT];
            if(EQ_conds.size()>0){
                void *temp;
                set_value(EQ_conds[0],temp);
                memcpy(key_lower + key_offset, temp, index_meta_.cols[i].len);
                memcpy(key_upper + key_offset, temp, index_meta_.cols[i].len);
                key_offset+=index_meta_.cols[i].len;
            }else{
                int G_size=GE_conds.size()+GT_conds.size();
                int L_size=LE_conds.size()+LT_conds.size();
                assert((G_size+L_size)>0);
                if(L_size>0){
                    std::vector<Condition> L_conds;
                    L_conds.insert(L_conds.end(), LE_conds.begin(), LE_conds.end());
                    L_conds.insert(L_conds.end(), LT_conds.begin(), LT_conds.end());
                    Condition upper_bound=find_upper_cond(L_conds);
                    void* temp;
                    set_value(upper_bound,temp);
                    memcpy(key_upper + key_offset, temp, index_meta_.cols[i].len);
                }else if(G_size>0){
                    std::vector<Condition> G_conds;
                    G_conds.insert(G_conds.end(), GE_conds.begin(), GE_conds.end());
                    G_conds.insert(G_conds.end(), GT_conds.begin(), GT_conds.end());
                    Condition lower_bound=find_lower_cond(G_conds);
                    void* temp;
                    set_value(lower_bound,temp);
                    memcpy(key_lower + key_offset, temp, index_meta_.cols[i].len);
                }
                key_offset+=index_meta_.cols[i].len;
            }
        }

        if(ix_compare(key_lower,key_upper,ix_handle->get_filehdr()->col_types_,ix_handle->get_filehdr()->col_lens_)>0){
            Iid no_node=Iid{0,0};
            scan_ = std::make_unique<IxScan>(ix_handle,no_node,no_node,sm_manager_->get_bpm());
            rid_=scan_->rid();
            delete []key_lower;
            delete []key_upper; 
            return;
        }

        auto begin=(ix_handle->find_leaf_page(key_lower,Operation::FIND,nullptr)).first;
        auto end=(ix_handle->find_leaf_page(key_upper,Operation::FIND,nullptr)).first;

        if(begin==nullptr||end==nullptr||begin->get_size()==0||end->get_size()==0){
            sm_manager_->get_bpm()->unpin_page(begin->get_page_id(),false);
            sm_manager_->get_bpm()->unpin_page(end->get_page_id(),false);
            Iid no_node=Iid{0,0};
            scan_ = std::make_unique<IxScan>(ix_handle,no_node,no_node,sm_manager_->get_bpm());
            rid_=scan_->rid();
            delete []key_lower;
            delete []key_upper; 
            return;
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

            delete []key_lower;
            delete []key_upper; 
            return;
        }       
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

    int compare_value(Value v1,Value v2){
        switch (v1.type) {
            case TYPE_INT:
                if(v1.int_val<v2.int_val){
                    return -1;
                }else if(v1.int_val>v2.int_val){
                    return 1;
                }else{
                    return 0;
                }
            case TYPE_FLOAT:
                if(v1.float_val<v2.float_val){
                    return -1;
                }else if(v1.float_val>v2.float_val){
                    return 1;
                }else{
                    return 0;
                }
            case TYPE_STRING:
                if(v1.str_val<v2.str_val){
                    return -1;
                }else if(v1.str_val>v2.str_val){
                    return 1;
                }else{
                    return 0;
                }
            default:
                break;
        }
    }

    Condition find_upper_cond(std::vector<Condition> v){
        if(v.size()==1){
            return v[0];
        }else{
            Condition min_con=v[0];
            for(int i=1;i<v.size();i++){
                auto it_con=v[i];
                if((compare_value(it_con.rhs_val,min_con.rhs_val))<0){
                    min_con=it_con;
                }else if(((compare_value(it_con.rhs_val,min_con.rhs_val))==0)&&it_con.op==OP_LT){
                    min_con=it_con;
                }
            }
            return min_con;
        }
    }

    Condition find_lower_cond(std::vector<Condition> v){
        if(v.size()==1){
            return v[0];
        }else{
            Condition max_con=v[0];
            for(int i=1;i<v.size();i++){
                auto it_con=v[i];
                if((compare_value(it_con.rhs_val,max_con.rhs_val))>0){
                    max_con=it_con;
                }else if(((compare_value(it_con.rhs_val,max_con.rhs_val))==0)&&it_con.op==OP_GT){
                    max_con=it_con;
                }
            }
            return max_con;
        }
    }

    void set_value(Condition &con,void *&convey){
        switch (con.rhs_val.type) {
            case TYPE_INT:
                convey=&(con.rhs_val.int_val);
                break;
            case TYPE_FLOAT:
                convey=&(con.rhs_val.float_val);
                break;
            case TYPE_STRING:
                convey=&(con.rhs_val.str_val[0]);
                break;
            default:
                break;
        }
    }
};