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

class SeqRecScan : public RecScan {
private:
    // 其他成员变量和方法
    const RmFileHandle *file_handle_;
    Rid rid_;
public:
    SeqRecScan(RmFileHandle *fh_) : file_handle_(fh_) {
        // Todo:
	    // 初始化file_handle和rid（指向第一个存放了记录的位置）
        rid_={1,-1};
        int start_page=rid_.page_no;
        int start_slot=rid_.slot_no;
        int i;
        for(i = start_page; i < file_handle_->file_hdr_.num_pages; i++){
            RmPageHandle page_handle = file_handle_->fetch_page_handle(i);
            if (page_handle.page_hdr->num_records != 0) {
                rid_ = {i, Bitmap::next_bit(true, page_handle.bitmap, page_handle.file_hdr->num_records_per_page, -1)};
                file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
                return;
            }
        }
        if(i==1&&i==file_handle_->file_hdr_.num_pages){
            RmPageHandle page_handle = file_handle_->fetch_page_handle(i);
            rid_ = {i, Bitmap::next_bit(true, page_handle.bitmap, page_handle.file_hdr->num_records_per_page, -1)};
            file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            return;
        }
        rid_ = {file_handle_->file_hdr_.num_pages, -1};
        return;
    }

    void next() override {
        // 实现获取下一个记录的逻辑
        // Todo:
	    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

	    //设置一个标志位来指示是在当前页还是下一页搜寻
	    bool flag = false;
	    // 当前槽位为 -1（表示结束或初始状态），则无需继续执行，函数直接返回
	    if (rid_.slot_no == -1) {
		    return;
	    }
	    int max_pages_num=file_handle_->file_hdr_.num_pages;

	    //for循环扫描
	    for (int i = rid_.page_no; i <= max_pages_num; i++) {
		    RmPageHandle page_handle = file_handle_->fetch_page_handle(i);

            if (page_handle.page_hdr->num_records != 0) {
                int slot_no = Bitmap::next_bit(true, page_handle.bitmap, page_handle.file_hdr->num_records_per_page, flag ? -1 : rid_.slot_no);
                int page_end=page_handle.file_hdr->num_records_per_page;
                //next_bit扫描到了页面末尾仍未找到1位,进入下一次for循环扫描下一页
                if (slot_no == page_end) {
                    rid_.slot_no = -1;
                    continue;
                }
                //nextbit找到了1位
                else{
                    rid_ = {i, slot_no};
                    file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
                    return;
                }
            }
            //unpin
            file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            //置标志位为1，从本页的slotno而不是-1开始寻找
            flag = true;
	    }
	    rid_ = {max_pages_num, -1};
	    return;
    }

    bool is_end() const override {
        // 实现判断是否到达记录集的末尾的逻辑
        return rid_.slot_no == -1;
    }

    Rid rid() const override {
        // 返回当前记录的 RID
        return rid_;
    }
};



class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<SeqRecScan> scan_;     // table_iterator

    SmManager *sm_manager_;
    std::vector<ColMeta> cols_check_;         // 条件语句中所有用到列的列的元数据信息

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;
        //hashmap
        context_ = context;

        fed_conds_ = conds_;//后续查询优化可以从fedcond入手!!!!!!!!!!!!!!!!!!!
        //初始化条件语句中所有用到列的列的元数据信息
        int con_size=fed_conds_.size();
        for (int loop=0;loop<con_size;loop++) {
            auto temp_con=fed_conds_[loop];

            // 检查左操作数是否为列操作数
            if (!temp_con.lhs_col.tab_name.empty() && !temp_con.lhs_col.col_name.empty()) {
                // 查找colmeta
                auto tabname_in_con_left=temp_con.lhs_col.tab_name;
                auto colname_in_con_left=temp_con.lhs_col.col_name;
                int size=tab.cols.size();
                for(int i=0;i<size;i++){//后续join也可能改tab，加入多表内容
                    if((tabname_in_con_left==tab.cols[i].tab_name)&&(colname_in_con_left==tab.cols[i].name)){
                        auto temp=tab.cols[i];
                        cols_check_.push_back(temp);
                    }
                }
            }
            // 检查右操作数是否为列操作数
            if (!temp_con.is_rhs_val && !temp_con.rhs_col.tab_name.empty() && !temp_con.rhs_col.col_name.empty()) {
                // 查找colmeta
                auto tabname_in_con_right=temp_con.rhs_col.tab_name;
                auto colname_in_con_right=temp_con.rhs_col.col_name;
                int size=tab.cols.size();
                for(int i=0;i<size;i++){//后续join也可能改tab，加入多表内容
                    if((tabname_in_con_right==tab.cols[i].tab_name)&&(colname_in_con_right==tab.cols[i].name)){
                        auto temp=tab.cols[i];
                        cols_check_.push_back(temp);
                    }
                }
            }
        }
    }

    const std::vector<ColMeta> &cols() const override {
    // 提供适当的实现，返回具体的 ColMeta 对象或者 std::vector<ColMeta>
        return cols_;
    }

    void beginTuple() override {
        // 初始化执行器的状态，准备开始遍历记录
        // 这个函数在第一次调用 Next() 前会被自动调用
        // 可以在这里进行一些初始化操作
    
        // 初始化记录迭代器
        scan_ = std::make_unique<SeqRecScan>(fh_);
        // 设置初始 RID
        rid_ = scan_->rid();
        _abstract_rid=rid_;

    }

    void nextTuple() override {
        scan_->next();
        rid_=scan_->rid();
        _abstract_rid=rid_;
    }

    bool is_end() const override{
    	return rid_.slot_no == -1;
    }

    std::unique_ptr<RmRecord> Next() override {

        //-1表明为空或到达了末尾
        if(rid_.slot_no==-1){
            return nullptr;
        }

        //设置标志位，表示是否找到符合条件record
        bool found=false;

        //进入循环，寻找到符合条件的record就返回，否则继续取下一条record
        for(;!scan_->is_end();nextTuple()){

            //取出当前记录
            auto record_for_check = fh_->get_record(rid_,nullptr);

            //标志位，表达式是否为true
            bool right=false;

            //依次判断所有condition
            int right_num=0;
            int should_right=fed_conds_.size();
            for (const auto& condition : fed_conds_) {
                if(!ConditionEvaluator::evaluate(condition,cols_check_,*record_for_check)){

                    //一旦有一个condition为false就终止求条件表达式值 break,然后进入外部的continue，取下一条记录
                    break;
                }

                //该条条件语句为true，计数器加一
                right_num++;
                
                //所有条件语句都true,返回该条记录
                if(right_num==should_right){
                    return record_for_check;
                }
            }
            continue;
        }
    }

    Rid &rid() override { return rid_; }
};