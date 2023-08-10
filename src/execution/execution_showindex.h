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
#include "record_printer.h"

class ShowIndexExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;
    Rid rid_;

   public:
    ShowIndexExecutor(SmManager *sm_manager, const std::string &tab_name, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        tab_name_ = tab_name;
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
        rid_={-1,-1};
    };

    void beginTuple() override {
        
    }

    void nextTuple() override {
        
    }

    std::unique_ptr<RmRecord> Next() override {
        std::fstream outfile;
        outfile.open("output.txt", std::ios::out | std::ios::app);
        //outfile << "| Table | U/N | Index |\n";
        RecordPrinter printer(3);
        //printer.print_separator(context_);
        //printer.print_record({"Table","U/N","Index"}, context_);
        printer.print_separator(context_);
        

        for (int m=0;m<tab_.indexes.size();m++) {
            auto entry=tab_.indexes[m];
            std::string idx_str="(";
            for(int i=0;i<entry.cols.size();i++){
                auto col=entry.cols[i];
                idx_str=idx_str+col.name;
                if(i==entry.cols.size()-1){
                    idx_str=idx_str+")";
                    break;
                }
                idx_str=idx_str+",";
            }
            printer.print_record({entry.tab_name,"unique",idx_str}, context_);
            outfile << "| " << entry.tab_name <<" | "<<"unique"<<" | "<<idx_str<< " |\n";
        }
        printer.print_separator(context_);
        outfile.close();
        return nullptr;
    }

    Rid &rid() override { return rid_; }


    
};