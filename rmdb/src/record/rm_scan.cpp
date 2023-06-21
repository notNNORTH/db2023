/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    start_page = 1;
    rid_=RID(start_page,0);//从第一页的第0个slot开始

}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    // 获取当前文件的第一个存放数据的页号
    int start_page = rid_.page_no;
    int start_slot = rid_.slot_no+1;
    int max_records=file_handle_->file_hdr->num_records_per_page;
    
    // 遍历页面，找到下一个非空闲的位置
    for (int page_no = start_page; page_no < file_handle_->file_hdr->num_pages; ++page_no) {
        RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);
        char* bitmap_temp=page_handle.page_hdr->bitmap;
        if (page_handle.page_hdr->next_free_page_no==-1) {
            // 遍历记录槽位，找到下一个非空闲槽位
            for (int slot = start_slot; slot < max_records; ++slot) {
                if (bitmap_temp[slot/8]&(1<<(slot%8))==1) {//用位图检查该slot是否不空闲
                    // 找到非空闲槽位，更新rid_并返回
                    rid_ = Rid(page_no, slot);
                    return;
                }
            }
        }
    }
    // 若没有找到非空闲位置，则将rid_置为无效
    rid_ = Rid(-1, -1);

}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值

    // 获取文件中最后一个记录的位置
    Rid last_record = Rid(file_handle_->file_hdr->num_pages - 1, file_handle_->file_hdr->num_records_per_page - 1);

    // 判断当前记录的位置是否在最后一个记录之后
    return rid_ > last_record;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}