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
#include "record/bitmap.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
	// Todo:
	// 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_={1,-1};
    int start_page=rid_.page_no;
    int start_slot=rid_.slot_no;
	//从第一页的-1号位开始遍历
    for(int i = start_page; i < file_handle_->file_hdr_.num_pages; i++){
        auto page_handle = file_handle_->fetch_page_handle(i);
		if (page_handle.page_hdr->num_records != 0) {
			rid_ = {i, Bitmap::next_bit(true, page_handle.bitmap, page_handle.file_hdr->num_records_per_page, -1)};
			file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
			return;
		}
    }
    rid_ = {file_handle_->file_hdr_.num_pages, -1};
    return;
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
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
	for (int i = rid_.page_no; i < max_pages_num; i++) {
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

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
	// Todo: 修改返回值
	return rid_.slot_no == -1;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
	return rid_;
}

