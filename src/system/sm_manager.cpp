/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    

    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    // 若数据库不存在报错
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }

    // 切换到数据库目录
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }

    // 打开数据库元数据文件
    std::ifstream ifs(DB_META_NAME);

    // 读取数据库元数据
    ifs >> db_ ;  // 注意：此处重载了操作符>>

    // 关闭数据库元数据文件
    ifs.close();

    // 进行其他打开数据库的操作，例如加载表信息、初始化缓存等
    // 创建fhs,ihs
    // 遍历tabs
     for (auto it = db_.tabs_.begin(); it != db_.tabs_.end(); ++it) {
    // 使用 it->first 访问键，it->second 访问值
    // 在此处处理键值对的逻辑
    // 表名=文件名 用name代指
        
        std::string name = it -> first;
        
    // 用表名获取指针,用打开文件返回Filedl指针,再关闭文件-----------可简化直接将disk_manager createfile函数拿出来省一个步骤
        int fd = disk_manager_ -> open_file( name );
        
        std::unique_ptr<RmFileHandle> FileHdl = std::make_unique<RmFileHandle> ( disk_manager_, buffer_pool_manager_, fd);
        
        disk_manager_->close_file(fd);
        
    // 用fd创建索引  TODO--------------------------------------------------------------将来索引要进行修改 
        //std::unique_ptr<IxIndexHandle> IdxHdl = std::make_unique<IxIndexHandle>( disk_manager_, buffer_pool_manager_ , fd);
        
    // 插入map
        fhs_.emplace( name , std::move(FileHdl) );
        //ihs_.emplace( name , std::move(IdxHdl) );
        

    }   
    


}

/**s
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    //关闭数据库，删除信息释放空间
    flush_meta();
    delete disk_manager_;
    delete buffer_pool_manager_;
    delete rm_manager_;
    delete ix_manager_;
    delete &db_;
    
    // 切换回根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }


}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {

    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();

}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {

    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {

    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();


}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    //如果没有该表名报错


    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    //获取表的handle
    TabMeta TableD = db_.get_table(tab_name);
    //删除文件
    int fd = disk_manager_->get_file_fd(tab_name);
    disk_manager_-> close_file(fd);
    rm_manager_->destroy_file(tab_name);

    //在fhs中找到表的filehdl并删除记录
    fhs_.erase(tab_name);

    //在db tab_表中删除
    db_.tabs_.erase(tab_name);

    //删除当前table,table中为vector型变量会自动回收，map中table也会自动回收，所以不用调用delete
    //将数据刷盘
    flush_meta();


}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    auto idx_file_exist=ix_manager_->exists(tab_name,col_names);
    if(!idx_file_exist){
        //指定表里所有的列的meta值,指定的idx的数量
        auto& tab_col_meta=db_.tabs_[tab_name].cols;
        auto idx_num=col_names.size();
        auto tab_col_meta_size=tab_col_meta.size();

        //存指定idx列的meta值
        std::vector<ColMeta> idx_col_meta;

        //选择出idx对应的列的meta值

        int col_tot_len = 0;

        for(int i=0;i<idx_num;i++){

            bool found=false;
            auto idx_col_temp=col_names[i];

            for(int j=0;j<tab_col_meta_size;j++){

                auto& col_meta_ref=tab_col_meta[j];

                if(idx_col_temp==col_meta_ref.name){
                    found=true;
                    col_meta_ref.index=true;
                    idx_col_meta.push_back(col_meta_ref);
                    col_tot_len=col_tot_len+col_meta_ref.len;
                }

                if(found==true){
                    break;
                }

            }

            if(found==false){
                throw IndexNotFoundError(tab_name,col_names);
            }

        }

        //创建.idx文件
        ix_manager_->create_index(tab_name,idx_col_meta);

        //tabmeta.IndexMeta vector,更新indexes数组
        IndexMeta temp=IndexMeta{tab_name,col_tot_len,idx_num,idx_col_meta};
        db_.tabs_[tab_name].indexes.push_back(temp);

        // 更新ihs
        std::string ix_name = ix_manager_->get_index_name(tab_name, col_names);
        std::unique_ptr<IxIndexHandle> index_handle_ = ix_manager_->open_index(tab_name,col_names);
        ihs_.insert(std::make_pair(ix_name, std::move(index_handle_)));
        std::unique_ptr<IxIndexHandle>& index_handle = ihs_.at(ix_name);

        //如果表内已经有记录，idx文件需要初始化
        //扫描所有记录
        std::unique_ptr<RmFileHandle>& rmfile_handle = fhs_[tab_name];
        RmFileHandle* raw_rmfile_handle=rmfile_handle.get();
        RmScan *scan_init=new RmScan(raw_rmfile_handle);



        for(;!scan_init->is_end();scan_init->next()){
            auto record=raw_rmfile_handle->get_record(scan_init->rid(),context);
            char* key_buffer = new char[col_tot_len+1];  // 键的长度
            int offset=0;

            if(!record){break;}

            for (auto &col : idx_col_meta) {
                char *dest=key_buffer+offset;
                char *src=record->data+col.offset;
                memcpy(dest, src, col.len);
                offset=offset+col.len;
            }

            key_buffer[col_tot_len]='\0';
            index_handle->insert_entry(key_buffer,scan_init->rid(),nullptr);
        }
        //ix_manager_->close_index(index_handle.get());
        //将数据刷盘
        flush_meta();
    }

}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {

    auto idx_file_exist=ix_manager_->exists(tab_name,col_names);
    if(!idx_file_exist){
        throw IndexNotFoundError(tab_name,col_names);
    }
    else{
        auto index_handle=ihs_.at(ix_manager_->get_index_name(tab_name, col_names)).get();

        for(int i=2;i<index_handle->get_filehdr()->num_pages_;i++){
            PageId temp=PageId{index_handle->get_fd(),i};
            buffer_pool_manager_->delete_page(temp);
        }

        ix_manager_->close_index(index_handle);
        ix_manager_->destroy_index(tab_name,col_names);
        ihs_.erase(ix_manager_->get_index_name(tab_name,col_names));
        //修改indexes!!!!!!!
        auto& del_indexes=db_.tabs_[tab_name].indexes;
        //对表内的每一个index，进行对比
        for(int i=0;i<del_indexes.size();i++){

            //对colnames中的每一个列名，在当前indexmeta的cols中查找，只有全部都有且个数相同，才是对应的索引
            bool is_this_index=true;
            auto& index_it=del_indexes[i];
            int count_find_col=0;

            if((index_it.col_num==col_names.size())){
                
                for(int j=0;j<col_names.size();j++){
                    bool find_this_col=false;
                    auto col_name=col_names[j];
                    for(int it=0;it<index_it.cols.size();it++){
                        if(index_it.cols[it].name==col_name){
                            find_this_col=true;
                            break;
                        }
                    }
                    //只要有一个属性找不到就说明不是这个index
                    if(!find_this_col){
                        is_this_index=false;
                        break;
                    }
                    count_find_col++;
                }

            }

            if(is_this_index&&count_find_col==col_names.size()){
                auto it=del_indexes.begin()+i;
                del_indexes.erase(it);
                break;
            }

        }

        //修改colmeta!!!!!!!
        auto& tab_col_meta=db_.tabs_[tab_name].cols;

        for(int j=0;j<col_names.size();j++){
            for(int k=0;k<tab_col_meta.size();k++){
                if(col_names[j]==tab_col_meta[k].name){
                    tab_col_meta[k].index=false;
                    break;
                }
            }
        }

        flush_meta();
    }
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    std::vector<std::string> temp_col_names;
    for(int i=0;i<cols.size();i++){
        temp_col_names.push_back(cols[i].name);
    }
    drop_index(tab_name,temp_col_names,context);
}