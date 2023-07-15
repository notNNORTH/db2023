#include<iostream>
#include<vector>
#include<limits>
#include<string.h>

class LoserTree {
public:
    int leaves_num;     // 叶子节点的数量
    std::vector<std::string> temp_files_;   // 存储所有临时文件的名称
    std::vector<int> treeNode;  // 定义非叶子结点数组，非叶子结点的值表示该结点记录的是哪一个数组来源的值，loser
    std::vector<int> winnerNode;// winner
    std::vector<RmRecord> node; // 定义叶子结点数组，对应的数组小标表示是那个数据源，值表示数据源中的数据
    std::vector<std::vector<Value>> order_by_cols;   // 存储参与比较的属性值

    std::vector<ColMeta> cols_;     // 参与比较的属性
    std::vector<bool> is_desc_;     // true降序, false升序
    int record_size_;       // 记录长度
    std::vector<int> file_num_;     // 每个文件中记录的个数
    std::vector<int> file_count_;     // 每个文件中下一个读取记录的下标

    LoserTree(std::vector<std::string> temp_files, std::vector<ColMeta> cols, std::vector<bool> is_desc,
                int record_size, std::vector<int> file_num ){
        leaves_num = temp_files.size();
        temp_files_ = temp_files;
        treeNode.assign(leaves_num, -1);
        winnerNode.assign(leaves_num, -1);

        cols_ = cols;
        is_desc_ = is_desc;
        record_size_ = record_size;
        file_num_ = file_num;
        file_count_.assign(leaves_num, 0);

        for (int i = 0; i < leaves_num; i++){
            RmRecord record = readNextFromFile(i);
            node.push_back(record);

            std::vector<Value> order_by_cols_row;
            for (int j = 0; j < cols_.size(); j++) {
                Value value = get_col_value(record, cols_[j]);
                order_by_cols_row.push_back(value);
            }
            order_by_cols.push_back(order_by_cols_row);
        }
    }

    // 获取叶子结点的值
    RmRecord get(int index){
        return node[index];
    }

    // 设置叶子结点值
    void set_leaf(int index){
        if (file_count_[index] < file_num_[index]){
            node[index] = readNextFromFile(index);

            std::vector<Value> order_by_cols_row;
            for (int j = 0; j < cols_.size(); j++) {
                Value value = get_col_value(node[index], cols_[j]);
                order_by_cols_row.push_back(value);
            }
            order_by_cols[index] = order_by_cols_row;
        }else{
            node[index].data = nullptr;
            node[index].size = 0;
        }
    }

    void set_tree(){
        if(leaves_num == 1){
            treeNode[0] = 0;
            return;
        }

        // 开始从最后一个非叶子结点调整
        int index = leaves_num - 1;
        for(; index > 0; --index){
            // 找到index对应参与比较的叶子节点
            int sons[2] = {-1, -1};
            
            sons[0] = index * 2 - leaves_num >= 0 ? index * 2 - leaves_num : winnerNode[index * 2];
            sons[1] = index * 2 - leaves_num + 1 >= 0 ? index * 2 - leaves_num + 1 : winnerNode[index * 2 + 1];

            if(is_tuple_less(get(sons[0]), get(sons[1]), order_by_cols[sons[0]], order_by_cols[sons[1]])){
                winnerNode[index] = sons[0];
                treeNode[index] = sons[1];
            }else{
                winnerNode[index] = sons[1];
                treeNode[index] = sons[0];
            }
        }
        treeNode[index] = winnerNode[index + 1];
    }

    //败者树从node结点index开始向上调整
    void adjust(int index){
        //找到index对应的父结点
        int father = (index + leaves_num) >> 1;

        // 不为根节点
        while(father > 0){
            // 中间节点还未初始化，直接将子节点下标赋值给他
            if(is_tuple_less(get(treeNode[father]), get(index), order_by_cols[treeNode[father]], order_by_cols[index])){
                int tmp = treeNode[father];
                treeNode[father] = index;
                index = tmp;  //向上继续比较，更新index
            }
            father /= 2;
        }
        treeNode[0] = index;  //最终胜者
    }

    RmRecord get_tuple(){
        return node[treeNode[0]];   //败者树的0号节点存放的就是当前最小的node值索引
    }

    // 给定文件名，获取下一个元组
    RmRecord readNextFromFile(int index) {
        // 1.获取文件名
        const std::string& file_name = temp_files_[index];
        std::ifstream input_file(file_name, std::ios::binary);
        if (!input_file) {
            throw FileNotFoundError(file_name);
        }

        // 2.获取数据
        char* data = new char[record_size_];
        std::unique_ptr<RmRecord> record(new RmRecord());

        input_file.seekg(file_count_[index] * record_size_); // 设置读取位置为 begin
        input_file.read(data, record_size_);
        file_count_[index]++;
        record->data = data;
        record->size = record_size_;

        return *record;
    }

    bool is_tuple_less(const RmRecord& record1, const RmRecord& record2,
                    const std::vector<Value>& order_cols1, const std::vector<Value>& order_cols2) {
        if (record1.size == 0){ return false; };
        if (record2.size == 0){ return true; };

        for (size_t i = 0; i < order_cols1.size(); i++) {
            const Value& value1 = order_cols1[i];
            const Value& value2 = order_cols2[i];
            if (is_desc_[i]) {
                if (ConditionEvaluator::isGreaterThan(value1, value2))
                    return true;
                if (ConditionEvaluator::isLessThan(value1, value2))
                    return false;
            } else {
                if (ConditionEvaluator::isLessThan(value1, value2))
                    return true;
                if (ConditionEvaluator::isGreaterThan(value1, value2))
                    return false;
            }
        }
        return false;
    }

    // 给定record和元组的属性，返回属性值
    Value get_col_value(RmRecord& record, ColMeta& col) {
        int offset = col.offset;
        Value value;
        auto type = col.type;
        if (type == TYPE_INT) {
            char* charPointer1 = reinterpret_cast<char*>(record.data + col.offset);
            int int_val = *reinterpret_cast<int*>(charPointer1);
            value.set_int(int_val);
            value.init_raw(col.len);
            return value;
        } else if (type == TYPE_FLOAT) {
            char* charPointer2 = reinterpret_cast<char*>(record.data + col.offset);
            double float_val = *reinterpret_cast<double*>(charPointer2);
            value.set_float(float_val);
            value.init_raw(col.len);
            return value;
        } else if (type == TYPE_STRING) {
            char* charPointer3 = reinterpret_cast<char*>(record.data + col.offset);
            std::string str(charPointer3, charPointer3 + col.len);
            value.set_str(str);
            value.init_raw(col.len);
            return value;
        } else if (type == TYPE_BIGINT) {
            char* charPointer4 = reinterpret_cast<char*>(record.data + col.offset);
            BigInt bigint_val = *reinterpret_cast<BigInt*>(charPointer4);
            value.set_bigint(bigint_val);
            value.init_raw(col.len);
            return value;
        } else if (type == TYPE_DATETIME) {
            char* charPointer5 = reinterpret_cast<char*>(record.data + col.offset);
            DateTime datetime_val = *reinterpret_cast<DateTime*>(charPointer5);
            value.set_datetime(datetime_val);
            value.init_raw(col.len);
            return value;
        }
        return value;
    }
};