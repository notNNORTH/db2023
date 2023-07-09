//rz-dev
#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

#include <set>
//重写rmrecord便于比较，不想动rmrecord原函数
struct Record {
    char* data;  // 记录的数据
    int size;    // 记录的大小

    Record(RmRecord rmrecord) {
        data = new char();
        size = rmrecord.size;
        std::memcpy(data,rmrecord.data,size);
        
    }

    bool operator==(const Record& other) const {
        for (int i = 0; i < size; i++) {
            if (data[i] != other.data[i]) return false;
        }
        return true;
    }

    bool operator>(const Record& other) const {
        for (int i = 0; i < size; i++) {
            if (data[i] < other.data[i]) return false;
            if (data[i] > other.data[i]) return true;
        }
        return false;
    }

    bool operator<(const Record& other) const {
        for (int i = 0; i < size; i++) {
            if (data[i] < other.data[i]) return true;
            if (data[i] > other.data[i]) return false;
        }
        return false;
    }

    bool operator!=(const Record& other) const {
        return !(*this == other);
    }
};
class AggregateExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;                             
    //size_t tuple_num;
    std::vector<AggreOp> aops;
    //std::vector<size_t> used_tuple;
    //std::unique_ptr<RmRecord> current_tuple;
    int current;//指向第i个符号
    AggreOp cur_op;
    bool isend;
    int len; //长度
    std::vector<ColType> temptype;
    std::vector<ColMeta> colsout;//为了代码的一致性新建一个colsout实现输出cols的type,offset,len的重写,即调用cols()输出colout而不是cols
   public:
    AggregateExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<AggreOp> aops_,const std::vector<ColMeta> &all_cols ) {
        prev_ = std::move(prev);
        aops = aops_ ;
        cols_= all_cols;
        //tuple_num = 0;
        //used_tuple.clear();
        cur_op = aops[0];
        isend=false;
        current = 0;
        //_abstract_rid = prev_ ->rid();
        //context_ = context;
        colsout = cols_;
        for(int i = 0;i < cols_.size();i++){
            if(aops[i] == TYPE_COUNT || aops[i] == TYPE_COUNTALL){                 
               colsout[i].type = TYPE_INT;
               colsout[i].len = sizeof(int);
            }
        }

    }

    void beginTuple() override { 
        prev_ -> beginTuple();
    }

    void nextTuple() override {
        // if(prev_ -> is_end()){
        //     if(current == aops.size() - 1){
        //         isend = true;
        //     }
        //     else{
        //         current++;
        //         cur_op = aops[current];
        //         prev_ -> beginTuple(); 
        //     }
        // }
        // else{
        //    prev_ -> nextTuple();
        // }
        isend = true;
    }

    std::unique_ptr<RmRecord> Next() override {
        if(isend){return nullptr;}
        std::vector<double> temps;
        int len = 0;
        while(current < aops.size()){ //对于每个运算符循环          
            double temp = 0;
            ColType type = cols_[current].type;
            bool flag = true;//判断是否第一次进入min/max判断
            switch(aops[current]){
                case(TYPE_SUM):                    
                    while(!prev_ -> is_end()){
                        std::unique_ptr<RmRecord> cur_rec = prev_ -> Next();
                        if(type == TYPE_INT){
                            int cur_int = *(int*)((cur_rec -> data) + cols_[current].offset); //取出所要列的值
                            temp += cur_int;
                           
                        }
                        else{
                            double cur_float = *(double*)((cur_rec -> data) + cols_[current].offset); //取出所要列的值
                            temp += cur_float;                            
                        }
                        prev_ -> nextTuple();         
                    }
                    break;
                case(TYPE_MAX):
                    while(!prev_ -> is_end()){
                        std::unique_ptr<RmRecord> cur_rec = prev_ -> Next();
                        if(type == TYPE_INT){
                            int cur_int = *(int*)((cur_rec -> data) + cols_[current].offset); //取出所要列的值
                            if(flag){
                                temp = cur_int;
                                flag = false;
                            }        
                            else temp = temp > cur_int ? temp : cur_int;
                            
                        }
                        else{
                            double cur_float = *(double*)((cur_rec -> data) + cols_[current].offset); //取出所要列的值
                            if(flag){
                                temp = cur_float;
                                flag = false;
                            } 
                            else temp = temp > cur_float ? temp : cur_float;
                        }
                        prev_ -> nextTuple();                           
                    }
                    break;
                case(TYPE_MIN):
                    while(!prev_ -> is_end()){
                            std::unique_ptr<RmRecord> cur_rec = prev_ -> Next();
                            if(type == TYPE_INT){
                                int cur_int = *(int*)((cur_rec -> data) + cols_[current].offset); //取出所要列的值
                                if(flag){
                                    temp = cur_int;
                                    flag = false;
                                } 
                                else temp = temp < cur_int ? temp : cur_int;
                            }
                            else{
                                double cur_float = *(double*)((cur_rec -> data) + cols_[current].offset); //取出所要列的值
                                    if(flag){
                                        temp = cur_float;
                                        flag = false;
                                    } 
                                    else temp = temp < cur_float ? temp : cur_float;
                            }
                            prev_ -> nextTuple();                             
                        }
                        break;
                case(TYPE_COUNT):{                   
                    std::set<double> tempset;
                    std::set<std::string> tempsetstr;
                    while(!prev_ -> is_end()){
                        std::unique_ptr<RmRecord> cur_rec = prev_ -> Next();
                        if(type == TYPE_INT){
                            int cur_int = *(int*)((cur_rec -> data) + cols_[current].offset); //取出所要列的值
                            tempset.insert(cur_int);
                            
                        }
                        else if(type == TYPE_FLOAT){
                            double cur_float = *(double*)((cur_rec -> data) + cols_[current].offset); //取出所要列的值
                            tempset.insert(cur_float);                                
                        }else if(type == TYPE_STRING){
                            std::string cur_str(cur_rec->data + cols_[current].offset); //取出所要列的值
                            tempsetstr.insert(cur_str);
                        }
                        prev_ -> nextTuple();                             
                    }
                    temp = tempset.size()+tempsetstr.size();//一个为0,一个为所求,和为所求               
                    break;
                    } 
                case(TYPE_COUNTALL):{                                       
                    std::set<Record> countallset;
                    while(!prev_ -> is_end()){
                        std::unique_ptr<RmRecord> cur_rec = prev_ -> Next();
                        Record *rec=new Record(*cur_rec.get());
                        countallset.insert(*rec);
                        prev_ -> nextTuple(); 
                    }
                    temp = countallset.size();               
                break;
                }
            }
            if(aops[current] != TYPE_COUNT){
                temptype.push_back(type);
            }                    
            else{
                temptype.push_back(TYPE_INT);
            }
            temps.push_back(temp);
            current++;
        }
        //构造输出
        char *data = new char[len];
        char *cur_data = data; 
        int offset = 0;
        int i = 0;//指向当前处理col数
        std::vector<int> lens;//记录每条记录的长度
        std::vector<int> offsets;//记录每条记录的offsets
        for(auto type : temptype){
            switch(type){
                case(TYPE_INT):                    
                    *(int*)cur_data = (int)temps[i];
                    offsets.push_back(offset);
                    offset += sizeof(int);
                    cur_data += sizeof(int);
                    len += sizeof(int);
                    lens.push_back(sizeof(int));
                    break;
                case(TYPE_FLOAT): 
                    //*(float*)cur_data = (float)temps[i]; //输出要求为float类型
                    offsets.push_back(offset);
                    double f = (double)temps[i];
                    std::memcpy(data + offset, &f, sizeof(double));
                    offset += sizeof(double);
                    cur_data += sizeof(double); 
                    len += sizeof(double);
                    lens.push_back(sizeof(double));
                    break;               
            }
            i++;
        }   
        //将cols改为输出时的状态（len,type)否则输出为原来的cols类型而不是求得的类型
        for(int j = 0;j < cols_.size();j++){
            //colsout[j].type = temptype[j];
            colsout[j].len = lens[j];
            colsout[j].offset = offsets[j];
        }

        return std::make_unique<RmRecord>(len, data);
    }

    Rid &rid() override { return _abstract_rid; }
    bool is_end() const override{
        return isend;
    }
    const std::vector<ColMeta> &cols() const override {
    // 提供适当的实现，返回具体的 ColMeta 对象或者 std::vector<ColMeta>
        return colsout;
    }
};

