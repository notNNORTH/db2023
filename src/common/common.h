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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "defs.h"
#include "record/rm_defs.h"


struct TabCol {
    std::string tab_name;
    std::string col_name;

    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }
};

struct Value {
    ColType type;  // type of value
    union {
        int int_val;      // int value
        double float_val;  // float value  --by 星穹铁道高手
    };
    std::string str_val;  // string value
    BigInt bigint_val;
    DateTime datetime_val;

    std::shared_ptr<RmRecord> raw;  // raw record buffer

    void set_int (int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(double float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void set_bigint(BigInt bigint_val_) {
        type = TYPE_BIGINT;
        bigint_val = bigint_val_;
    }
    void set_datetime(DateTime datetime_val_) {
        type = TYPE_DATETIME;
        datetime_val = datetime_val_;
    }

    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(double));
            *(double *)(raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        } else if (type == TYPE_BIGINT){
            //8byte
            assert(len == sizeof(BigInt));
            memset(raw->data, 0, len);
            memcpy(raw->data, &bigint_val, len);
        }else if (type == TYPE_DATETIME){
            //8byte
            assert(len == sizeof(DateTime));
            memset(raw->data, 0, len);
            memcpy(raw->data, &datetime_val, len);
        }
    }
};

enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };

struct Condition {
    TabCol lhs_col;   // left-hand side column
    CompOp op;        // comparison operator
    bool is_rhs_val;  // true if right-hand side is a value (not a column)
    TabCol rhs_col;   // right-hand side column
    Value rhs_val;    // right-hand side value
};

struct SetClause {
    TabCol lhs;
    Value rhs;
};

class ConditionEvaluator {
public:
    bool evaluate(Condition& condition, std::vector<ColMeta>& cols, RmRecord& record) {
        Value invalid=Value{};
        Value& lhsValue = getOperandValue(condition.lhs_col, cols, record, invalid);
        Value& rhsValue = getOperandValue(condition.rhs_col, cols, record, condition.rhs_val);

        if(lhsValue.type != rhsValue.type){
            if(lhsValue.type == TYPE_BIGINT && rhsValue.type == TYPE_INT){
                BigInt bigint(rhsValue.int_val);
                rhsValue.set_bigint(bigint);
            }else if(lhsValue.type == TYPE_INT && rhsValue.type == TYPE_BIGINT){
                BigInt bigint(rhsValue.int_val);
                rhsValue.set_bigint(bigint);
            }else if(lhsValue.type == TYPE_STRING && rhsValue.type == TYPE_DATETIME){
                std::string str = rhsValue.datetime_val.get_datetime();
                rhsValue.set_str(str);
            }
        }
        switch (condition.op) {
            case OP_EQ:
                return isEqual(lhsValue, rhsValue);
            case OP_NE:
                return !isEqual(lhsValue, rhsValue);
            case OP_LT:
                return isLessThan(lhsValue, rhsValue);
            case OP_GT:
                return isGreaterThan(lhsValue, rhsValue);
            case OP_LE:
                return isLessThanOrEqual(lhsValue, rhsValue);
            case OP_GE:
                return isGreaterThanOrEqual(lhsValue, rhsValue);
            default:
                throw std::string("Invalid comparison operator");
        }
    }
    bool evaluate(Condition& condition, std::vector<ColMeta>& cols, RmRecord& record_l, RmRecord& record_r){
        Value invalid=Value{};
        Value& lhsValue = getOperandValue(condition.lhs_col, cols, record_l, invalid);
        Value& rhsValue = getOperandValue(condition.rhs_col, cols, record_r, condition.rhs_val);
        //rz-dev
        if(lhsValue.type != rhsValue.type){
            if(lhsValue.type == TYPE_BIGINT && rhsValue.type == TYPE_INT){
                    BigInt bigint(rhsValue.int_val);
                    rhsValue.set_bigint(bigint);
                }else if(lhsValue.type == TYPE_INT && rhsValue.type == TYPE_BIGINT){
                    BigInt bigint(rhsValue.int_val);
                    rhsValue.set_bigint(bigint);
                }
        }
        switch (condition.op) {
            case OP_EQ:
                return isEqual(lhsValue, rhsValue);
            case OP_NE:
                return !isEqual(lhsValue, rhsValue);
            case OP_LT:
                return isLessThan(lhsValue, rhsValue);
            case OP_GT:
                return isGreaterThan(lhsValue, rhsValue);
            case OP_LE:
                return isLessThanOrEqual(lhsValue, rhsValue);
            case OP_GE:
                return isGreaterThanOrEqual(lhsValue, rhsValue);
            default:
                throw std::string("Invalid comparison operator");
        }
        
    }

private:
    Value& getOperandValue(TabCol& col, std::vector<ColMeta>& cols, RmRecord& record, Value& value) {
        
        if (col.tab_name.empty() && col.col_name.empty()) {
            // 使用常量值
            return value;
        } else {
            // 使用列值
            value=Value{};
            for (const auto& meta : cols) {
                if (meta.tab_name == col.tab_name && meta.name == col.col_name) {
                    auto type=meta.type;
                    if(type==TYPE_INT){
                        char* charPointer1 = reinterpret_cast<char*>(record.data + meta.offset);  
                        int int_val = *reinterpret_cast<int*>(charPointer1);
                        value.set_int(int_val);
                        value.init_raw(meta.len);
                        return value;
                    }else if(type==TYPE_FLOAT){
                        char* charPointer2 = reinterpret_cast<char*>(record.data + meta.offset);  
                        double float_val = *reinterpret_cast<double*>(charPointer2);
                        value.set_float(float_val);
                        value.init_raw(meta.len);
                        return value;
                    }else if(type==TYPE_STRING){
                        char* charPointer3 = reinterpret_cast<char*>(record.data + meta.offset); 
                        std::string str(charPointer3, charPointer3+meta.len); 
                        value.set_str(str);
                        value.init_raw(meta.len);
                        return value;
                    }else if(type==TYPE_BIGINT){
                        char* charPointer4 = reinterpret_cast<char*>(record.data + meta.offset);  
                        BigInt bigint_val = *reinterpret_cast<BigInt*>(charPointer4);
                        value.set_bigint(bigint_val);
                        value.init_raw(meta.len);
                        return value;
                    }else if(type == TYPE_DATETIME){
                        char* charPointer5 = reinterpret_cast<char*>(record.data + meta.offset);  
                        DateTime datetime_val = *reinterpret_cast<DateTime*>(charPointer5);
                        value.set_datetime(datetime_val);
                        value.init_raw(meta.len);
                        return value;
                    }
                }
            }
            throw std::string("Invalid column reference");
        }
    }

    static bool isEqual(const Value& lhs, const Value& rhs) {
        if((lhs.type==TYPE_INT)&&(rhs.type==TYPE_FLOAT)){
            return lhs.int_val == rhs.float_val;
        }else if((lhs.type==TYPE_FLOAT)&&(rhs.type==TYPE_INT)){
            return lhs.float_val == rhs.int_val;
        }else if((lhs.type==TYPE_INT)&&(rhs.type==TYPE_INT)){
            return lhs.int_val == rhs.int_val;
        }else if((lhs.type==TYPE_FLOAT)&&(rhs.type==TYPE_FLOAT)){
            return lhs.float_val == rhs.float_val;
        }else if((lhs.type==TYPE_STRING)&&(rhs.type==TYPE_STRING)){
            return lhs.str_val == rhs.str_val;
        }else if((lhs.type==TYPE_BIGINT)&&(rhs.type==TYPE_BIGINT)){
            return lhs.bigint_val == rhs.bigint_val;
        }else if((lhs.type==TYPE_DATETIME)&&(rhs.type==TYPE_DATETIME)){
            return lhs.datetime_val == rhs.datetime_val;
        }else{
            throw std::string("Invalid value type");
        }
    }

    static bool isLessThan(const Value& lhs, const Value& rhs) {
        if((lhs.type==TYPE_INT)&&(rhs.type==TYPE_FLOAT)){
            return lhs.int_val < rhs.float_val;
        }else if((lhs.type==TYPE_FLOAT)&&(rhs.type==TYPE_INT)){
            return lhs.float_val < rhs.int_val;
        }else if((lhs.type==TYPE_INT)&&(rhs.type==TYPE_INT)){
            return lhs.int_val < rhs.int_val;
        }else if((lhs.type==TYPE_FLOAT)&&(rhs.type==TYPE_FLOAT)){
            return lhs.float_val < rhs.float_val;
        }else if((lhs.type==TYPE_STRING)&&(rhs.type==TYPE_STRING)){
            return lhs.str_val < rhs.str_val;
        }else if((lhs.type==TYPE_BIGINT)&&(rhs.type==TYPE_BIGINT)){
            return lhs.bigint_val < rhs.bigint_val;
        }else if((lhs.type==TYPE_DATETIME)&&(rhs.type==TYPE_DATETIME)){
            return lhs.datetime_val < rhs.datetime_val;
        }else{
            throw std::string("Invalid value type");
        }
    }

    static bool isGreaterThan(const Value& lhs, const Value& rhs) {
        if((lhs.type==TYPE_INT)&&(rhs.type==TYPE_FLOAT)){
            return lhs.int_val > rhs.float_val;
        }else if((lhs.type==TYPE_FLOAT)&&(rhs.type==TYPE_INT)){
            return lhs.float_val > rhs.int_val;
        }else if((lhs.type==TYPE_INT)&&(rhs.type==TYPE_INT)){
            return lhs.int_val > rhs.int_val;
        }else if((lhs.type==TYPE_FLOAT)&&(rhs.type==TYPE_FLOAT)){
            return lhs.float_val > rhs.float_val;
        }else if((lhs.type==TYPE_STRING)&&(rhs.type==TYPE_STRING)){
            return lhs.str_val > rhs.str_val;
        }else if((lhs.type==TYPE_BIGINT)&&(rhs.type==TYPE_BIGINT)){
            return lhs.bigint_val > rhs.bigint_val;
        }else if((lhs.type==TYPE_DATETIME)&&(rhs.type==TYPE_DATETIME)){
            return lhs.datetime_val > rhs.datetime_val;
        }else{
            throw std::string("Invalid value type");
        }
    }

    static bool isLessThanOrEqual(const Value& lhs, const Value& rhs) {
        return isEqual(lhs, rhs) || isLessThan(lhs, rhs);
    }

    static bool isGreaterThanOrEqual(const Value& lhs, const Value& rhs) {
        return isEqual(lhs, rhs) || isGreaterThan(lhs, rhs);
    }
};
