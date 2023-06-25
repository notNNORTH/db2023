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
        float float_val;  // float value
    };
    std::string str_val;  // string value

    std::shared_ptr<RmRecord> raw;  // raw record buffer

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            *(float *)(raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
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
    static bool evaluate(const Condition& condition, const std::vector<ColMeta>& cols, const RmRecord& record) {
        const Value& lhsValue = getOperandValue(condition.lhs_col, cols, record);
        const Value& rhsValue = getOperandValue(condition.rhs_col, cols, record, condition.rhs_val);

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
    static const Value& getOperandValue(const TabCol& col, const std::vector<ColMeta>& cols, const RmRecord& record, const Value& value = {}) {
        if (col.tab_name.empty() && col.col_name.empty()) {
            // 使用常量值
            return value;
        } else {
            // 使用列值
            for (const auto& meta : cols) {
                if (meta.tab_name == col.tab_name && meta.name == col.col_name) {
                    char* dataPtr = record.data + meta.offset;
                    return *reinterpret_cast<Value*>(dataPtr);
                }
            }
            throw std::string("Invalid column reference");
        }
    }

    static bool isEqual(const Value& lhs, const Value& rhs) {
        if (lhs.type != rhs.type) {
            throw std::string("Type mismatch in comparison");
        }
        switch (lhs.type) {
            case TYPE_INT:
                return lhs.int_val == rhs.int_val;
            case TYPE_FLOAT:
                return lhs.float_val == rhs.float_val;
            case TYPE_STRING:
                return lhs.str_val == rhs.str_val;
            default:
                throw std::string("Invalid value type");
        }
    }

    static bool isLessThan(const Value& lhs, const Value& rhs) {
        if (lhs.type != rhs.type) {
            throw std::string("Type mismatch in comparison");
        }
        switch (lhs.type) {
            case TYPE_INT:
                return lhs.int_val < rhs.int_val;
            case TYPE_FLOAT:
                return lhs.float_val < rhs.float_val;
            case TYPE_STRING:
                return lhs.str_val < rhs.str_val;
            default:
                throw std::string("Invalid value type");
        }
    }

    static bool isGreaterThan(const Value& lhs, const Value& rhs) {
        if (lhs.type != rhs.type) {
            throw std::string("Type mismatch in comparison");
        }
        switch (lhs.type) {
            case TYPE_INT:
                return lhs.int_val > rhs.int_val;
            case TYPE_FLOAT:
                return lhs.float_val > rhs.float_val;
            case TYPE_STRING:
                return lhs.str_val > rhs.str_val;
            default:
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