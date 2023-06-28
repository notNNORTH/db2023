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

#include <iostream>
#include <map>

#include <fstream>

//添加
#include <string.h>

// 此处重载了<<操作符，在ColMeta中进行了调用
template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val) {
    os << static_cast<int>(enum_val);
    return os;
}

template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val) {
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

struct Rid {
    int page_no;
    int slot_no;

    friend bool operator==(const Rid &x, const Rid &y) {
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }
};

enum ColType {
    TYPE_INT, TYPE_FLOAT, TYPE_STRING , TYPE_BIGINT
};

inline std::string coltype2str(ColType type) {
    std::map<ColType, std::string> m = {
            {TYPE_INT,    "INT"},
            {TYPE_FLOAT,  "FLOAT"},
            {TYPE_STRING, "STRING"},
            {TYPE_BIGINT, "BIGINT"}
    };
    return m.at(type);
}

class RecScan {
public:
    virtual ~RecScan() = default;

    virtual void next() = 0;

    virtual bool is_end() const = 0;

    virtual Rid rid() const = 0;
};

class BigInt {
public:
    //由两个数10进制数拼成高位10位有符号，低位9位无符号
    int high;
    int low;
    BigInt() {
        high = 0;
        low = 0;
    }
    //由char* 生成BigInt构造函数
    BigInt(char* bit) {
        high = 0;
        low = 0;
        int len = strlen(bit);
        if (len <= 9) {
            if (bit[0] != '-') {
                for (int i = len - 1; i >= 0; i--) {
                    low += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 1)));
                }
            }
            else {
                for (int i = len - 1; i >= 1; i--) {
                    low += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 1)));
                }
                low = -low;
            }
        }
        //负9位数单独处理
        else if (len == 10 && bit[0] == '-') {
            for (int i = len - 1; i >= 1; i--) {
                low += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 1)));
            }
            low = -low;
        }
        else {
            if (bit[0] != '-') {
                for (int i = len - 10; i >= 0; i--) {
                    high += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 10)));
                }
                for (int i = len - 1; i >= len - 9; i--) {
                    low += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 1)));
                }
            }

            else {
                high = 0;
                low = 0;
                for (int i = len - 10; i >= 1; i--) {
                    high += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 10)));
                }
                for (int i = len - 1; i >= len - 9; i--) {
                    low += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 1)));
                }
                high = -high;

            }
        }
    }
    //操作符重载
    static int pow(int m, int n) {
        if (n == 0) return 1;
        return m * pow(m, n - 1);
        std::cout << "pow";
    }
    //输出
    friend std::ostream& operator<<(std::ostream& os, const BigInt& num) {
        //用0补全末位
        if (num.high == 0) {
            os << num.low << '\n';
            return os;
        }
        //用i表示位数共9位
        int i = 1;
        for (; i <= 9; ++i) {
            if (num.low < (pow(10 , i))) break;
        }
        os << num.high;
        for (; i < 9; i++) {
            os << 0;
        }
        os << num.low << '\n';
        return os;
    }
    //输入
    //输入 高位含-0会出错如-0123456789暂忽略此情况，可以添加代码if(bit[0] == '-' && bit[1] == '0') throw完成修改
    friend std::istream& operator>>(std::istream& is, BigInt& num) {
        //通过对长度大于9分类与对是否小于0分类
        num.high = 0;
        num.low = 0;
        char* bit = new char();
        is >> bit;
        int len = strlen(bit);
        if (len <= 9) {
            if(bit[0] != '-') {
                for (int i = len-1; i >= 0; i--) {
                    num.low += unsigned(bit[i] - '0') * unsigned(pow(10 ,(len - i - 1)));
                }
            }
            else {
                for (int i = len - 1; i >= 1; i--) {
                    num.low += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 1)));
                }
                num.low = -num.low;
            }
        }
        //负9位数单独处理
        else if (len == 10 && bit[0] == '-') {
            for (int i = len - 1; i >= 1; i--) {
                num.low += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 1)));
            }
            num.low = -num.low;
        }
        else {
            if (bit[0] != '-') {
                for (int i = len - 10; i >= 0; i--) {
                    num.high += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 10)));
                }
                for (int i = len - 1; i >= len - 9; i--) {
                    num.low += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 1)));
                }
            }
            
            else {
                num.high = 0;
                num.low = 0;
                for (int i = len - 10; i >= 1; i--) {
                    num.high += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 10)));
                }
                for (int i = len - 1; i >= len - 9; i--) {
                    num.low += unsigned(bit[i] - '0') * unsigned(pow(10, (len - i - 1)));
                }
                num.high = -num.high;

            }
        }
        return is;
    }

    //转化为string
    std::string tostring() {
        std::string str0 = "";
        if (high == 0) {
            return std::to_string(low);
        }
        int i = 1;
        for (; i <= 9; ++i) {
            if (low < (pow(10, i))) break;
        }
        std::string str = std::to_string(high);
        for (; i < 9; i++) {
            str0 += "0";
        }
        //补0
        
        std::string strlow = std::to_string(low);
        return str + str0 + strlow;

    }


    bool operator==(const BigInt& other) const {
        return high == other.high && low == other.low;
    }

    bool operator<(const BigInt& other) const {
        if (high < other.high) {
            return true;
        }
        else if (high > other.high) {
            return false;
        }
        else {
            return ((high < 0) && (low > other.low)) || ((high >= 0) && (low < other.low));
        }
    }

    bool operator>(const BigInt& other) const {
        if (high > other.high) {
            return true;
        }
        else if (high < other.high) {
            return false;
        }
        else {
            return ((high < 0) && (low < other.low)) || ((high >= 0) && (low > other.low));
        }
    }

    
};

