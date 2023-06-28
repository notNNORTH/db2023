/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)    // parse:语法树
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse))
    {
        // 处理表名
        query->tables = std::move(x->tabs);
        /** TODO: 检查表是否存在 */
        int table_size = query->tables.size();
        for (int i = 0; i < table_size; i++){
            auto tab = query->tables[i];
            if (!sm_manager_->db_.is_table(tab)){
                throw TableNotFoundError(tab);
            }
        }

        // 插入属性
        // 处理target list，再target list中添加上表名，例如 a.id
        int col_size = x->cols.size();
        for (int i = 0; i < col_size; i++){
            auto sv_sel_col = x->cols[i];
            TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
            query->cols.push_back(sel_col);
        }
        
        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);      // 获取（对应表名）的所有关系的元组
        if (query->cols.empty()) {      // 对应语句: SELECT *
            // select all columns
            for (auto &col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                query->cols.push_back(sel_col);
            }
        } else {
            // infer table name from column name
            for (auto &sel_col : query->cols) {
                sel_col = check_column(all_cols, sel_col);  // 列元数据校验
            }
        }
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);

    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        /** TODO: */
        // 处理表名
        query->tables.push_back(x->tab_name);
        auto tab = query->tables.back();
        if (!sm_manager_->db_.is_table(tab)){
            throw TableNotFoundError(tab);
        }

        // 处理要更新的列及对应的值
        int set_clauses_size = x->set_clauses.size();
        for (int i = 0; i < set_clauses_size; i++){
            auto sv_set_clause = x->set_clauses[i];
            SetClause set_clause;
            set_clause.lhs.tab_name = x->tab_name;
            set_clause.lhs.col_name = sv_set_clause->col_name;
            set_clause.rhs = convert_sv_value(sv_set_clause->val);
            query->set_clauses.push_back(set_clause);
        }

        // 处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);

    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        // 处理表名
        query->tables.push_back(x->tab_name);
        auto tab = query->tables.back();
        if (!sm_manager_->db_.is_table(tab)){
            throw TableNotFoundError(tab);
        }

        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);        
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        // 处理表名
        query->tables.push_back(x->tab_name);
        auto tab = query->tables.back();
        if (!sm_manager_->db_.is_table(tab)){
            throw TableNotFoundError(tab);
        }

        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }

    } else {
        // do nothing
    }
    query->parse = std::move(parse);
    return query;
}

// 验证属性是否存在(
TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        /** TODO: Make sure target column exists */
        bool columnExists = false;
        for (const auto &col : all_cols) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                columnExists = true;
                break;
            }
        }
        if (!columnExists) {
            throw ColumnNotFoundError(target.col_name);
        }
    }
    return target;
}

// 用于获取给定表名列表中所有列的元数据信息，并将其存储在 all_cols 参数中。
void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

// 将传入的一组条件表达式（sv_conds）转换为内部表示的条件对象（Query.conds）
void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
        }
        conds.push_back(cond);
    }
}

// 检查条件集合（conds）中的条件是否合法和兼容
void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        if ((lhs_type == TYPE_INT && rhs_type == TYPE_FLOAT) || (lhs_type == TYPE_FLOAT && rhs_type == TYPE_INT)){
            continue;
        }
        if (lhs_type != rhs_type) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}

// 将抽象语法树（AST）中的值转换为查询引擎内部使用的值类型
Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else if (auto bigint_lit = std::dynamic_pointer_cast<ast::BigIntLit>(sv_val)) {
        val.set_bigint(bigint_lit->val);
    }else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

// 将抽象语法树（AST）中的操作符转换为查询引擎内部使用的操作符类型
CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}
