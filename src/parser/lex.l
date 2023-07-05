    /* keywords are case insensitive */
%option caseless
    /* we don't need yywrap() function */
%option noyywrap
    /* we don't need yyunput() function */
%option nounput
    /* we don't need input() function */
%option noinput
    /* enable location */
%option bison-bridge
%option bison-locations

%{
#include "ast.h"
#include "yacc.tab.h"
#include <iostream>

//rz dev
#include <cstring>
#include "errors.h"
// automatically update location
#define YY_USER_ACTION \
    yylloc->first_line = yylloc->last_line; \
    yylloc->first_column = yylloc->last_column; \
    for (int i = 0; yytext[i] != '\0'; i++) { \
        if(yytext[i] == '\n') { \
            yylloc->last_line++; \
            yylloc->last_column = 1; \
        } else { \
            yylloc->last_column++; \
        } \
    }

%}

alpha [a-zA-Z]
digit [0-9]
white_space [ \t]+
new_line "\r"|"\n"|"\r\n"
sign "+"|"-"
identifier {alpha}(_|{alpha}|{digit})*
value_int {sign}?{digit}+
value_float {sign}?{digit}+\.({digit}+)?
value_string '[^']*'
single_op ";"|"("|")"|","|"*"|"="|">"|"<"|"."

value_bigint {sign}?{digit}+
value_datetime  '{digit}+\-{digit}+\-{digit}+\ {digit}+\:{digit}+\:{digit}\+'

%x STATE_COMMENT

%%
    /* block comment */
"/*" { BEGIN(STATE_COMMENT); }
<STATE_COMMENT>"*/" { BEGIN(INITIAL); }
<STATE_COMMENT>[^*] { /* ignore the text of the comment */ }
<STATE_COMMENT>\* { /* ignore *'s that aren't part of */ }
    /* single line comment */
"--".* { /* ignore single line comment */ }
    /* white space and new line */
{white_space} { /* ignore white space */ }
{new_line} { /* ignore new line */ }
    /* keywords */
"SHOW" { return SHOW; }
"BEGIN" { return TXN_BEGIN; }
"COMMIT" { return TXN_COMMIT; }
"ABORT" { return TXN_ABORT; }
"ROLLBACK" { return TXN_ROLLBACK; }
"TABLES" { return TABLES; }
"CREATE" { return CREATE; }
"TABLE" { return TABLE; }
"DROP" { return DROP; }
"DESC" { return DESC; }
"INSERT" { return INSERT; }
"INTO" { return INTO; }
"VALUES" { return VALUES; }
"DELETE" { return DELETE; }
"FROM" { return FROM; }
"WHERE" { return WHERE; }
"UPDATE" { return UPDATE; }
"SET" { return SET; }
"SELECT" { return SELECT; }
"INT" { return INT; }
"CHAR" { return CHAR; }
"FLOAT" { return FLOAT; }
"INDEX" { return INDEX; }
"AND" { return AND; }
"JOIN" {return JOIN;}
"EXIT" { return EXIT; }
"HELP" { return HELP; }
"ORDER" { return ORDER; }
"BY" {  return BY;  }
"ASC" { return ASC; }
"LIMIT" { return LIMIT; }

"BIGINT" { return BIGINT;}
"DATETIME" { return DATETIME;}
    /* operators */
">=" { return GEQ; }
"<=" { return LEQ; }
"<>" { return NEQ; }
{single_op} { return yytext[0]; }
    /* id */
{identifier} {
    yylval->sv_str = yytext;
    return IDENTIFIER;
}
    /* literals */
{value_datetime} {
    DateTime *datetime = new DateTime(yytext);
    datetime->flag = datetime->isLegal();
    yylval->sv_datetime = *datetime;
    return VALUE_DATETIME;
}
    /* rz-dev */
{value_int} {       // by 星穹铁道高手
    std::string str_value(yytext);  // 将输入字符转为string
    int int_value = atoi(yytext);   // 将输入字符转为int
    long long bigint_value = std::atoll(yytext);   // 将输入字符转为long long

    if (std::to_string(int_value) == str_value){    // 转换前后字符相等
        yylval->sv_int = atoi(yytext);
        return VALUE_INT;
    }
    else{
        BigInt *bigint = new BigInt(yytext);
        if (std::to_string(bigint->value) != str_value){
            bigint->flag = true;
        }
        yylval->sv_bigint = *bigint;
        return VALUE_BIGINT;
    }
}
{value_float} {
    yylval->sv_float = atof(yytext);
    return VALUE_FLOAT;
}
{value_string} {
    char char1 = '-';
    char char2 = ':';
    char char3 = ' ';
    std::string str = std::string(yytext + 1, strlen(yytext) - 2);
    int count1 = 0;
    int count2 = 0;
    int count3 = 0;

    for (char c : str) {
        if (c == char1){ count1++; }
        else if (c == char2) { count2++; }
        else if (c == char3) { count3++; }
    }

    if (count1 == 2 && count2 == 2 && count3 == 1){
        DateTime *datetime = new DateTime(str);
        datetime->flag = datetime->isLegal();
        yylval->sv_datetime = *datetime;
        return VALUE_DATETIME;
    }

    yylval->sv_str = str;
    return VALUE_STRING;
}
    /* rz-dev */
{value_bigint} {
    yylval->sv_bigint = *(new BigInt(yytext));
    return VALUE_BIGINT;
}
    /* EOF */
<<EOF>> { return T_EOF; }
    /* unexpected char */
. { std::cerr << "Lexer Error: unexpected character " << yytext[0] << std::endl; }
%%

