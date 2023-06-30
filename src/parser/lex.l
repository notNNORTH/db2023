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

"BIGINT" { return BIGINT;}
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
    /* rz-dev */
{value_int} {
    char *p = yytext; 
    //判断是否为int或bigint
    int l = std::strlen(yytext);
    
    if(p[0]== '-'){
        if(l <= 10) {
           yylval->sv_int = atoi(yytext);
            return VALUE_INT; 
        }
        else if(l == 11){
            std::string valuestr(p + 1);
            if(valuestr <= "2147483648"){           
                yylval->sv_int = atoi(yytext);
                return VALUE_INT;
            }
        }
    }else{
        if(l <= 9) {
           yylval->sv_int = atoi(yytext);
            return VALUE_INT; 
        }else if(l == 10){
            std::string valuestr(p);
            if(valuestr <= "2147483647"){
                yylval->sv_int = atoi(yytext);
                return VALUE_INT;
            }
        }
    }

    BigInt *BI = new BigInt(yytext);
    if(p[0]== '-'){
        if(l >= 20){
            BI -> flag=1;
        }
        else if(l == 19){        
            std::string valuestr(p + 1);
            if(valuestr > "9223372036854775808"){           
                BI -> flag=1;
            }
        }
    }else{
        if(l >= 19){
            BI -> flag=1;
        }
        else if(l == 18){
            std::string valuestr(p);
            if(valuestr > "9223372036854775807"){
                BI -> flag=1;
            }
        }
    }
    
    yylval->sv_bigint = *(BI);
    return VALUE_BIGINT;
    
}
{value_float} {
    yylval->sv_float = atof(yytext);
    return VALUE_FLOAT;
}
{value_string} {
    yylval->sv_str = std::string(yytext + 1, strlen(yytext) - 2);
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
