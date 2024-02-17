/**
 * @file sql_shell.cpp - Implementation of SQL Shell to initialize database
 * enviroment, accept user input and execute SQL commands
 * @author Duc Vo
 *
 * @see "Seattle University, CPSC 5300, Winter Quarter 2024"
 */
#include "sql_shell.h"
#include "sql_exec.h"

#include <stdio.h>

#include <iostream>
#include <sstream>
#include <string>
#define DEBUG_ENABLED
#include "debug.h"

using namespace std;
using namespace hsql;

bool SQLShell::initialized = false;

void SQLShell::initializeEnv(const char *envHome, DbEnv *env) {
    if (this->initialized) {
        cerr << "(sql5300: database environment already initialized) \n";
        return;
    };
    env->set_message_stream(&cout);
    env->set_error_stream(&cerr);
    try {
        env->open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }
    _DB_ENV = env;
    DEBUG_OUT("before init\n");
    initialize_schema_tables();
    DEBUG_OUT("after init\n");
    this->initialized = true;
    printf("(sql5300: running with database environment at %s)\n", envHome);
}

void SQLShell::run() {
    while (true) {
        printf("SQL> ");
        string query;
        getline(cin, query);
        if (query.length() == 0) continue;
        if (query == "quit") break;
        if (query == "test") {
            printf("\nTESTING SQL PARSER...\n");
            this->testSQLParser();
            printf("\nTESTING HEAP STORAGE CLASSES\n");
            printf("\nHEAP STORAGE TEST: %s\n",
                   (test_heap_storage() ? "OK" : "FAILED"));
            continue;
        }

        SQLParserResult *result = SQLParser::parseSQLString(query);

        if (result->isValid()) {
            for (uint i = 0; i < result->size(); ++i)
                printf("%s\n", this->execute(result->getStatement(i)).c_str());
        } else {
            printf("Invalid SQL: %s\n", query.c_str());
        }
        delete result;
    }
}

string SQLShell::execute(const SQLStatement *stmt) {
    // outsource execute to SQLExec
    // could refactor this so that we don't even need to have SQLShell::Exec at all
    SQLExec *sql_exec = new SQLExec();
    QueryResult *result = sql_exec->execute(stmt);
    return result->get_message();
}

//
void SQLShell::printExpression(Expr *expr, stringstream &ss) {
    switch (expr->type) {
        case kExprStar:
            ss << "*";
            break;
        case kExprColumnRef:
            if (expr->hasTable()) {
                ss << expr->table << ".";
            }
            ss << expr->name;
            break;
        case kExprLiteralInt:
            ss << expr->ival;
            break;
        case kExprLiteralString:
            ss << expr->name;
            break;
        case kExprOperator:
            this->printExpression(expr->expr, ss);
            switch (expr->opType) {
                case Expr::SIMPLE_OP:
                    ss << " " << expr->opChar << " ";
                    break;
                default:
                    ss << expr->opType;
                    break;
            }
            if (expr->expr2 != NULL) this->printExpression(expr->expr2, ss);
            break;
        default:
            fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
            return;
    }
}

void SQLShell::printTableRefInfo(TableRef *table, stringstream &ss) {
    switch (table->type) {
        case kTableSelect:
            break;
        case kTableName:
            ss << table->name;
            break;
        case kTableJoin:
            this->printTableRefInfo(table->join->left, ss);
            if (table->join->type == kJoinLeft) {
                ss << " LEFT";
            }
            ss << " JOIN ";
            this->printTableRefInfo(table->join->right, ss);
            ss << " ON ";
            printExpression(table->join->condition, ss);
            break;
        case kTableCrossProduct:
            int count = table->list->size();
            for (TableRef *tbl : *table->list) {
                this->printTableRefInfo(tbl, ss);
                if (--count) {
                    ss << ", ";
                }
            }
            break;
    }
    if (table->alias != NULL) {
        ss << " AS " << table->alias;
    }
}

// remove?
string SQLShell::columnDefinitionToString(const ColumnDefinition *col) {
    string ret(col->name);
    switch (col->type) {
        case ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case ColumnDefinition::INT:
            ret += " INT";
            break;
        case ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " ...";
            break;
    }
    return ret;
}

void SQLShell::testParseSQLQuery(string query, string expected) {
    SQLParserResult *result = SQLParser::parseSQLString(query);
    if (!result->isValid()) {
        printf("SQL> %s\n", query.c_str());
        printf("invalid SQL: %s\n", query.c_str());
    } else {
        for (uint i = 0; i < result->size(); ++i) {
            const SQLStatement *stmt = result->getStatement(i);
            string res = execute(stmt);
            printf("SQL> %s\n", query.c_str());
            printf(">>>> %s\n", res.c_str());
            if (res != expected) printf("TEST FAILED\n");
        }
    }
    delete result;
}

// Tests are broken for now
void SQLShell::testSQLParser() {
    string query;
    string expected;

    // query = "select * from foo left join goober on foo.x=goober.x";
    // expected = "SELECT * FROM foo LEFT JOIN goober ON foo.x = goober.x";
    // this->testParseSQLQuery(query, expected);

    // query = "foo bar blaz";
    // expected = "";
    // this->testParseSQLQuery(query, expected);

    printf("Testing SQLShell needs to be implemented with new tests!\n");
}