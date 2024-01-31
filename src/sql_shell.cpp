#include "sql_shell.h"

#include <stdio.h>

#include <iostream>
#include <sstream>
#include <string>

using namespace std;
using namespace hsql;

bool SqlShell::initialized = false;

void SqlShell::initializeDbEnv(const char *envHome, DbEnv *env) {
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
    this->initialized = true;
    printf("(sql5300: running with database environment at %s)\n", envHome);
}

void SqlShell::run() {
    while (true) {
        printf("SQL> ");
        string query;
        getline(cin, query);
        if (query.length() == 0) continue;
        if (query == "quit") break;
        if (query == "test") {
            printf("test_sql_parser: ");
            this->testSQLParser();
            printf("test_heap_storage: %s\n",
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

string SqlShell::execute(const hsql::SQLStatement *stmt) {
    stringstream ss;
    if (stmt->type() == kStmtSelect) {
        ss << "SELECT ";
        int count = ((SelectStatement *)stmt)->selectList->size();
        for (Expr *expr : *((SelectStatement *)stmt)->selectList) {
            this->printExpression(expr, ss);
            if (--count) {
                ss << ",";
            }
            ss << " ";
        }

        ss << "FROM ";

        this->printTableRefInfo(((SelectStatement *)stmt)->fromTable, ss);

        if (((SelectStatement *)stmt)->whereClause != NULL) {
            ss << " WHERE ";
            this->printExpression(((SelectStatement *)stmt)->whereClause, ss);
        }
    }

    if (stmt->type() == kStmtCreate) {
        ss << "CREATE TABLE " << ((CreateStatement *)stmt)->tableName;

        ss << " (";
        int count = ((CreateStatement *)stmt)->columns->size();
        for (auto col : *((CreateStatement *)stmt)->columns) {
            ss << this->columnDefinitionToString(col);
            if (--count) {
                ss << ", ";
            }
        }
        ss << ")";
    }

    return ss.str();
}

void SqlShell::testSQLParser() {
    printf("Testing SQL Parser\n");
    string query;
    string expected;

    query = "select * from foo left join goober on foo.x=goober.x";
    expected = "SELECT * FROM foo LEFT JOIN goober ON foo.x = goober.x";
    this->testParseSQLQuery(query, expected);

    query = "select * from foo as f left join goober on f.x = goober.x";
    expected = "SELECT * FROM foo AS f LEFT JOIN goober ON f.x = goober.x";
    this->testParseSQLQuery(query, expected);

    query = "select * from foo as f left join goober as g on f.x = g.x";
    expected = "SELECT * FROM foo AS f LEFT JOIN goober AS g ON f.x = g.x";
    this->testParseSQLQuery(query, expected);

    query = "select a,b,g.c from foo as f, goo as g";
    expected = "SELECT a, b, g.c FROM goo AS g, foo AS f";
    this->testParseSQLQuery(query, expected);

    query = "select a,b,c from foo where foo.b > foo.c + 6";
    expected = "SELECT a, b, c FROM foo WHERE foo.b > foo.c + 6";
    this->testParseSQLQuery(query, expected);

    query =
        "select f.a,g.b,h.c from foo as f join goober as g on f.id = g.id "
        "where f.z >1";
    expected =
        "SELECT f.a, g.b, h.c FROM foo AS f JOIN goober AS g ON f.id = "
        "g.id WHERE f.z > 1";
    this->testParseSQLQuery(query, expected);

    query = "create table foo (a text, b integer, c double)";
    expected = "CREATE TABLE foo (a TEXT, b INT, c DOUBLE)";
    this->testParseSQLQuery(query, expected);

    query = "foo bar blaz";
    expected = "";
    this->testParseSQLQuery(query, expected);

    printf("Testing SQL Executor Passed!\n");
}

void SqlShell::printExpression(Expr *expr, stringstream &ss) {
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

void SqlShell::printTableRefInfo(TableRef *table, stringstream &ss) {
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

string SqlShell::columnDefinitionToString(const ColumnDefinition *col) {
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

void SqlShell::testParseSQLQuery(string query, string expected) {
    SQLParserResult *result = SQLParser::parseSQLString(query);
    if (!result->isValid()) {
        printf("SQL> %s\n", query.c_str());
        printf("invalid SQL: %s\n", query.c_str());
    } else {
        for (uint i = 0; i < result->size(); ++i) {
            const SQLStatement *stmt = result->getStatement(i);
            string query = execute(stmt);
            printf("SQL> %s\n", query.c_str());
            printf(">>>> %s\n", expected.c_str());
            if (query != expected) printf("TEST FAILED\n");
        }
    }
    delete result;
}
