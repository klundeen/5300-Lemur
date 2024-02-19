/**
 * @file sql_shell.cpp - Implementation of SQL Shell to initialize database
 * enviroment, accept user input and execute SQL commands
 * @author Duc Vo
 *
 * @see "Seattle University, CPSC 5300, Winter Quarter 2024"
 */
#include "sql_shell.h"
#include "sql_exec.h"
#include "parse_tree_to_string.h"

#include <stdio.h>

#include <iostream>
#include <sstream>
#include <string>
// #define DEBUG_ENABLED
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
    initialize_schema_tables();
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
            printf("\nTESTING HEAP STORAGE CLASSES\n");
            printf("\nHEAP STORAGE TEST: %s\n",
                   (test_heap_storage() ? "OK" : "FAILED"));
            continue;
        }

        SQLParserResult *parser_result = SQLParser::parseSQLString(query);
        SQLExec *sql_exec = new SQLExec();
        parse_tree_to_string *pt_to_s = new parse_tree_to_string();

        if (parser_result->isValid()) {
            for (uint i = 0; i < parser_result->size(); ++i)
            {
                // First, echo the command entered by the user
                std::cout << pt_to_s->statement(parser_result->getStatement(i)) << std::endl;

                try {
                    // Now, execute it using SQLExec
                    QueryResult *query_result = sql_exec->execute(parser_result->getStatement(i));
                    std::cout << *query_result;
                    delete query_result;
                } catch (SQLExecError &e) {
                    cout << "Error: " << e.what() << endl;
                }
            }
        } else {
            printf("Invalid SQL: %s\n", query.c_str());
        }
        delete parser_result;
        delete sql_exec;
    }
}
