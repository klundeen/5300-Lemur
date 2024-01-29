/**
 * @file sql5300.cpp - main entry for the relation manaager's SQL shell
 * @author Kevin Lundeen
 * @see "Seattle University, cpsc4300/5300, Spring 2021"
 */
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "heap_storage.h"

using namespace std;
using namespace hsql;

/*
 * we allocate and initialize the _DB_ENV global
 */
DbEnv *_DB_ENV;

/**
 * Execute an SQL statement (but for now, just spit back the SQL)
 * @param stmt  Hyrise AST for the statement
 * @returns     a string (for now) of the SQL statment
 */
string execute(const SQLStatement *stmt) {
    switch (stmt->type()) {
        case kStmtSelect:
            return "SELECT...";  // FIXME
        case kStmtInsert:
            return "INSERT...";  // FIXME
        case kStmtCreate:
			return "CREATE...";  // FIXME
        default:
            return "Not implemented";
    }
}

/**
 * Main entry point of the sql5300 program
 * @args dbenvpath  the path to the BerkeleyDB database environment
 */
int main(int argc, char *argv[]) {

    // Open/create the db enviroment
    if (argc != 2) {
        cerr << "Usage: cpsc5300: dbenvpath" << endl;
        return 1;
    }
    char *envHome = argv[1];
    cout << "(sql5300: running with database environment at " << envHome << ")" << endl;
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    try {
        env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        cerr << "(sql5300: " << exc.what() << ")";
        exit(1);
    }
    _DB_ENV = &env;
    
    // Enter the SQL shell loop
    while (true) {
        cout << "SQL> ";
        string query;
        getline(cin, query);
        if (query.length() == 0)
            continue;  // blank line -- just skip
        if (query == "quit")
            break;  // only way to get out
        if (query == "test") {
            cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            continue;
        }

        // use the Hyrise sql parser to get us our AST
        SQLParserResult *result = SQLParser::parseSQLString(query);
        if (!result->isValid()) {
            cout << "invalid SQL: " << query << endl;
	    	delete result;
            continue;
        }

        // execute the statement
        for (uint i = 0; i < result->size(); ++i) {
            cout << execute(result->getStatement(i)) << endl;
        }
		delete result;
    }
    return EXIT_SUCCESS;
}
