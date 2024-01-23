#pragma once
#include "SqlExec.h"

class SqlShell {
    static const string HOME;
    bool initialized = false;
    SqlExec sqlExec;

public:
    /** Run the SQL shell */
    void run();

    /**
     * Run the SQL shell with a specific database environment
     * @param envHome  path to environment directory
     */
    void run(string envHome);

    /**
     * For testing purposes, run the SQL shell with a vector of SQL statements
     * @param statements  vector of SQL statements
     */
    string run(string statement, string envHome);
};

class SqlShellTest  {
    SqlShell shell;
    string envHome;
public:
    SqlShellTest(string envHome);
    void testParseSQLQuery(string query, string expected);
    void run();
};