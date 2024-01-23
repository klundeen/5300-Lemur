#include "SqlShell.h"
#include <stdio.h>
#include <string>
#include <iostream>

#include "SQLParser.h"

using namespace std;
using namespace hsql;

const string SqlShell::HOME = "~/sql5300/data";

void SqlShell::run()
{
    if (!initialized) {
        run(HOME);
    }
    
    SQLParserResult* result;
    string query;
    while (query != "quit") {
        cout << "SQL> ";
        getline(cin, query);
        result = SQLParser::parseSQLString(query);
        
        if (result->isValid()) {
            cout << sqlExec.execute(result) << endl;
        } else if (query != "quit") {
            cout << "Invalid SQL: " << query << endl;
        }

        delete result;
    }
}

void SqlShell::run(string envHome)
{
    if (!initialized) {
        if (!envHome.empty()) 
            sqlExec.initializeDbEnv(envHome);
        else
            sqlExec.initializeDbEnv(HOME);
        initialized = true;
    }
    run();
}

string SqlShell::run(string statement, string envHome)
{
    if (!initialized) {
        sqlExec.initializeDbEnv(envHome);
        initialized = true;
    }
    SQLParserResult* result = SQLParser::parseSQLString(statement);
    string query = "";
    if (result->isValid()) {
        query = sqlExec.execute(result);
    }
    delete result;
    return query;
}


    

SqlShellTest::SqlShellTest(string envHome) {
    this->envHome = envHome;
}

void SqlShellTest::testParseSQLQuery(string query, string expected) {
    if (shell.run(query, envHome) != expected) {
        cout << "TEST FAILED\n";
        cout << ">>>> " << expected << "\n"; 
    }
}

void SqlShellTest::run() {
    cout << "Testing SQL Parser\n";
    string query;
    string expected;
    
    query = "select * from foo left join goober on foo.x=goober.x";
    expected = "SELECT * FROM foo LEFT JOIN goober ON foo.x = goober.x";
    testParseSQLQuery(query, expected);

    query = "select * from foo as f left join goober on f.x = goober.x";
    expected = "SELECT * FROM foo AS f LEFT JOIN goober ON f.x = goober.x";
    testParseSQLQuery(query, expected);

    query = "select * from foo as f left join goober as g on f.x = g.x";
    expected = "SELECT * FROM foo AS f LEFT JOIN goober AS g ON f.x = g.x";
    testParseSQLQuery(query, expected);

    query = "select a,b,g.c from foo as f, goo as g";
    expected = "SELECT a, b, g.c FROM goo AS g, foo AS f";
    testParseSQLQuery(query, expected);

    query = "select a,b,c from foo where foo.b > foo.c + 6";
    expected = "SELECT a, b, c FROM foo WHERE foo.b > foo.c + 6";
    testParseSQLQuery(query, expected);

    query = "select f.a,g.b,h.c from foo as f join goober as g on f.id = g.id where f.z >1";
    expected = "SELECT f.a, g.b, h.c FROM foo AS f JOIN goober AS g ON f.id = g.id WHERE f.z > 1";
    testParseSQLQuery(query, expected);

    query = "create table foo (a text, b integer, c double)";
    expected = "CREATE TABLE foo (a TEXT, b INT, c DOUBLE)";
    testParseSQLQuery(query, expected);

    query = "foo bar blaz";
    expected = "";
    testParseSQLQuery(query, expected);
}
