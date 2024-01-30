#include "sql_exec.cpp"

class SqlShell : public SqlExec
{
public:
    /**
     *  Run the SQL Parser
     * @param statement user input SQL statement
     */
    string run(string statement)
    {
        SQLParserResult *result = SQLParser::parseSQLString(statement);
        string query = "";
        if (result->isValid())
            query = execute(result);
        {
            cout << "Invalid SQL: " << statement << "\n";
        }
    
        delete result;
        return query;
    }

    /**
     * Run test on SQL Parser
     */
    void test()
    {
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

        cout << "Testing SQL Executor Passed!\n";
    }

private:
    void testParseSQLQuery(string query, string expected)
    {
        if (SqlShell::run(query) != expected)
        {
            cout << "TEST FAILED\n";
            cout << ">>>> " << expected << "\n";
        }
    }
};
