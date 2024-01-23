#include "SQLParser.h"

using namespace std;
using namespace hsql;

class SqlExec
{
    const string DB_NAME = "lemur.db";
    const unsigned BLOCK_SZ = 4096;

public:
    /**
     * Initialize database with environment configuration
     * @param envdir  path to environment directory
     */
    void initializeDbEnv(string envdir);

    /**
     * Convert the parser result back into equivalent SQL command string
     * Support SELECT and CREATE statment
     * @param result  parser result to unparse
     * @return        SQL equivalent to *result
     */
    string execute(SQLParserResult *result);

private:
    /**
     * Extract expressions and append to sql command string builder
     * @param expr  expression pointer contains sql data
     * @param ss    string builder used to append data component to sql string
     */
    void printExpression(Expr *expr, stringstream &ss);

    /**
     * Parse table reference into sql data component and append to sql string
     * @param table  table reference pointer
     * @param ss     string builder used to append data component to sql string
     */
    void printTableRefInfo(TableRef *table, stringstream &ss);

    /**
     * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
     * @param col  column definition to unparse
     * @return     SQL equivalent to *col
     */
    string columnDefinitionToString(const ColumnDefinition *col);
};