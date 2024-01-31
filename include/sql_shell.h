
#pragma once
#include "SQLParser.h"
#include "heap_storage.h"

/**
 * SQL Shell to accept user input and execute SQL commands
 * Support SELECT and CREATE statment
 */
class SqlShell {
   public:
    /**
     * Initialize the database environment with the given home directory
     * @param envHome  the home directory of the database
     */
    void initializeDbEnv(const char *envHome);

    /**
     *  Run SQL Shell to accept user input and execute SQL commands
     */
    void run();

    /**
     * Run automatic test on SQL Parser
     */
    void testSQLParser();

    /**
     * Convert the parser result back into equivalent SQL command string
     * Support SELECT and CREATE statment
     * @param result  parser result to unparse
     * @return        SQL equivalent to *result
     */
    std::string execute(const hsql::SQLStatement *stmt);

   private:
    void printExpression(hsql::Expr *expr, std::stringstream &ss);

    void printTableRefInfo(hsql::TableRef *table, std::stringstream &ss);

    std::string columnDefinitionToString(const hsql::ColumnDefinition *col);

    void testParseSQLQuery(std::string query, std::string expected);
};
