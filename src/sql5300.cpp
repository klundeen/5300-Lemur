#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sstream>
#include "db_cxx.h"

// include the sql parser
#include "SQLParser.h"

const char *HOME = "~/sql5300/data";
const char *DB_NAME = "lemur.db";
const unsigned int BLOCK_SZ = 4096;

/**
 * Initialize database with environment configuration
 * @param envdir  path to environment directory
*/
void initializeDbEnvironment(std::string envdir);

/**
 * Extract expressions and append to sql command string builder
 * @param expr  expression pointer contains sql data
 * @param ss    string builder used to append data component to sql string
*/
void printExpression(hsql::Expr* expr, std::stringstream& ss);

/**
 * Parse table reference into sql data component and append to sql string
 * @param table  table reference pointer
 * @param ss     string builder used to append data component to sql string
*/
void printTableRefInfo(hsql::TableRef* table, std::stringstream& ss);

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
std::string columnDefinitionToString(const hsql::ColumnDefinition *col);

/**
 * Convert the parser result back into equivalent SQL command string
 * Support SELECT and CREATE statment
 * @param result  parser result to unparse
 * @return        SQL equivalent to *result
*/
std::string execute(hsql::SQLParserResult* result);

/** Test driver for execute query function
 * Output failed test into standard output
 * @param query     input sql test query string
 * @param expected  expected parsed sql string
 */
void testParseSQLQuery(std::string query, std::string expected);

/** run all the specified test cases from assignment */
void runTestCases();

/** Start the sql shell loop */
void runSqlShell();

int main(int argc, char *argv[]) {
	std::string envdir = HOME;
    if ( argc >= 2 ) 
        envdir = argv[1];
    
    std::cout << "Have you created a dir: " << HOME  << "? (y/n) ";
	std::string ans;
    std::getline(std::cin, ans);
	if( ans[0] != 'y')
		return 1;
    
    try {
        initializeDbEnvironment(envdir);
    } catch (DbException e) {
        std::cout << "Provided directory not created! exiting..." << std::endl;
        return 1;
    }
    
    runTestCases();

    runSqlShell();
    
	return EXIT_SUCCESS;
}


void initializeDbEnvironment(std::string envdir) {
	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

	Db db(&env, 0);
	db.set_message_stream(env.get_message_stream());
	db.set_error_stream(env.get_error_stream());
	db.set_re_len(BLOCK_SZ);
	db.open(NULL, DB_NAME, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);
    printf("(sql5300: running with database environment at %s)\n", envdir.c_str());
}

void printExpression(hsql::Expr* expr, std::stringstream& ss) {
    switch (expr->type) {
    case hsql::kExprStar:
        ss << "*";
        break;
    case hsql::kExprColumnRef:
        if (expr->hasTable()) {
            ss << expr->table << ".";
        }
        ss << expr->name;
        break;
    case hsql::kExprLiteralInt:
        ss << expr->ival;
        break;
    case hsql::kExprLiteralString:
        ss << expr->name;
        break;
    case hsql::kExprOperator:
        printExpression(expr->expr, ss);
        switch (expr->opType) {
            case hsql::Expr::SIMPLE_OP:
                ss << " " << expr->opChar << " ";
                break;
            default:
                ss << expr->opType;
                break;
        }
        if (expr->expr2 != NULL) printExpression(expr->expr2, ss);
        break;
    default:
        fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
        return;
    }
}

void printTableRefInfo(hsql::TableRef* table, std::stringstream& ss) {
    switch (table->type) {
        case hsql::kTableName:
            ss << table->name;
            break;
        case hsql::kTableJoin:
            printTableRefInfo(table->join->left, ss);
            if (table->join->type == hsql::kJoinLeft) {
                ss << " LEFT";
            }
            ss << " JOIN ";
            printTableRefInfo(table->join->right, ss);
            ss << " ON ";
            printExpression(table->join->condition, ss);
            break;
        case hsql::kTableCrossProduct:
            int count = table->list->size();
            for (hsql::TableRef* tbl : *table->list) {
                printTableRefInfo(tbl, ss);
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

std::string columnDefinitionToString(const hsql::ColumnDefinition *col) {
    std::string ret(col->name);
    switch(col->type) {
    case hsql::ColumnDefinition::DOUBLE:
        ret += " DOUBLE";
        break;
    case hsql::ColumnDefinition::INT:
        ret += " INT";
        break;
    case hsql::ColumnDefinition::TEXT:
        ret += " TEXT";
        break;
    default:
        ret += " ...";
        break;
    }
    return ret;
}

std::string execute(hsql::SQLParserResult* result) {
    std::stringstream ss;
    const hsql::SQLStatement * stmt = result->getStatement(0);

    if (stmt->type() == hsql::kStmtSelect) { 
        ss << "SELECT ";
        int count = ((hsql::SelectStatement*)stmt)->selectList->size();
        for (hsql::Expr* expr : *((hsql::SelectStatement*)stmt)->selectList) {
            printExpression(expr, ss);
            if (--count) {
                ss << ",";
            }
            ss << " ";
        }
        
        ss << "FROM ";

        printTableRefInfo(((hsql::SelectStatement*)stmt)->fromTable, ss);

        if (((hsql::SelectStatement*)stmt)->whereClause != NULL) {
            ss << " WHERE ";
            printExpression(((hsql::SelectStatement*)stmt)->whereClause, ss);
        }
    }

    if (stmt->type() == hsql::kStmtCreate) { 
        ss << "CREATE TABLE " <<  ((hsql::CreateStatement*)stmt)->tableName;

       ss << " (";
        int count = ((hsql::CreateStatement*)stmt)->columns->size();
        for (auto col : *((hsql::CreateStatement*)stmt)->columns) {
           ss << columnDefinitionToString(col);
            if (--count) {
               ss << ", ";
            }
        }
       ss << ")";
    }

    return ss.str();
}

void testParseSQLQuery(std::string query, std::string expected) {
    hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(query);

    if (result->isValid() && execute(result) != expected) {
        std::cout << "TEST FAILED\n";
        std::cout << ">>>> " << expected << "\n"; 
    }
    delete result;
}

void runTestCases() {
    std::string query;
    std::string expected;
    
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

void runSqlShell() {
    hsql::SQLParserResult* result;
    std::string query;
    while (query != "quit") {
        std::cout << "SQL> ";
        std::getline(std::cin, query);
        
        result = hsql::SQLParser::parseSQLString(query);
        
        if (result->isValid()) {
            std::cout << execute(result) << std::endl;
        } else if (query != "quit") {
            std::cout << "Invalid SQL: " << query << std::endl;
        }

        delete result;
    }
}