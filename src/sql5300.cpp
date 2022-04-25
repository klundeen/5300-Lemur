/* sql5300.cpp
 * Diego Hoyos, Erika Skornia-Olsen
 * SeattleU CPSC4300, Spring 2021
 * Sprint Verano: Milestone 1
 * Beginning implementation of database focusing on setting up environment
 * and parsing SQL queries
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "heap_storage.h"

using namespace std;
using namespace hsql;

string columnDefToString(const ColumnDefinition* col);
string execute(const SQLStatement* stmt);
string executeCreate(const CreateStatement* stmt);
string executeInsert(const InsertStatement* stmt);
string executeSelect(const SelectStatement* stmt);
string expressionToString(const Expr* expr);
string operatorToString(const Expr* expr);
string tableRefToString(const TableRef* table);
string toLower(string str);

// Converts column inputs into acceptable SQL data types
string columnDefToString(const ColumnDefinition* col)
{
  string query = col->name;
  switch (col->type)
  {
    case ColumnDefinition::DOUBLE:
      query += " DOUBLE";
      break;
    case ColumnDefinition::INT:
      query += " INT";
      break;
    case ColumnDefinition::TEXT:
      query += " TEXT";
      break;
    default:
      query += " ...";
      break;
  }
  return query;
}

// Passes SQL queries to SQL functions
string execute(const SQLStatement* stmt)
{
  switch (stmt->type())
  {
    case kStmtCreate:
      return executeCreate((const CreateStatement*) stmt);
    case kStmtInsert:
      return executeInsert((const InsertStatement*) stmt);
    case kStmtSelect:
      return executeSelect((const SelectStatement*) stmt);
    default:
      return "Not implemented";
  }
}

// Executes SQL create statement, eventually
string executeCreate(const CreateStatement* stmt)
{
  string query = "CREATE TABLE ";
  if (stmt->type != CreateStatement::kTable)
    return query + "...";
  if (stmt->ifNotExists)
    query += "IF NOT EXISTS ";
  query += string(stmt->tableName) + " (";
  bool comma = false;
  for (ColumnDefinition* col: *stmt->columns)
  {
    if (comma)
      query += ", ";
    query += columnDefToString(col);
    comma = true;
  }
  query += ")";
  return query;
}

// Executes SQL insert statement, eventually
string executeInsert(const InsertStatement* stmt)
{
  string query = "INSERT INTO ";
  if (stmt->tableName == NULL)
    return query + "...";
  query += string(stmt->tableName) + " (";
  bool comma = false;
  if (stmt->columns == NULL)
    return query + "...";
  for (char* col_name: *stmt->columns)
  {
    if (comma)
      query += ", ";
    query += string(col_name);
    comma = true;
  }
  
  query += ") VALUES (";
  comma = false;
  switch (stmt->type)
    {
    case InsertStatement::kInsertValues:
      for (Expr* expr: *stmt->values)
      {
        if (comma)
          query += ", ";
        query += expressionToString(expr);
        comma = true;
      }
      query += ")";
      break;
    case InsertStatement::kInsertSelect:
      query += executeSelect((SelectStatement*)stmt->type);
    }
  return query;
}
// Executes SQL select statement, eventually
string executeSelect(const SelectStatement* stmt)
{
  string query = "SELECT ";
  bool comma = false;
  for (Expr* expr: *stmt->selectList)
  {
    if (comma)
      query += ", ";
    query += expressionToString(expr);
    comma = true;
  }
  query += " FROM " + tableRefToString(stmt->fromTable);
  if (stmt->whereClause != NULL)
    query += " WHERE " + expressionToString(stmt->whereClause);
  return query;
}

// Converts literals and operators into acceptable SQL
string expressionToString(const Expr* expr)
{
  string query;
  switch (expr->type)
  {
    case kExprStar:
      query += "*";
      break;
    case kExprColumnRef:
      if (expr->table != NULL)
        query += string(expr->table) + ".";
    case kExprLiteralString:
      query += expr->name;
      break;
    case kExprLiteralFloat:
      query += to_string(expr->fval);
      break;
    case kExprLiteralInt:
      query += to_string(expr->ival);
      break;
    case kExprFunctionRef:
      query += string(expr->name) + "?" + expr->expr->name;
      break;
    case kExprOperator:
      query += operatorToString(expr);
      break;
    default:
      query += "???";
      break;
  }
  if (expr->alias != NULL)
    query += string(" AS ") + expr->alias;
  return query;
}

// Converts operators into acceptable SQL
string operatorToString(const Expr* expr)
{
  if (expr == NULL)
    return "null";
  string query;
  if (expr->opType == Expr::NOT)
    query += "NOT ";
  query += expressionToString(expr->expr) + " ";
  switch (expr->opType)
  {
    case Expr::SIMPLE_OP:
      query += expr->opChar;
      break;
    case Expr::AND:
      query += "AND";
      break;
    case Expr::OR:
      query += "OR";
      break;
    default:
      break;
  }
  if (expr->expr2 != NULL)
    query += " " + expressionToString(expr->expr2);
  return query;
}

// Converts table name, aliases, joins into acceptable SQL
string tableRefToString(const TableRef* table)
{
  string query;
  switch (table->type)
  {
    case kTableSelect:
      query += executeSelect(table->select);
      break;
    case kTableName:
      query += table->name;
      if (table->alias != NULL)
        query += " AS " + string(table->alias);
      break;
    case kTableJoin:
      query += tableRefToString(table->join->left);
      switch (table->join->type)
      {
        case kJoinCross:
          query += "CROSS JOIN";
        case kJoinInner:
          query += " INNER JOIN ";
          break;
        case kJoinOuter:
          query += " OUTER JOIN ";
          break;
        case kJoinLeftOuter:
          query += " LEFT OUTER JOIN ";
          break;
        case kJoinLeft:
          query += " LEFT JOIN ";
          break;
        case kJoinRightOuter:
          query += " RIGHT OUTER JOIN ";
        case kJoinRight:
          query += " RIGHT JOIN ";
          break;
        case kJoinNatural:
          query += " NATURAL JOIN ";
          break;
      }
      query += tableRefToString(table->join->right);
      if (table->join->condition != NULL)
        query += " ON " + expressionToString(table->join->condition);
      break;
    case kTableCrossProduct:
      bool comma = false;
      for (TableRef* tbl: *table->list)
      {
        if (comma)
          query += ", ";
        query += tableRefToString(tbl);
        comma = true;
      }
      break;
  }
  return query;
}

// Entry point of sql5300
int main(int argc, char** argv)
{
  // Database environment
  if (argc != 2)
  {
    cerr << "Usage: CPSC5300: dbenvpath" << endl;
    return 1;
  }  
  char* home = getenv("HOME");
  string envDir = string(home) + argv[1];
  cout << "sql5300: running with database environment at " << envDir << endl;
  DbEnv env(0U);
  env.set_message_stream(&cout);
  env.set_error_stream(&cerr);
  try
  {
    env.open(envDir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
  }
  catch (DbException &exc)
  {
    cerr << "sql5300: " << exc.what() << endl;
    exit(1);
  }

  // SQL shell loop
  string query;
  while (true)
  {
    cout << "SQL> ";
    getline(cin, query);
    if (query.length() == 0)
      continue;
    if (toLower(query) == "quit")
      break;
    if (toLower(query) == "test") {
            //cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            continue;
        }
    SQLParserResult* result = SQLParser::parseSQLString(query.c_str());
    if (!result->isValid())
    {
      cout << "invalid SQL: " << query << endl;
      delete result;
      continue;
    }
    for (uint i = 0; i < result->size(); i++)
    {
      cout << execute(result->getStatement(i)) << endl;
    }
    delete result;
  }
  return EXIT_SUCCESS;
}

// Makes string lowercase
string toLower(string str)
{
  for (uint i = 0; i < str.length(); i++)
    str[i] = tolower(str[i]);
  return str;
}
