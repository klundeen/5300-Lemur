#include "SQLParser.h"
#include <sstream>
#include <string>
#include <stdio.h>
#include <iostream>

using namespace std;
using namespace hsql;

class SqlExec
{
public:
    /**
     * Convert the parser result back into equivalent SQL command string
     * Support SELECT and CREATE statment
     * @param result  parser result to unparse
     * @return        SQL equivalent to *result
     */
    string execute(SQLParserResult *result)
    {
        stringstream ss;
        for (size_t i = 0; i < result->size(); ++i)
        {
            const SQLStatement *stmt = result->getStatement(i);
            if (stmt->type() == kStmtSelect)
            {
                ss << "SELECT ";
                int count = ((SelectStatement *)stmt)->selectList->size();
                for (Expr *expr : *((SelectStatement *)stmt)->selectList)
                {
                    printExpression(expr, ss);
                    if (--count)
                    {
                        ss << ",";
                    }
                    ss << " ";
                }

                ss << "FROM ";

                printTableRefInfo(((SelectStatement *)stmt)->fromTable, ss);

                if (((SelectStatement *)stmt)->whereClause != NULL)
                {
                    ss << " WHERE ";
                    printExpression(((SelectStatement *)stmt)->whereClause, ss);
                }
            }

            if (stmt->type() == kStmtCreate)
            {
                ss << "CREATE TABLE " << ((CreateStatement *)stmt)->tableName;

                ss << " (";
                int count = ((CreateStatement *)stmt)->columns->size();
                for (auto col : *((CreateStatement *)stmt)->columns)
                {
                    ss << columnDefinitionToString(col);
                    if (--count)
                    {
                        ss << ", ";
                    }
                }
                ss << ")";
            }
            
            if (i != result->size() - 1)
                ss << "\n";
        }
        return ss.str();
    }

private:
    /**
     * Extract expressions and append to sql command string builder
     * @param expr  expression pointer contains sql data
     * @param ss    string builder used to append data component to sql string
     */
    void printExpression(Expr *expr, stringstream &ss)
    {
        switch (expr->type)
        {
        case kExprStar:
            ss << "*";
            break;
        case kExprColumnRef:
            if (expr->hasTable())
            {
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
            printExpression(expr->expr, ss);
            switch (expr->opType)
            {
            case Expr::SIMPLE_OP:
                ss << " " << expr->opChar << " ";
                break;
            default:
                ss << expr->opType;
                break;
            }
            if (expr->expr2 != NULL)
                printExpression(expr->expr2, ss);
            break;
        default:
            fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
            return;
        }
    }

    /**
     * Parse table reference into sql data component and append to sql string
     * @param table  table reference pointer
     * @param ss     string builder used to append data component to sql string
     */
    void printTableRefInfo(TableRef *table, stringstream &ss)
    {
        switch (table->type)
        {
        case kTableSelect:
            break;
        case kTableName:
            ss << table->name;
            break;
        case kTableJoin:
            printTableRefInfo(table->join->left, ss);
            if (table->join->type == kJoinLeft)
            {
                ss << " LEFT";
            }
            ss << " JOIN ";
            printTableRefInfo(table->join->right, ss);
            ss << " ON ";
            printExpression(table->join->condition, ss);
            break;
        case kTableCrossProduct:
            int count = table->list->size();
            for (TableRef *tbl : *table->list)
            {
                printTableRefInfo(tbl, ss);
                if (--count)
                {
                    ss << ", ";
                }
            }
            break;
        }
        if (table->alias != NULL)
        {
            ss << " AS " << table->alias;
        }
    }

    /**
     * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
     * @param col  column definition to unparse
     * @return     SQL equivalent to *col
     */
    string columnDefinitionToString(const ColumnDefinition *col)
    {
        string ret(col->name);
        switch (col->type)
        {
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
};
