#include <sstream>
#include "db_cxx.h"
#include "SqlExec.h"

void SqlExec::initializeDbEnv(string envdir)
{
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    Db db(&env, 0);
    db.set_message_stream(env.get_message_stream());
    db.set_error_stream(env.get_error_stream());
    db.set_re_len(BLOCK_SZ);
    db.open(NULL, DB_NAME.c_str(), NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644);
    printf("(sql5300: running with database environment at %s)\n", envdir.c_str());
}

string SqlExec::execute(SQLParserResult *result)
{
    stringstream ss;
    const SQLStatement *stmt = result->getStatement(0);

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

    return ss.str();
}

void SqlExec::printExpression(Expr *expr, stringstream &ss)
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

void SqlExec::printTableRefInfo(TableRef *table, stringstream &ss)
{
    switch (table->type) {
        case kTableSelect:
            break;
        case kTableName:
            ss << table->name;
            break;
        case kTableJoin:
            printTableRefInfo(table->join->left, ss);
            if (table->join->type == kJoinLeft) {
                ss << " LEFT";
            }
            ss << " JOIN ";
            printTableRefInfo(table->join->right, ss);
            ss << " ON ";
            printExpression(table->join->condition, ss);
            break;
        case kTableCrossProduct:
            int count = table->list->size();
            for (TableRef* tbl : *table->list) {
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

string SqlExec::columnDefinitionToString(const ColumnDefinition *col)
{
        string ret(col->name);
    switch(col->type) {
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
