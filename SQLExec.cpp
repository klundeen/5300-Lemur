/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

Value value_from_expr(const Expr *expr, const DbRelation &table) {
    Value value;
    if (!expr) {
        throw SQLExecError("Null expression");
    }
    switch (expr->type) {
        case kExprLiteralInt:
            value.data_type = ColumnAttribute::INT;
            value.n = expr->ival;
            break;
        case kExprLiteralString:
            value.data_type = ColumnAttribute::TEXT;
            value.s = expr->name;
            break;
        default:
            throw SQLExecError("Unsupported data type in expression");
    }
    return value;
}

ValueDict where_clause_from_expr(const Expr *expr, const DbRelation &table) {
    ValueDict where_clause;
    if (!expr) {
        throw SQLExecError("Null WHERE expression");
    }
    if (expr->type == kExprOperator) {

        // Simple equality check implementation. 
        if (expr->opType == Expr::SIMPLE_OP && expr->opChar == '=') {
            if (expr->expr && expr->expr->type == kExprColumnRef && expr->expr2) {
                string column_name = expr->expr->name;
                Value value = value_from_expr(expr->expr2, table);
                where_clause[column_name] = value;
            } else {
                throw SQLExecError("Unsupported WHERE expression structure");
            }
        } 
        // AND expression check implementation
        else if (expr->opType == Expr::OperatorType::AND) {
            if (expr->expr && expr->expr2) {
                ValueDict left_clause = where_clause_from_expr(expr->expr, table);
                ValueDict right_clause = where_clause_from_expr(expr->expr2, table);
                // Merge the two ValueDict instances using logical AND
                for (const auto& pair : left_clause) {
                    const string& column_name = pair.first;
                    const Value& value = pair.second;
                    if (right_clause.find(column_name) != right_clause.end() && right_clause[column_name] == value) {
                        where_clause[column_name] = value;
                    }
                }
                for (const auto& pair : right_clause) {
                    const string& column_name = pair.first;
                    const Value& value = pair.second;
                    if (right_clause.find(column_name) != right_clause.end() && right_clause[column_name] == value) {
                        where_clause[column_name] = value;
                    }
                }
            } else {
                throw SQLExecError("Invalid AND expression");
            }
        } else {
            throw SQLExecError("Unsupported operator in WHERE expression");
        }
    } else {
        throw SQLExecError("Unsupported WHERE expression type");
    }
    return where_clause;
}


QueryResult *SQLExec::insert(const InsertStatement *statement) {
    try {
        Identifier table_name = statement->tableName;
        DbRelation &table = SQLExec::tables->get_table(table_name);
        ValueDict row;
        ColumnNames column_names;

        // If columns are specified in the statement
        if (statement->columns != nullptr) {
            for (unsigned int i = 0; i < statement->columns->size(); ++i) {
                string column_name = (*statement->columns)[i];
                Expr *value_expr = (*statement->values)[i];
                Value value = value_from_expr(value_expr, table); 
                row[column_name] = value;
            }
        } else {
            // If columns are not specified, assume values for all columns
            column_names = table.get_column_names(); // You need to implement this method if it doesn't exist
            if (column_names.size() != statement->values->size()) {
                throw SQLExecError("Values provided do not match number of columns in table");
            }
            for (unsigned int i = 0; i < column_names.size(); ++i) {
                Expr *value_expr = (*statement->values)[i];
                Value value = value_from_expr(value_expr, table);
                row[column_names[i]] = value;
            }
        }
        
        Handle handle = table.insert(&row);

        // Update indices if any
        auto index_names = SQLExec::indices->get_index_names(table_name);
        for (const auto &index_name : index_names) {
            DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
            index.insert(handle);
        }
        
        return new QueryResult("successfully inserted 1 row into " + table_name);
    } catch (const exception &e) {
        throw SQLExecError(string("Insert failed: ") + e.what());
    }
}

QueryResult *SQLExec::del(const DeleteStatement *statement) {
    try {
        Identifier table_name = statement->tableName;
        DbRelation &table = SQLExec::tables->get_table(table_name);
        EvalPlan *plan = new EvalPlan(table);

        if (statement->expr != nullptr) {
            ValueDict where_clause = where_clause_from_expr(statement->expr, table);
            plan = new EvalPlan(&where_clause, plan);
        }

        EvalPlan *optimized = plan->optimize();

        EvalPipeline pipeline = optimized->pipeline();

        auto index_names = SQLExec::indices->get_index_names(table_name);
        for (auto const &index_name : index_names) {
            DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
            for (auto const &handle : *pipeline.second) {
                // uncomment this once we have btree implemented
                // index.del(handle);
            }
        }

        for (auto const &handle : *pipeline.second) {
            table.del(handle);
        }

        return new QueryResult("Deleted " + to_string(pipeline.second->size()) + " rows from " + table_name);
    } catch (const exception &e) {
        throw SQLExecError(string("DELETE failed: ") + e.what());
    }
}


QueryResult *SQLExec::select(const SelectStatement *statement) {
    Identifier table_name = statement->fromTable->getName();

    // check table exists
    ValueDict where = {{"table_name", Value(table_name)}};
    Handles* tabMeta = SQLExec::tables->select(&where);
    bool tableExists = !tabMeta->empty();
    delete tabMeta;
    if (!tableExists)
        throw SQLExecError("attempting to select from non-existent table " + table_name);
    DbRelation& table = SQLExec::tables->get_table(table_name);
    ColumnNames* cn = new ColumnNames();
    for (const Expr* expr : *statement->selectList) {
        if (expr->type == kExprStar)
            for (const Identifier& col : table.get_column_names())
                cn->push_back(col);
        else
            cn->push_back(expr->name);
    }

    // start base of plan at a TableScan
    EvalPlan* plan = new EvalPlan(table);

    // enclose in selection if where clause exists
    if (statement->whereClause){
        ValueDict where_clause = where_clause_from_expr(statement->whereClause, table);
        plan = new EvalPlan(&where_clause, plan);
    }
            
    // wrap in project
    plan = new EvalPlan(cn, plan);

    // optimize and evaluate
    plan = plan->optimize();
    ValueDicts* rows = plan->evaluate();
    delete plan;
    return new QueryResult(cn, table.get_column_attributes(*cn), rows, "successfully return " + to_string(rows->size()) + " rows");
}


void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames &table_columns = table.get_column_names();
    for (auto const &col_name: *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it did
    }
    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const &index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  // drop the index
    }
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);  // remove all rows from _indices for each index on this table
    delete handles;

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

// SHOW ...
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}


QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}