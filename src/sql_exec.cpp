/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include <cstring>

#include "sql_exec.h"
#define DEBUG_ENABLED
#include "debug.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    DEBUG_OUT("operator<< - begin\n");
    if (qres.column_names != nullptr) {
        DEBUG_OUT("operator<< - column_names != nullptr\n");
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
                    case ColumnAttribute::INT:  out << value.n;                 break;
                    case ColumnAttribute::TEXT: out << "\"" << value.s << "\""; break;
                    case ColumnAttribute::BOOLEAN: {
                        if (value.n == 1) {
                            out << "true";
                        } else {
                            out << "false";
                        }
                        break;
                    }
                    default:                    out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message << "\n";
    DEBUG_OUT("operator<< - end\n");
    return out;
}

QueryResult::~QueryResult() {
    delete column_names;
    delete column_attributes;
    delete rows;
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // FIXED: initialize _tables table, if not yet present
    DEBUG_OUT("SQLExec::execute() - begin\n");
    if (!tables) {
        tables = new Tables();
        tables->open();
    }

    if (!indices) {
        indices = new Indices();
        indices->open();
    }

    try {
        DEBUG_OUT("SQLExec::execute() - try\n");
        switch (statement->type()) {
            case kStmtCreate:   return create((const CreateStatement *) statement);
            case kStmtDrop:     return drop((const DropStatement *) statement);
            case kStmtShow:     return show((const ShowStatement *) statement);
            default:            return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        DEBUG_OUT("SQLExec::execute() - catch\n");
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
    DEBUG_OUT("SQLExec::execute() - end\n");
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    DEBUG_OUT("SQLExec::column_definition() - begin\n");
    column_name = std::string(col->name);
    switch (col->type) {
        case ColumnDefinition::INT:     column_attribute.set_data_type(ColumnAttribute::INT);   break;
        case ColumnDefinition::TEXT:    column_attribute.set_data_type(ColumnAttribute::TEXT);  break;
        default:                        throw SQLExecError("Unknown column type");
    }
    DEBUG_OUT("SQLExec::column_definition() - end\n");
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    DEBUG_OUT("SQLExec::create() - begin\n");
    switch (statement->type) {
        case CreateStatement::CreateType::kTable:   return create_table(statement);
        case CreateStatement::CreateType::kIndex:   return create_index(statement);
        default:                                    return new QueryResult("Cannot create unknown entity type!");
    }
    DEBUG_OUT("SQLExec::create() - end\n");
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    DEBUG_OUT("SQLExec::create() - begin\n");

    // Add table to _tables
    string table_name = string(statement->tableName);
    ValueDict table_record = {{"table_name", Value(table_name)}};
    Handle table_handle;
    try {
        DEBUG_OUT("SQLExec::create() - try\n");
        table_handle = tables->insert(&table_record);
    } catch (DbRelationError &e) {
        DEBUG_OUT_VAR("SQLExec::create() - catch: %s\n", e.what());
        throw e;
    }

    // Add columns to _columns
    DbRelation &columns_table = tables->get_table(Columns::TABLE_NAME);
    Handles *column_handles = new Handles();
    try {
        DEBUG_OUT("SQLExec::create() - try\n");
#ifdef DEBUG_ENABLED
        int loop_count = 0;
#endif
        for (auto const &column : *statement->columns) {
            DEBUG_OUT_VAR("SQLExec::create() - for(%d)\n", loop_count++);
            string type;
            if (column->type == ColumnDefinition::DataType::INT)
                type = "INT";
            else if (column->type == ColumnDefinition::DataType::TEXT) {
                type = "TEXT";
            } else {
                throw DbRelationError("Bad type");
            }

            ValueDict row;
            row["table_name"] = Value(table_name);
            row["column_name"] = Value(column->name);
            row["data_type"] = Value(type);
            DEBUG_OUT("SQLExec::create() - insert\n");
            Handle column_handle = columns_table.insert(&row);
            column_handles->push_back(column_handle);
        }
    } catch (DbRelationError &e) {
        DEBUG_OUT_VAR("SQLExec::create() - catch: %s\n", e.what());
        // Something prevented adding columns to _columns,
        // must remove table, as well as columns we did add.
        tables->del(table_handle); // Deleting the wrong table?

        for (auto const &handle : *column_handles) {
            columns_table.del(handle);
        }
        delete column_handles;
        DEBUG_OUT("SQLExec::create() - end catch\n");
        throw e;
    }

    // Create file for table
    DbRelation &table = tables->get_table(table_name);
    table.create();

    DEBUG_OUT("SQLExec::create() - end (success)\n");
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::EntityType::kTable:    return drop_table(statement);
        case DropStatement::EntityType::kIndex:     return drop_index(statement);
        default:                                    return new QueryResult("Cannot drop unknown entity type!");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    DEBUG_OUT("SQLExec::drop_table() - begin\n");
    string table_name = statement->name;
    if (table_name == "_tables" || table_name == "_columns") {
        DEBUG_OUT("SQLExec::drop() - end throw\n");
        throw SQLExecError("SQLExecError: Cannot drop a schema table");
    }

    ValueDict where;
    where["table_name"] = table_name;
    Handles *handles = tables->select(&where);

    if (handles->size() == 0) {
        DEBUG_OUT("SQLExec::drop() - end throw\n");
        delete handles;
        throw SQLExecError("SQLExecError: Table does not exist");
    }

    // remove from _tables schema
    tables->del((*handles)[0]);

    // remove from _columns schema
    DbRelation &columns_table = tables->get_table(Columns::TABLE_NAME);
    where.clear();
    where["table_name"] = Value(table_name);
    handles = columns_table.select(&where);
    for (Handle handle : *handles) {
        columns_table.del(handle);
    }
    delete handles;

    // drop the table
    DbRelation &table = tables->get_table(table_name);
    table.drop();
    string message = "dropped " + table_name;
    DEBUG_OUT("SQLExec::drop_table() - end\n");
    return new QueryResult(message);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    DEBUG_OUT("SQLExec::drop_index() - begin\n");
    string table_name = string(statement->name);
    string index_name = string(statement->indexName);

    DEBUG_OUT("SQLExec::drop_index() - got the index\n");
    ValueDict target;
    target["table_name"] = table_name;
    target["index_name"] = index_name;
    Handles *handles = indices->select(&target);
    DEBUG_OUT("SQLExec::drop_index() - got the handles\n");
    for (Handle &handle : *handles) {
        indices->del(handle);
    }
    delete handles;

    DEBUG_OUT("SQLExec::drop_index() - deleted the handles\n");
    // Indices::del() handles dropping the DbIndex object
    DEBUG_OUT("SQLExec::drop_index() - end (success)\n");
    return new QueryResult("dropped index " + index_name);
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::EntityType::kTables:    return show_tables();
        case ShowStatement::EntityType::kColumns:   return show_columns(statement);
        case ShowStatement::EntityType::kIndex:     return show_index(statement);
        default:                                    return new QueryResult("Cannot show unknown entity type!");
    }
}

QueryResult *SQLExec::show_tables() {
    DEBUG_OUT("SQLExec::show_tables() - begin\n");
    ColumnNames *names = new ColumnNames();
    ColumnAttributes *attribs = new ColumnAttributes();
    tables->get_columns(Tables::TABLE_NAME, *names, *attribs);

    ValueDicts *rows = new ValueDicts();
    Handles *handles = tables->select();
    for (Handle &handle : *handles) {
        DEBUG_OUT("SQLExec::show_tables() - for\n");
        ValueDict *row = tables->project(handle);
        if ((*row)["table_name"].s == "_tables"  ||
            (*row)["table_name"].s == "_columns" ||
            (*row)["table_name"].s == "_indices") {
            continue;
        }
        rows->push_back(row);
    }
    string message = "successfully returned " + std::to_string(rows->size()) + " rows";
    DEBUG_OUT_VAR("SQLExec::show_tables() - names: %ld\n", names->size());
    DEBUG_OUT_VAR("SQLExec::show_tables() - attribs: %ld\n", attribs->size());
    DEBUG_OUT_VAR("SQLExec::show_tables() - message: %s\n", message.c_str());

    delete handles;
    DEBUG_OUT("SQLExec::show_tables() - end\n");
    return new QueryResult(names, attribs, rows, message);
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DEBUG_OUT("SQLExec::show_columns() - begin\n");

    ColumnNames *names = new ColumnNames();
    ColumnAttributes *attribs = new ColumnAttributes();
    tables->get_columns(Columns::TABLE_NAME, *names, *attribs);

    ValueDict target_table;
    target_table["table_name"] = Value(statement->tableName);

    DbRelation &col_table = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *col_handles = col_table.select(&target_table);

    ValueDicts *rows = new ValueDicts();
    for (auto const &handle : *col_handles)
    {
        ValueDict *row = col_table.project(handle, names);
        rows->push_back(row);
    }

    string message = "successfully returned " + std::to_string(rows->size()) + " rows";

    DEBUG_OUT_VAR("SQLExec::show_columns() - names->size(): %ld\n", names->size());
    DEBUG_OUT_VAR("SQLExec::show_columns() - attribs->size(): %ld\n", attribs->size());
    DEBUG_OUT_VAR("SQLExec::show_columns() - rows->size(): %ld\n", rows->size());
    DEBUG_OUT_VAR("SQLExec::show_columns() - message: %s\n", message.c_str());

    DEBUG_OUT("SQLExec::show_columns() - end\n");
    delete col_handles;
    return new QueryResult(names, attribs, rows, message);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    DEBUG_OUT("SQLExec::create_index() - begin\n");
    string table_name = string(statement->tableName);
    string index_name = string(statement->indexName);
    
    ValueDict target;
    target["table_name"] = Value(table_name);

    // Make sure table exists first
    Handles *handles = tables->select(&target);
    if (handles->size() == 0) {
        delete handles;
        throw SQLExecError("table " + table_name + " does not exist!");
    }
    delete handles;

    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["seq_in_index"] = Value(0);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value((strcmp("BTREE", statement->indexType) == 0));

    DEBUG_OUT("SQLExec::create_index() - sequence in index\n");
    size_t seq_num = 1;
    for (auto const &column : *statement->indexColumns) {
        DEBUG_OUT_VAR("SQLExec::create_index() - for(%ld)\n", seq_num);
        row["seq_in_index"] = Value(seq_num++);
        row["column_name"] = Value(column);
        indices->insert(&row);
    }

    // Create file for index
    DbIndex &index = indices->get_index(table_name, index_name);
    index.create();

    DEBUG_OUT("SQLExec::create_index() - end\n");
    return new QueryResult("created index " + index_name);
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    DEBUG_OUT("SQLExec::show_index() - begin\n");
    string table_name = string(statement->tableName);
    ColumnNames *names = new ColumnNames(indices->get_column_names());
    ColumnAttributes *attribs = new ColumnAttributes();

    ValueDicts *rows = new ValueDicts();
    ValueDict target_table;
    target_table["table_name"] = table_name;

    Handles *handles = indices->select(&target_table);
    for (Handle &handle : *handles) {
        ValueDict *row = indices->project(handle);
        rows->push_back(row);
    }

    delete handles;
    DEBUG_OUT("SQLExec::show_index() - end\n");
    string message = "successfully returned " + std::to_string(rows->size()) + " rows";
    return new QueryResult(names, attribs, rows, message);
}
