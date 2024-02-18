/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Winter Quarter 2024"
 */
#include "sql_exec.h"
#define DEBUG_ENABLED
#include "debug.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

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

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
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

    // Add table to _tables
    string table_name = string(statement->tableName);
    ValueDict table_record = {{"table_name", Value(table_name)}};
    try {
        DEBUG_OUT("SQLExec::create() - try\n");
        tables->insert(&table_record);
    } catch (DbRelationError &e) {
        DEBUG_OUT_VAR("SQLExec::create() - catch: %s\n", e.what());
        return new QueryResult("Error: DbRelationError: " + string(e.what()));
    }

    // Create table
    DbRelation &table = tables->get_table(table_name);
    table.create();

    // Add columns to _columns
    try {
        DEBUG_OUT("SQLExec::create() - try\n");
        DbRelation &columns_table = tables->get_table(Columns::TABLE_NAME); // "runtime polymorphism"
        try {
            DEBUG_OUT("SQLExec::create() - try\n");
            for (auto const &column : *statement->columns) {
                DEBUG_OUT("SQLExec::create() - for\n");
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
                columns_table.insert(&row);
            }
        } catch (DbRelationError &e) {
            DEBUG_OUT_VAR("SQLExec::create() - catch: %s\n", e.what());
            for (size_t i = 0; i < statement->columns->size(); i++) {
                Handles *handles = columns_table.select(&table_record);
                for (auto handle : *handles) {
                    columns_table.del(handle);
                }
            }
            DEBUG_OUT("SQLExec::create() - end catch\n");
            return new QueryResult("Error: DbRelationError: " + string(e.what()));
        }
    } catch (DbRelationError &e) {
        DEBUG_OUT_VAR("SQLExec::create() - catch: %s\n", e.what());
        tables->del((*(tables->select(&table_record)))[0]); // get first record, FIXME: this looks bad
        DEBUG_OUT("SQLExec::create() - end catch\n");
        return new QueryResult("Error: DbRelationError: " + string(e.what()));
    }

    DEBUG_OUT("SQLExec::create() - end (success)\n");
    return new QueryResult("created " + table_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    string table_name = statement->name;
    if (table_name == "_tables" || table_name == "_columns") {
        throw SQLExecError("Cannot drop a schema table");
    }

    DbRelation &table = tables->get_table(table_name);
    ValueDict where;
    where["table_name"] = table_name;
    Handles *handles = table.select(&where);

    if (handles->size() == 0) {
        throw SQLExecError("Table does not exist");
    }

    // remove from _tables schema
    tables->del((*handles)[0]);

    // remove from _columns schema
    DbRelation &columns_table = tables->get_table(Columns::TABLE_NAME);
    Handles *col_handles = columns_table.select(&where);

    for (Handle &handle : *col_handles) {
        columns_table.del(handle);
    }

    table.drop();

    delete handles;
    delete col_handles;

    string message = "Dropped " + table_name;
    return new QueryResult(message);
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::EntityType::kTables:    return show_tables();
        case ShowStatement::EntityType::kColumns:   return show_columns(statement);
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
        ValueDict *row = tables->project(handle);
        if ((*row)["table_name"].s == "_tables" || (*row)["table_name"].s == "_columns") {
            continue;
        }
        rows->push_back(row);
    }
    string message = "successfully returned " + std::to_string(rows->size()) + " rows";
    DEBUG_OUT_VAR("SQLExec::show_tables() - msg: %s\n", message.c_str());
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
    return new QueryResult(names, attribs, rows, message);
}
