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
        DEBUG_OUT("operator - column_names != nullptr\n");
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
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
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
    if (!tables) {
        tables = new Tables();
        tables->open();
    }
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    throw SQLExecError("not implemented");  // FIXME
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    string table_name = string(statement->tableName);
    ValueDict table_record = {{"table_name", Value(table_name)}};
    tables->insert(&table_record);
    try {
        DbRelation &columns_table = tables->get_table(table_name); // "runtime polymorphism"
        try {
            for (auto const &column : *statement->columns) {
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
                columns_table.insert(&row);
            }
        } catch (DbRelationError &e) {
            for (size_t i = 0; i < statement->columns->size(); i++) {
                Handles *handles = columns_table.select(&table_record);
                for (auto handle : *handles) {
                    columns_table.del(handle);
                }
            }
            return new QueryResult("ERR CREATE: " + string(e.what()) + "\n");
        }
    } catch (DbRelationError &e) {
        tables->del((*(tables->select(&table_record)))[0]); // get first record, FIXME: this looks bad
        return new QueryResult("ERR CREATE: " + string(e.what()) + "\n");
    }
    return new QueryResult("Created " + table_name + "\n");
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
        case ShowStatement::EntityType::kTables:
            return show_tables();
        case ShowStatement::EntityType::kColumns:
            return show_columns(statement);
        default:
            return new QueryResult("Cannot show unknown entity type!");
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
    string message = "succesfully returned " + std::to_string(rows->size()) + " rows";
    DEBUG_OUT_VAR("SQLExec::show_tables() - msg: %s\n", message.c_str());
    DEBUG_OUT_VAR("SQLExec::show_tables() - names: %ld\n", names->size());
    DEBUG_OUT_VAR("SQLExec::show_tables() - attribs: %ld\n", attribs->size());
    delete handles;
    DEBUG_OUT("SQLExec::show_tables() - end\n");
    return new QueryResult(names, attribs, rows, message);
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DEBUG_OUT("SQLExec::show_columns() - begin\n");
    string table_name = string(statement->tableName);
    ColumnNames *names = new ColumnNames();
    ColumnAttributes *attribs = new ColumnAttributes();
    tables->get_columns(table_name, *names, *attribs);
    DEBUG_OUT_VAR("SQLExec::show_columns() - names->size(): %ld\n", names->size());
    DEBUG_OUT_VAR("SQLExec::show_columns() - attribs->size(): %ld\n", attribs->size());

    ValueDicts *rows = new ValueDicts();
    for (Identifier name : *names) {
        ValueDict row;
        row["column_name"] = name;
        rows->push_back(&row);
    }
    DEBUG_OUT_VAR("SQLExec::show_columns() - rows->size(): %ld\n", rows->size());

    string message = "succesfully returned " + std::to_string(rows->size()) + " rows";
    DEBUG_OUT("SQLExec::show_columns() - end\n");
    return new QueryResult(names, attribs, rows, message);
}
