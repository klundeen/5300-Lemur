/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @authors Luan (Remi) Ta, Preedhi Garg
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "SQLExecutor.h"
#include "ParseTreeToString.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;


Tables* SQLExec::getInstance()
{
    if (tables == nullptr){        
        initialize_schema_tables();
        tables = new Tables();        
    }    
    return tables;
}

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

void SQLExec::closeMeta()
{
    if (tables != nullptr) {
        tables->close();
        delete tables;
    }
}

QueryResult::~QueryResult() {
    if (column_names != nullptr) delete column_names;
    if (column_attributes != nullptr) delete column_attributes;
    if (rows != nullptr) delete rows;    
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    if (tables == nullptr){        
        initialize_schema_tables();
        tables = new Tables();        
    }    
    cout << ParseTreeToString::statement(statement) << endl;
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
    column_name = string(col->name);

    switch (col->type) {
        case ColumnDefinition::DataType::INT:
            column_attribute = ColumnAttribute(ColumnAttribute::DataType::INT);
            break;
        case ColumnDefinition::DataType::TEXT:
            column_attribute = ColumnAttribute(ColumnAttribute::DataType::TEXT);
            break;
        
        default:
            throw InvalidDataType("DataType not recognized");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    ColumnNames* colNames = new ColumnNames;
    ColumnAttributes* colAtrs = new ColumnAttributes;

    for (ColumnDefinition* def : *statement->columns) {
        Identifier curName;
        ColumnAttribute curAtr;

        column_definition(def, curName, curAtr);
        colNames->push_back(curName);
        colAtrs->push_back(curAtr);
    }

    HeapTable tab(string(statement->tableName), *colNames, *colAtrs);
    Handles colHands; // in case we need to rollback
    Handle* tabHand = nullptr;

    DbRelation* colMeta = &tables->get_table("_columns"); // DO NOT deallocate: singleton in table cache

    try {
        
        // Insert metadata
        unique_ptr<ValueDict> tabData(new ValueDict);
        (*tabData)["table_name"] = Value(statement->tableName);
        Handle tmp(tables->insert(tabData.get()));
        tabHand = &tmp;  

        ValueDict colData;

        for (int i = 0; i < colAtrs->size(); i++) {
            switch (colAtrs->at(i).get_data_type()) {
                case ColumnAttribute::DataType::INT:
                    colData["data_type"] = Value("INT");
                    break;
                case ColumnAttribute::DataType::TEXT:
                    colData["data_type"] = Value("TEXT");
                    break;
                case ColumnAttribute::DataType::BOOLEAN:
                    colData["data_type"] = Value("BOOLEAN");
                    break;
                default:
                    throw InvalidDataType("DataType not recognized");
            }
            colData["table_name"] = Value(statement->tableName);
            colData["column_name"] = Value(colNames->at(i));

            colHands.push_back(colMeta->insert(&colData)); // in case we need to rollback
        } 

        // We're all clear. Can go ahead and create the physical table
        tab.create();
        tab.close();
    
        return new QueryResult("created " + string(statement->tableName));

    } catch (DbRelationError& e) {
        // Failure... rollback changes
        if (tabHand != nullptr) tables->del(*tabHand);
        for (Handle& hand : colHands) colMeta->del(hand);
        
        return new QueryResult("Error: DbRelationError: " + string(e.what()));
    }
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    string tabName(statement->name);
    if (statement->type != DropStatement::EntityType::kTable)
        return new QueryResult("Error: not implemented");
    if (tabName == Columns::TABLE_NAME || tabName == Tables::TABLE_NAME || tabName == Indices::TABLE_NAME)
        return new QueryResult("Error: Unable to drop " + tabName);
    
    try {
        // Drop table file first
        DbRelation* tab = &tables->get_table(statement->name);
        tab->drop();

        ValueDict where;
        where["table_name"] = Value(tabName);
        // Delete metadata from _tables
        tables->del(*unique_ptr<Handles>(tables->select(&where))->begin());

        // Delete metadata from _columns
        DbRelation* colMeta = &tables->get_table("_columns"); // DO NOT deallocate: singleton in table cache
    
        std::unique_ptr<Handles> rows(colMeta->select(&where));

        for (const Handle& hand : *rows)
            colMeta->del(hand);        

        return new QueryResult("dropped ");

    } catch (DbRelationError& e) {
        return new QueryResult("Error: DbRelationError: " + string(e.what()));
    }

}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    if (statement->type == ShowStatement::EntityType::kTables)    
        return show_tables();
    return show_columns(statement);
}

QueryResult *SQLExec::show_tables() {
    unique_ptr<Handles> handles(tables->select());

    ValueDicts* rows = new ValueDicts;

    ColumnNames* colNames = new ColumnNames;
    colNames->push_back("table_name");

    ColumnAttributes* atrs = new ColumnAttributes;
    atrs->push_back(ColumnAttribute(ColumnAttribute::DataType::TEXT));

    for (const Handle &handle: *handles) {
        ValueDict* row = tables->project(handle, colNames);
        if (((*row)["table_name"]) != Value(Tables::TABLE_NAME) 
         && ((*row)["table_name"]) != Value(Columns::TABLE_NAME)
         && ((*row)["table_name"]) != Value(Indices::TABLE_NAME) ) {
            rows->push_back(row);
        }

    }

    return new QueryResult(colNames, atrs, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    ValueDicts* rows = new ValueDicts;

    ColumnNames colNames;
    ColumnAttributes colAtrs;

    tables->get_columns(statement->tableName, colNames, colAtrs);    

    for (int i = 0; i < colNames.size(); i++) {
        ValueDict* row = new ValueDict;
        (*row)["table_name"] = Value(string(statement->tableName));
        (*row)["column_name"] = Value(colNames.at(i)); 
        
        if (colAtrs.at(i).get_data_type() == ColumnAttribute::DataType::INT)
            (*row)["data_type"] = Value(string("INT"));
        else if (colAtrs.at(i).get_data_type() == ColumnAttribute::DataType::TEXT)
            (*row)["data_type"] = Value(string("TEXT"));
        else if (colAtrs.at(i).get_data_type() == ColumnAttribute::DataType::BOOLEAN)
            (*row)["data_type"] = Value(string("BOOLEAN"));
        else
            throw InvalidDataType("DataType not recognized");        

        rows->push_back(row);
    }

    ColumnNames* showColNames = new ColumnNames;
    ColumnAttributes* showColAtrs = new ColumnAttributes;

    showColNames->push_back("table_name");
    showColNames->push_back("column_name");
    showColNames->push_back("data_type");

    showColAtrs->push_back(ColumnAttribute(ColumnAttribute::DataType::TEXT));
    showColAtrs->push_back(ColumnAttribute(ColumnAttribute::DataType::TEXT));
    showColAtrs->push_back(ColumnAttribute(ColumnAttribute::DataType::TEXT));

    return new QueryResult(showColNames, showColAtrs, rows, "successfully returned " + to_string(rows->size()) + " rows");
}


QueryResult *SQLExec::show_index(const ShowStatement *statement) {
     return new QueryResult("show index not implemented"); // FIXME
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    return new QueryResult("drop index not implemented");  // FIXME
}