/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @authors Luan (Remi) Ta, Preedhi Garg
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "SQLExecutor.h"
#include "ParseTreeToString.h"
#include <vector>

using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;


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
                    case ColumnAttribute::BOOLEAN:
                        out << "\"" << (value.b ? "true" : "false") << "\"";
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
    if (indices != nullptr) {
        indices->close();
        delete indices;
    }
}

QueryResult::~QueryResult() {
    // Col names & atrs are not dynamic but static
    if (rows != nullptr) {
        for (const ValueDict* dict : *rows) delete dict;
        delete rows; 
    }   
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    if (tables == nullptr) {        
        initialize_schema_tables();
        tables = new Tables();        
    } 
    if (indices == nullptr) {
        indices = new Indices();
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

QueryResult *SQLExec::create(const CreateStatement *statement) 
{
    switch (statement->type)
    {
    case CreateStatement::CreateType::kTable:
        return createTable(statement);
    case CreateStatement::CreateType::kIndex:
        return createIndex(statement);
    default:
        return new QueryResult("Unknown CreateType not implemented");
    }
}

QueryResult* SQLExec::createTable(const CreateStatement *statement) 
{
    ColumnNames colNames;
    ColumnAttributes colAtrs;

    for (ColumnDefinition* def : *statement->columns) {
        Identifier curName;
        ColumnAttribute curAtr;

        column_definition(def, curName, curAtr);
        colNames.push_back(curName);
        colAtrs.push_back(curAtr);
    }

    HeapTable tab(string(statement->tableName), colNames, colAtrs);
    Handles colHands; // in case we need to rollback
    Handle* tabHand = nullptr;

    DbRelation* colMeta = &tables->get_table(Columns::TABLE_NAME); // DO NOT deallocate: singleton in table cache

    try {
        
        // Insert metadata
        unique_ptr<ValueDict> tabData(new ValueDict);
        (*tabData)["table_name"] = Value(statement->tableName);
        Handle tmp(tables->insert(tabData.get()));
        tabHand = &tmp;  

        ValueDict colData;

        for (int i = 0; i < colAtrs.size(); i++) {
            switch (colAtrs.at(i).get_data_type()) {
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
            colData["column_name"] = Value(colNames.at(i));

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
QueryResult* SQLExec::createIndex(const CreateStatement *statement)
{
    string tabName(statement->tableName);
    string idxName(statement->indexName);
    string idxType(statement->indexType);
    ColumnNames cols;
    ColumnAttributes colAtrs;

    try {
        DbRelation* tab = &tables->get_table(tabName);
    } catch (...) {            
        return new QueryResult("Error: table " + tabName + " does not exist");
    }

    tables->get_columns(tabName, cols, colAtrs);    
    ValueDict row;
    row["table_name"] = tabName;
    row["index_name"] = idxName;
    row["index_type"] = idxType;    
    row["is_unique"] = Value(idxType == "BTREE");

    int seq = 1;
    Handles insertedIdx;
    for (const char* idxCol : *statement->indexColumns) {
        string curCol(idxCol);
        if (find(cols.begin(), cols.end(), curCol) == cols.end()){
            for (const Handle& hand : insertedIdx) indices->del(hand);
            return new QueryResult("Error: Mismatch index columns on table " + tabName + " for index " + idxName);
        }
        row["column_name"] = curCol;
        row["seq_in_index"] = seq++;
        try {
            insertedIdx.push_back(indices->insert(&row));
        } catch (DbRelationError& e) {
            // Failure... rollback changes
            for (const Handle& hand : insertedIdx) indices->del(hand);
            
            return new QueryResult("Error: DbRelationError: " + string(e.what()));
        }
    }
    // All's clear
    DbIndex* idxTab = &indices->get_index(tabName, idxName);
    idxTab->create();
    idxTab->close();

    return new QueryResult("created index " + idxName);    
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type)
    {
    case DropStatement::EntityType::kTable:
        return drop_table(statement);   
    case DropStatement::EntityType::kIndex:
        return drop_index(statement->name, statement->indexName); 
    default:
        return new QueryResult("Unknown EntityType not implemented");
    }

}

QueryResult *SQLExec::drop_table(const hsql::DropStatement *statement)
{
    string tabName(statement->name);
    if (statement->type != DropStatement::EntityType::kTable)
        return new QueryResult("Error: not implemented");
    if (tabName == Columns::TABLE_NAME || tabName == Tables::TABLE_NAME || tabName == Indices::TABLE_NAME)
        return new QueryResult("Error: Unable to drop " + tabName);
    
    // Drop each index
    for (auto const& idx : indices->get_index_names(tabName))
        unique_ptr<QueryResult>(drop_index(tabName, idx));

    // Drop table file
    DbRelation* tab = &tables->get_table(statement->name);
    tab->drop();

    ValueDict where;
    where["table_name"] = Value(tabName);
    // Delete metadata from _tables
    tables->del(*unique_ptr<Handles>(tables->select(&where))->begin());

    // Delete metadata from _columns
    DbRelation* colMeta = &tables->get_table(Columns::TABLE_NAME); // DO NOT deallocate: singleton in table cache

    std::unique_ptr<Handles> rows(colMeta->select(&where));

    for (const Handle& hand : *rows)
        colMeta->del(hand);

    return new QueryResult("dropped table " + tabName);
}

QueryResult *SQLExec::drop_index(string tabName, string idxName) {  
    DbIndex* idxTab = &indices->get_index(tabName, idxName);

    try {
        idxTab->drop();
    } catch (...) {
        return new QueryResult("Index " + idxName + " from table " + tabName + " is not found");
    }

    ValueDict where;
    where["table_name"] = tabName;
    where["index_name"] = idxName;
    unique_ptr<Handles> hands(indices->select(&where));

    for (const Handle& hand : *hands) // should only run once
        indices->del(hand);
    
    return new QueryResult("dropped index " + idxName);  // FIXME
}


QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type)
    {
    case ShowStatement::EntityType::kTables:
        return show_tables();
    case ShowStatement::EntityType::kColumns:
        return show_columns(statement);
    case ShowStatement::EntityType::kIndex:
        return show_index(statement);
    default:
        return new QueryResult("Unknown EntityType not implemented");
    }       
}

QueryResult *SQLExec::show_tables() {
    unique_ptr<Handles> handles(tables->select());

    ValueDicts* rows = new ValueDicts;

    ColumnNames* colNames = &Tables::COLUMN_NAMES();

    ColumnAttributes* atrs = &Tables::COLUMN_ATTRIBUTES();

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

    ColumnNames* showColNames = &Columns::COLUMN_NAMES();
    ColumnAttributes* showColAtrs = &Columns::COLUMN_ATTRIBUTES();

    return new QueryResult(showColNames, showColAtrs, rows, "successfully returned " + to_string(rows->size()) + " rows");
}


QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames* colNames = &Indices::COLUMN_NAMES();
    ColumnAttributes* colAtrs = &Indices::COLUMN_ATTRIBUTES();
    string tabName(statement->tableName);

    ValueDict where;
    where["table_name"] = tabName;
    unique_ptr<Handles> hands(indices->select(&where));

    ValueDicts* rows = new ValueDicts;

    for (const Handle& hand : *hands) {
        rows->push_back(indices->project(hand));
    }

    return new QueryResult(colNames, colAtrs, rows, "successfully returned " + to_string(rows->size()) + " rows");
}
