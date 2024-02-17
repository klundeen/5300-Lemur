/**
 * @file heap_table.cpp - Implementation of HeapTable class
 * Operations: Create, Drop, Open, Close, Insert, Update, Delete,
 * Select, Project
 *
 * @see "Seattle University, CPSC 5300, Winter Quarter 2024"
 */
#include <cstring>

#include "heap_storage.h"
#define DEBUG_ENABLED
#include "debug.h"

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                     ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes),
      file(table_name) {}

void HeapTable::create() { 
    DEBUG_OUT("create_ 1\n");
    this->file.create(); 
    DEBUG_OUT("create_ 2\n");
    }

void HeapTable::create_if_not_exists() {
    try {
        DEBUG_OUT("create_if 1\n");
        this->file.open();
    } catch (DbException &e) {  // DBNoSuchFileError
        DEBUG_OUT("create_if 2\n");
        this->create();
        DEBUG_OUT("create_if 3\n");
    }
}

void HeapTable::drop() { this->file.drop(); }

void HeapTable::open() { this->file.open(); }

void HeapTable::close() { this->file.close(); }

Handle HeapTable::insert(const ValueDict *row) {
    ValueDict *validatedRow = this->validate(row);
    Handle handle = this->append(validatedRow);
    delete validatedRow;
    return handle;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {}

void HeapTable::del(const Handle handle) {}

// prof
bool HeapTable::selected(Handle handle, const ValueDict *where) {
    if (where == nullptr)
        return true;
    ValueDict *row = this->project(handle, where);
    bool is_selected = *row == *where;
    delete row;
    return is_selected;
}

// Prof code
Handles *HeapTable::select() {
    return select(nullptr);
}
 
// Prof code
Handles *HeapTable::select(const ValueDict *where) {
    open();
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id: *block_ids) {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id: *record_ids) {
            Handle handle(block_id, record_id);
            if (selected(handle, where))
                handles->push_back(Handle(block_id, record_id));
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict *HeapTable::project(Handle handle) {
    return this->project(handle, &this->column_names);
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID blockID = handle.first;
    RecordID recordID = handle.second;
    SlottedPage *page = this->file.get(blockID);
    Dbt *data = page->get(recordID);
    ValueDict *row = this->unmarshal(data);

    ValueDict *projectedRow = new ValueDict();
    for (auto const &columnName : *column_names) {
        auto search = row->find(columnName);
        if (search == row->end()) throw DbRelationError("Invalid column name");
        Value columnValue = search->second;
        projectedRow->insert({columnName, columnValue});
    }
    delete row;
    return projectedRow;
}

ValueDict *HeapTable::validate(const ValueDict *row) const {
    ValueDict *full_row = new ValueDict();
    for (auto const &column_name: this->column_names) {
        Value value;
        ValueDict::const_iterator column = row->find(column_name);
        if (column == row->end())
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        else
            value = column->second;
        (*full_row)[column_name] = value;
    }
    return full_row;
}

Handle HeapTable::append(const ValueDict *row) {
    SlottedPage *page;
    Dbt *data = this->marshal(row);
    Handle handle;
    bool hasRoom = false;
    BlockIDs *blockIDs = this->file.block_ids();
    for (BlockID &blockID : *blockIDs) {
        page = this->file.get(blockID);
        try {
            RecordID recordID = page->add(data);
            this->file.put(page);
            handle = Handle(blockID, recordID);
            hasRoom = true;
            delete page;
            break;
        } catch (DbBlockNoRoomError &e) {
            delete page;
        }
    }

    // no room in any existing blocks - create a new block
    if (!hasRoom) {
        page = this->file.get_new();
        RecordID recordID = page->add(data);
        BlockID blockID = page->get_block_id();
        this->file.put(page);
        delete page;
        handle = Handle(blockID, recordID);
    }
    delete data;
    delete blockIDs;
    return handle;
}

Dbt *HeapTable::marshal(const ValueDict *row) const {
    char bytes[DbBlock::BLOCK_SZ];
    uint offset = 0;
    uint columnNumber = 0;

    for (auto const &columnName : this->column_names) {
        ColumnAttribute ca = this->column_attributes[columnNumber++];
        ValueDict::const_iterator column = row->find(columnName);
        Value value = column->second;
        ColumnAttribute::DataType type = ca.get_data_type();
        if (type == ColumnAttribute::INT) {
            // n is int32_t we want 4 bytes
            auto size = sizeof(value.n);
            memcpy(bytes + offset, &value.n, size);
            offset += size;
        } else if (type == ColumnAttribute::TEXT) {
            // assume string length fits in 2 bytes (64k)
            auto size = sizeof(u_int16_t);
            u_int16_t len = value.s.length();
            memcpy(bytes + offset, &len, size);
            offset += size;
            // assume ascii
            memcpy(bytes + offset, value.s.c_str(), len);
            offset += len;
        } else {
            throw DbRelationError("Only supports marshaling INT and TEXT");
        }
    }

    if (offset > DbBlock::BLOCK_SZ - 8)
        throw DbRelationError("Row too large to marshal");

    char *trimmedBytes = new char[offset];
    memcpy(trimmedBytes, bytes, offset);

    return new Dbt(trimmedBytes, offset);
}

ValueDict *HeapTable::unmarshal(Dbt *data) const {
    char *bytes = (char *)data->get_data();
    uint offset = 0;
    uint columnNumber = 0;

    ValueDict *row = new ValueDict();
    for (auto const &columnName : this->column_names) {
        ColumnAttribute ca = this->column_attributes[columnNumber++];
        auto type = ca.get_data_type();
        if (type == ColumnAttribute::INT) {
            int32_t n;
            auto size = sizeof(int32_t);
            memcpy(&n, bytes + offset, size);
            row->insert({columnName, Value(n)});
            offset += size;
        } else if (type == ColumnAttribute::TEXT) {
            u_int16_t len;
            auto size = sizeof(u_int16_t);
            memcpy(&len, bytes + offset, size);
            offset += size;
            std::string s(bytes + offset, len);
            row->insert({columnName, Value(s)});
            offset += len;
        } else {
            throw DbRelationError("Only supports unmarshaling INT and TEXT");
        }
    }

    return row;
}

