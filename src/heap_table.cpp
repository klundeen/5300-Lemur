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
    DEBUG_OUT("HeapTable::create() - begin\n");
    this->file.create(); 
    DEBUG_OUT("HeapTable::create() - end\n");
}

void HeapTable::create_if_not_exists() {
    DEBUG_OUT("HeapTable::create_if_not_exists() - begin\n");
    try {
        DEBUG_OUT("HeapTable::create_if_not_exists() - try\n");
        this->file.open();
    } catch (DbException &e) {  // DBNoSuchFileError
        DEBUG_OUT("HeapTable::create_if_not_exists() - catch\n");
        this->create();
    }
    DEBUG_OUT("HeapTable::create_if_not_exists() - end\n");
}

void HeapTable::drop() {
    DEBUG_OUT("HeapTable::drop() - begin\n");
    this->create_if_not_exists();
    this->file.close();
    this->file.drop();
    DEBUG_OUT("HeapTable::drop() - end\n");
}

void HeapTable::open() {
    DEBUG_OUT("HeapTable::open() - begin\n");
    this->file.open();
    DEBUG_OUT("HeapTable::open() - end\n");
}

void HeapTable::close() {
    DEBUG_OUT("HeapTable::close() - begin\n");
    this->file.close();
    DEBUG_OUT("HeapTable::close() - end\n");
}

Handle HeapTable::insert(const ValueDict *row) {
    DEBUG_OUT("HeapTable::insert() - begin\n");
    this->open();
    ValueDict *validatedRow = this->validate(row);
    Handle handle = this->append(validatedRow);
    delete validatedRow;
    DEBUG_OUT_VAR("HeapTable::insert() - end (block: %d, record: %d)\n", handle.first, handle.second);
    return handle;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw DbRelationError("Not implemented");
}

void HeapTable::del(const Handle handle) {
    DEBUG_OUT_VAR("HeapTable::del(block: %d, record: %d)\n", handle.first, handle.second);
    open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);

#ifdef DEBUG_ENABLED
    DEBUG_OUT_VAR("Records in block %d: ( ", block_id);
    RecordIDs *record_ids = block->ids();
    for (auto const &record_id: *record_ids) {
        printf("%d ", record_id);
    }
    printf(")\n");
#endif

    block->del(record_id);
    this->file.put(block);

#ifdef DEBUG_ENABLED
    DEBUG_OUT_VAR("Records remaining in block %d: ( ", block_id);
    record_ids = block->ids();
    for (auto const &record_id: *record_ids) {
        printf("%d ", record_id);
    }
    printf(")\n");
#endif

    delete block;
    DEBUG_OUT("HeapTable::del() - end\n");
}

Handles *HeapTable::select() {
    return select(nullptr);
}

Handles *HeapTable::select(const ValueDict *where) {
    DEBUG_OUT("HeapTable::select() - begin\n");
    open();
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id: *block_ids) {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id: *record_ids) {
            Handle handle(block_id, record_id);
            if (selected(handle, where))
            {
                DEBUG_OUT_VAR("selected block_id: %d record_id: %d\n", block_id, record_id);
                handles->push_back(Handle(block_id, record_id));
            }
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    DEBUG_OUT("HeapTable::select() - end\n");
    return handles;
}

ValueDict *HeapTable::project(Handle handle) {
    DEBUG_OUT("HeapTable::project(handle) - begin/end\n");
    return this->project(handle, &this->column_names);
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    delete data;
    delete block;
    if (column_names->empty())
        return row;
    ValueDict *result = new ValueDict();
    for (auto const &column_name: *column_names) {
        if (row->find(column_name) == row->end())
            throw DbRelationError("table does not have column named '" + column_name + "'");
        (*result)[column_name] = (*row)[column_name];
    }
    delete row;
    return result;
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
    DEBUG_OUT("HeapTable::append() - begin\n");
    Dbt *data = marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        DEBUG_OUT("HeapTable::append() - try\n");
        record_id = block->add(data);
    } catch (DbBlockNoRoomError &e) {
        DEBUG_OUT("HeapTable::append() - catch\n");
        // need a new block
        delete block;
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    delete block;
    delete[] (char *) data->get_data();
    delete data;
    DEBUG_OUT("HeapTable::append() - end\n");
    return Handle(this->file.get_last_block_id(), record_id);
}

Dbt *HeapTable::marshal(const ValueDict *row) const {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;

        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            if (offset + 4 > DbBlock::BLOCK_SZ - 4)
                throw DbRelationError("row too big to marshal");
            *(int32_t *) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u_long size = value.s.length();
            if (size > UINT16_MAX)
                throw DbRelationError("text field too long to marshal");
            if (offset + 2 + size > DbBlock::BLOCK_SZ)
                throw DbRelationError("row too big to marshal");
            *(uint16_t *) (bytes + offset) = size;
            offset += sizeof(uint16_t);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            if (offset + 1 > DbBlock::BLOCK_SZ - 1)
                throw DbRelationError("row too big to marshal");
            *(uint8_t *) (bytes + offset) = (uint8_t) value.n;
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to marshal INT, TEXT, and BOOLEAN");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict *HeapTable::unmarshal(Dbt *data) const {
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char *) data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t *) (bytes + offset);
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint16_t size = *(uint16_t *) (bytes + offset);
            offset += sizeof(uint16_t);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = std::string(buffer);  // assume ascii for now
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            value.n = *(uint8_t *) (bytes + offset);
            offset += sizeof(uint8_t);
        } else {
            throw DbRelationError("Only know how to unmarshal INT, TEXT, and BOOLEAN");
        }
        (*row)[column_name] = value;
    }
    return row;
}

bool HeapTable::selected(Handle handle, const ValueDict *where) {
    if (where == nullptr) {
        return true;
    }
    ValueDict *row = this->project(handle, where);
    bool is_selected = *row == *where;
    delete row;
    return is_selected;
}
