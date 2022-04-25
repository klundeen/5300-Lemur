/* heap_storage.cpp
 * Diego Hoyos, Erika Skornia-Olsen
 * CPSC 4300/5300 Spring 2022
 * References: Milestone 2h, heap_storage.py
 * Oracle Berkeley DB Release 18.1
 * www.geeksforgeeks.org/vector-in-cpp-stl/
 */

#include "heap_storage.h"
#include <cstring>
#include <memory.h>
using namespace std;

typedef u_int16_t u16;

// Constructor
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// Get record from the block, return nullptr if doesn't exist
Dbt *SlottedPage::get(RecordID record_id) {
    u16 size;
    u16 loc; 
    get_header(size, loc, record_id);

    if (size == 0) return nullptr;

    char* data = new char(size);

    memcpy(data, this->address(loc), size);
    
    return new Dbt(this->address(loc), size);

}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u_int16_t offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

// Update record with new data
void SlottedPage::put(RecordID record_id, const Dbt &data){
    u16 size;
    u16 loc;
    get_header(size, loc, record_id);
    u16 newSize = data.get_size();
    
    if (newSize > size) {
        u16 extra = newSize - size;
        if (!has_room(extra))
            throw DbBlockNoRoomError("not enough room for updated record");
        slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), newSize);
    } else {
        memcpy(this->address(loc), data.get_data(), newSize);
        slide(loc + newSize, loc + size);
    }
    
    get_header(size, loc, record_id);
    put_header(record_id, newSize, loc);
}

// Deletes record by setting size/loc to 0 and shifting free data
void SlottedPage::del(RecordID record_id) {
    u16 size;
    u16 loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

// Returns pointer to a vector of recordIDs
RecordIDs *SlottedPage::ids(void) {
    RecordIDs* record_ids = new RecordIDs();
    u16 size;
    u16 loc;
    for (RecordID record_id = 1; record_id < this->num_records + 1; record_id++) {
        get_header(size, loc, record_id);
        if (size > 0)
            record_ids->push_back(record_id);
    }
    return record_ids;
}

// Get size and offset for recordID
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

// Calculate if there's room to store record of given size
bool SlottedPage::has_room(u_int16_t size) {
    u16 available = this->end_free - ((this->num_records + 2) * 4);
    return size <= available;
}

// Moves data to accommodate a smaller or larger data input
void SlottedPage::slide(u_int16_t start, u_int16_t end){
    int shift = end - start;
    if (shift == 0)
        return;
    int newSize = start - (this->end_free + 1);
    memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_free + 1), newSize);
    RecordIDs* record_ids = ids();
    for (auto const& record_id : *record_ids) {
        u16 size;
        u16 loc;
        get_header(size, loc, record_id);
        if (loc <= start) {
            loc += shift;
            put_header(record_id, size, loc);
        }
    }
    delete record_ids;
    this->end_free += shift;
    put_header();
}


//************************************HEAPFILE************************************

// Create file
void HeapFile::create(void) {
    db_open(DB_CREATE | DB_EXCL);
    put(get_new());
}

// Delete file
void HeapFile::drop(void) {
    this->close();
    remove(dbfilename.c_str());
}

// Open file
void HeapFile::open(void) {
    db_open();
}

// Close file
void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

SlottedPage *HeapFile::get(BlockID block_id) {
    Dbt data;
    Dbt key(&block_id, sizeof(block_id));
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id);
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

// Write block back to database file
void HeapFile::put(DbBlock* block) {
    u16 block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), DB_NOOVERWRITE);
}

// Return pointer to vector of block ids
BlockIDs* HeapFile::block_ids() {
    BlockIDs* block_ids = new BlockIDs();
    for (BlockID block_id = 1; block_id <= this->last; block_id++)
        block_ids->push_back(block_id);
    return block_ids;
}


void HeapFile::db_open(uint flags) {
    if (!this->closed)
        return;
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    DBTYPE dbT = DB_RECNO;
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, dbT, flags, 0);
    this->closed = false;
}

//************************************HEAPTABLE************************************
// Constructor
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names,column_attributes), file(table_name) {

}

// Create table
void HeapTable::create() {
    file.create();
}

// Open existing table
void HeapTable::open() {
    file.open();
}

// Close table
void HeapTable::close() {
    file.close();
}

// Create table if doesn't exist
void HeapTable::create_if_not_exists() {
    try {
        file.open();
    } catch (...){
        file.create();
    }
}

// Drop table
void HeapTable::drop() {
    file.drop();
}

// Return handle of inserted row
Handle HeapTable::insert(const ValueDict *row) {
    open();
    return append(validate(row));
}

// Update values by finding handle
void HeapTable::update(const Handle handle, const ValueDict* new_values) {
    
}

// Delete values from table
void HeapTable::del(const Handle handle) {
    open();
    BlockID block_id = get<0>(handle);
    RecordID record_id = get<1>(handle);
    SlottedPage* block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
}

// Return column names for handle
ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
   open();
    BlockID block_id = get<0>(handle);
    RecordID record_id = get<1>(handle);
    SlottedPage* block = file.get(block_id);
    Dbt* data = block->get(record_id);
    ValueDict* row = unmarshal(data);
    delete data;
    delete block;
    if (column_names->empty())
        return row;
    ValueDict* result = new ValueDict();
    for (uint i = 0; i < result->size(); i++)
        result[i] = row[i];
    delete row;
    return result;
}

// Check if given row can accept insert; if yes, return full row
ValueDict* HeapTable::validate(const ValueDict* row) {
    ValueDict* full_row = new ValueDict();
    for (auto const& column_name: this->column_names) {
        if (row->find(column_name) != row->end()) {
            Value value = row->find(column_name)->second;
            (*full_row)[column_name] = value;
        }
        else
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
    }
    return full_row;
}

// Assume row is fleshed out, append record to file
Handle HeapTable::append(const ValueDict* row) {
    Dbt* data = marshal(row);
    BlockID block_id = this->file.get_last_block_id();
    SlottedPage* block = this->file.get(block_id);
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbBlockNoRoomError& e) {
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    delete data;
    delete block;
    return Handle(block_id, record_id);
}

// Return the bits to go into the file
// Caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }

    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

// Convert data back to values
ValueDict* HeapTable::unmarshal(Dbt* data) {
    ValueDict* row = new ValueDict();
    Value value;
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t*)((char*)data->get_data() + offset);
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16*)((char*)data->get_data() + offset);
            offset += sizeof(u16);
            char* buffer = new char[size];
            memcpy(buffer, (char*)data->get_data() + offset, size);
            value.s = string(buffer);
            offset += size;
            delete[] buffer;
        } else {
            throw DbRelationError("only know how to unmarshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    return row;
}

// Return handles for rows requested by select
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

bool test_heap_storage() {return true;}
