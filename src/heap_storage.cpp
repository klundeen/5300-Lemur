#include "heap_storage.h"
#include <cstring>
bool test_heap_storage() {return true;}

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
void* SlottedPage::address(u16 offset) {
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
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0) {
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

// Calculate if there's room to store record of given size
bool SlottedPage::has_room(u_int16_t size) {
    u16 available = this->end_free - ((this->num_records + 2) * 4);
    return size <= available;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end){
    //TODO
}

void *SlottedPage::address(u_int16_t offset) {
    //TODO
}


//************************************HEAPFILE************************************

HeapFile::HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {

}

void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage* block = this->get_new();
    this->put(block);
}

void HeapFile::drop(void) {
    this->close();
    remove(dbfilename.c_str());
}

void HeapFile::open(void) {
    this->db_open();
    //TODO
}

void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

SlottedPage *HeapFile::get(BlockID block_id) {
    return SlottedPage()
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

void HeapFile::put(DbBlock *block) {

}

BlockIDs *HeapFile::block_ids() {

}

void HeapFile::db_open(uint flags = 0) {

}



//************************************HEAPTABLE************************************

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names,column_attributes), file(table_name) {

}

void HeapTable::create() {
    file.create();
}

void HeapTable::open() {
    file.open();
}

void HeapTable::close() {
    file.close();
}

void HeapTable::create_if_not_exists() {
    try {
        file.open();
    } catch (...){
        file.create();
    }
}

void HeapTable::drop() {
    file.drop();
}

Handle HeapTable::insert(const ValueDict *row) {
    //TODO but we'll only handle two data types for now, INTEGER (or INT) and TEXT
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
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