/**
 * @file heap_storage.cpp
 * @author K Lundeen
 * @see Seattle University, CPSC5300
 */

#include "heap_storage.h"
#include <cstring>
#pragma GCC diagnostic ignored "-Wpointer-arith"
#pragma GCC diagnostic ignored "-Wdelete-incomplete"

using u16 = u_int16_t;
using namespace std;

//==============================[SlottedPage]==============================

/**
 * SlottedPage constructor
 * @param block
 * @param block_id
 * @param is_new
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

/**
 * Add a new record to the block.
 * @param data
 * @return the new block's id
 */
RecordID SlottedPage::add(const Dbt *data) {
    if (!has_room((u16) data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1U;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

/**
 * Get a record from the block.
 * @param record_id
 * @return the bits of the record as stored in the block, or nullptr if it has been deleted (freed by caller)
 */
Dbt *SlottedPage::get(RecordID record_id) const {
    u16 size, loc;
    get_header(size, loc, record_id);
    if (loc == 0)
        return nullptr;  // this is just a tombstone, record has been deleted
    return new Dbt(this->address(loc), size);
}

/**
 * Replace the record with the given data.
 * @param record_id   record to replace
 * @param data        new contents of record_id
 * @throws DbBlockNoRoomError if it won't fit
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16) data.get_size();
    if (new_size > size) {
        u16 extra = new_size - size;
        if (!has_room(extra))
            throw DbBlockNoRoomError("not enough room for enlarged record");
        slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}

/**
 * Delete a record from the page.
 *
 * Mark the given id as deleted by changing its size to zero and its location to 0.
 * Compact the rest of the data in the block. But keep the record ids the same for everyone.
 *
 * @param record_id  record to delete
 */
void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);  // 0 is the tombstone sentinel
    slide(loc, loc + size);
}

/**
 * Sequence of all non-deleted record IDs.
 * @return  sequence of IDs (freed by caller)
 */
RecordIDs *SlottedPage::ids(void) const {
    RecordIDs *vec = new RecordIDs();
    u16 size, loc;
    for (RecordID record_id = 1; record_id <= this->num_records; record_id++) {
        get_header(size, loc, record_id);
        if (loc != 0)
            vec->push_back(record_id);
    }
    return vec;
}

/**
 * Get the size and offset for given id. For id of zero, it is the block header.
 * @param size  set to the size from given header
 * @param loc   set to the byte offset from given header
 * @param id    the id of the header to fetch
 */
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) const {
    size = get_n((u16) 4 * id);
    loc = get_n((u16) (4 * id + 2));
}

/**
 * Store the size and offset for given id. For id of zero, store the block header.
 * @param id
 * @param size
 * @param loc
 */
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u16) 4 * id, size);
    put_n((u16) (4 * id + 2), loc);
}

/**
 * Calculate if we have room to store a record with given size. The size should include the 4 bytes
 * for the header, too, if this is an add.
 * @param size   size of the new record (not including the header space needed)
 * @return       true if there is enough room, false otherwise
 */
bool SlottedPage::has_room(u16 size) const {
    return 4 * (this->num_records + 1) + size <= this->end_free;
}

/**
 * Slide the contents to compensate for a smaller/larger record.
 *
 * If start < end, then remove data from offset start up to but not including offset end by sliding data
 * that is to the left of start to the right. If start > end, then make room for extra data from end to start
 * by sliding data that is to the left of start to the left.
 * Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
 * shift (end < start).
 *
 * @param start  beginning of slide
 * @param end    end of slide
 */
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    int shift = end - start;
    if (shift == 0)
        return;

    // slide data
    void *to = this->address((u16) (this->end_free + 1 + shift));
    void *from = this->address((u16) (this->end_free + 1));
    int bytes = start - (this->end_free + 1U);
    memmove(to, from, bytes);

    // fix up headers to the right
    RecordIDs *record_ids = ids();
    for (auto const &record_id : *record_ids) {
        u16 size, loc;
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

/**
 * Get 2-byte integer at given offset in block.
 */
u16 SlottedPage::get_n(u16 offset) const {
    return *(u16 *) this->address(offset);
}

/**
 * Put a 2-byte integer at given offset in block.
 * @param offset number of bytes into the page
 * @param n
 */
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16 *) this->address(offset) = n;
}

/**
 * Make a void* pointer for a given offset into the data block.
 * @param offset
 * @return
 */
void *SlottedPage::address(u16 offset) const {
    return (void *) ((char *) this->block.get_data() + offset);
}



//==============================[HeapFile]==============================


/**
 * Constructor
 * @param name
 */
HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
    this->dbfilename = this->name + ".db";
}

/**
 * Create physical file.
 */
void HeapFile::create(void) {
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *page = get_new(); // force one page to exist
    delete page;
}

/**
 * Delete the physical file.
 */
void HeapFile::drop(void) {
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

/**
 * Open physical file.
 */
void HeapFile::open(void) {
    db_open();
}

/**
 * Close the physical file.
 */
void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

/**
 * Allocate a new block for the database file.
 * @return the new empty DbBlock that is managing the records in this block and its block id.
 */
SlottedPage *HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization done to it
    delete page;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, this->last);
}

/**
 * Get a block from the database file.
 * @param block_id
 * @return          the given slotted page (freed by caller)
 */
SlottedPage *HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}

/**
 * Write a block back to the database file.
 * @param block
 */
void HeapFile::put(DbBlock *block) {
    int block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

/**
 * Sequence of all block ids.
 * @return block ids
 */
BlockIDs *HeapFile::block_ids() const {
    BlockIDs *vec = new BlockIDs();
    for (BlockID block_id = 1; block_id <= this->last; block_id++)
        vec->push_back(block_id);
    return vec;
}

/**
 * Ask BerkDb how many blocks we are currently using in the file.
 * @return number of blocks
 */
uint32_t HeapFile::get_block_count() {
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    uint32_t bt_ndata = stat->bt_ndata;
    free(stat);
    return bt_ndata;
}

/**
 * Wrapper for Berkeley DB open, which does both open and creation.
 * @param flags BerkDb flags
 */
void HeapFile::db_open(uint flags) {
    if (!this->closed)
        return;
    this->db.set_re_len(DbBlock::BLOCK_SZ); // record length - will be ignored if file already exists
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);

    this->last = flags ? 0 : get_block_count();
    this->closed = false;
}


//==============================[HeapTable]==============================

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(
        table_name, column_names, column_attributes), file(table_name) {
}

/**
 * Execute: CREATE TABLE <table_name> ( <columns> )
 * Is not responsible for metadata storage or validation.
 */
void HeapTable::create() {
    file.create();
}

/**
 * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
 * Is not responsible for metadata storage or validation.
 */
void HeapTable::create_if_not_exists() {
    try {
        open();
    } catch (DbException &e) {
        create();
    }
}

/**
 * Execute: DROP TABLE <table_name>
 */
void HeapTable::drop() {
    file.drop();
}

/**
 * Open existing table. Enables: insert, update, delete, select, project
 */
void HeapTable::open() {
    file.open();
}

/**
 * Closes the table. Disables: insert, update, delete, select, project
 */
void HeapTable::close() {
    file.close();
}

/**
 * Execute: INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
 * @param row a dictionary with column name keys
 * @return the handle of the inserted row
 */
Handle HeapTable::insert(const ValueDict *row) {
    open();
    ValueDict *full_row = validate(row);
    Handle handle = append(full_row);
    delete full_row;
    return handle;
}

/**
 * Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <handle>
 * where handle is sufficient to identify one specific record (e.g., returned from an insert
 * or select).
 * @param handle the row to be updated
 * @param new_values a dictionary with column name keys
 */
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw DbRelationError("Not implemented");
}

/**
 * Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
 * where handle is sufficient to identify one specific record (e.g., returned from an insert
 * or select).
 * @param handle the row to be deleted
 */
void HeapTable::del(const Handle handle) {
    open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
}

/**
 * Conceptually, execute: SELECT <handle> FROM <table_name> WHERE 1
 * @return a list of handles for qualifying rows
 */
Handles *HeapTable::select() {
    return select(nullptr);
}

/**
 * The select command
 * @param where ignored for now FIXME
 * @return list of handles of the selected rows
 */
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

/**
 * Project all columns from a given row.
 * @param handle row to be projected
 * @return a sequence of all values for handle
 */
ValueDict *HeapTable::project(Handle handle) {
    return project(handle, &this->column_names);
}

/**
 * Project given columns from a given row.
 * @param handle row to be projected
 * @param column_names of columns to be included in the result
 * @return a sequence of values for handle given by column_names
 */
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

/**
 * Check if the given row is acceptable to insert.
 * @param row to be validated
 * @return the full row dictionary
 * @throws DbRelationError if not valid
 */
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

/**
 * Appends a record to the file.
 * @param row to be appended
 * @return handle of newly inserted row
 */
Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
        record_id = block->add(data);
    } catch (DbBlockNoRoomError &e) {
        // need a new block
        delete block;
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    delete block;
    delete[] (char *) data->get_data();
    delete data;
    return Handle(this->file.get_last_block_id(), record_id);
}

/**
 * Figure out the bits to go into the file.
 * The caller is responsible for freeing the returned Dbt and its enclosed ret->get_data().
 * @param row data for the tuple
 * @return bits of the record as it should appear on disk
 */
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
            *(u16 *) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
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

/**
 * Figure out the memory data structures from the given bits gotten from the file.
 * @param data file data for the tuple
 * @return row data for the tuple
 */
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
            u16 size = *(u16 *) (bytes + offset);
            offset += sizeof(u16);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = string(buffer);  // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    return row;
}

/**
 * See if the row at the given handle satisfies the given where clause
 * @param handle  row to check
 * @param where   conditions to check
 * @return        true if conditions met, false otherwise
 */
bool HeapTable::selected(Handle handle, const ValueDict *where) {
    if (where == nullptr)
        return true;
    ValueDict *row = this->project(handle, where);
    return *row == *where;
}

ValueDict* HeapTable::project(Handle handle, const ValueDict *where) {
    ColumnNames t;
    for (auto const &column: *where)
        t.push_back(column.first);
    return this->project(handle, &t);
}
