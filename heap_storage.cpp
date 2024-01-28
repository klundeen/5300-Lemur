#include "heap_storage.h"
#include <cstring>
#include <cassert>

using namespace std;

string dbttos(const Dbt *data);
Dbt stodbt(std::string s);

bool test_slotted_page()
{
    // Initialize SlottedPage
    char bytes[DbBlock::BLOCK_SZ];
    memset(bytes, 0, sizeof(bytes));
    Dbt block(bytes, sizeof(bytes));
    SlottedPage page(block, 1, true);

    // Test Addition SlottedPage::add

    string hello = "HELLO";
    Dbt data = stodbt(hello);
    
    RecordID id = page.add(&data);
    
    string res = dbttos(page.get(id));
    assert(strcmp(res.c_str(), hello.c_str()) == 0);
    printf("PASS\n");

    string cpsc5300 = "CPSC5300";
    Dbt data2 = stodbt(cpsc5300);
    
    RecordID id2 = page.add(&data2);
    
    res = dbttos(page.get(id2));
    assert(strcmp(res.c_str(), cpsc5300.c_str()) == 0);
    printf("PASS\n");

    // Test Replacement SlottedPage::put

    string goodbye = "GOODBYE";
    char value2[goodbye.length() + 1];
    strcpy(value2, goodbye.c_str());
    Dbt data3(value2, sizeof(value2));
    
    page.put(id, data3);

    res = dbttos(page.get(id));
    assert((res == goodbye));
    printf("PASS\n");
    res = dbttos(page.get(id2));
    assert((res == cpsc5300));
    printf("PASS\n");

    // Test Iteration SlottedPage::ids

    auto ids = page.ids();
    assert(ids->size() == 2 && ids->at(0) == 1 && ids->at(1) == 2);
    printf("PASS\n");
    
    // Test Deletion SlottedPage::del

    page.del(id);
    res = dbttos(page.get(id));
    assert(res == "");
    printf("PASS\n");
    res = dbttos(page.get(id2));
    assert(res == cpsc5300);
    printf("PASS\n");

    // More Tests
     
    Dbt data4 = stodbt("uis aute irure dolor in ");
    Dbt data5 = stodbt("reprehenderit in voluptate");
    Dbt data6 = stodbt("lorem ipsum dolor sit amet");
    Dbt data7 = stodbt("proident, sunt in culpa qui");
    Dbt data8 = stodbt("HELLO AGAIN");
    Dbt data9 = stodbt("deserunt mollit anim id est laborum");
    page.add(&data4);
    RecordID id5 = page.add(&data5);
    page.add(&data6);
    page.add(&data7);
    page.put(id5, data8); 

    return true;
}

bool test_heap_storage()
{
    return test_slotted_page();
}

string dbttos(const Dbt *data)
{
    char *bytes = (char *)data->get_data();
    if (!data->get_size()) return "";
    string s(bytes, data->get_size() - 1);
    return s;
}

Dbt stodbt(std::string s)
{
    char value[s.length() + 1];
    strcpy(value, s.c_str());
    Dbt data(value, sizeof(value));
    return data;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
////////////////////// S L O T T E D  - P A G E //////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
    if (is_new)
    {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    }
    else
    {
        get_header(this->num_records, this->end_free);
    }
}

RecordID SlottedPage::add(const Dbt *data)
{
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u_int16_t id = ++this->num_records;
    u_int16_t size = (u_int16_t) data->get_size();
    this->end_free -= size;
    u_int16_t loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memmove(this->address(loc), data->get_data(), size);
    // cout << "id=" << id << " size=" << size << " loc=" << loc << "data=" << dbttos(data) << endl;
    return id;
}

Dbt *SlottedPage::get(RecordID record_id)
{
    u_int16_t size, loc;
    get_header(size, loc, record_id);
    return new Dbt(this->address(loc), size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data)
{
    u_int16_t size, loc;
    this->get_header(size, loc, record_id);
    u_int16_t new_size = data.get_size();

    if (new_size > size)
    {
        u_int16_t extra = new_size - size;
        if (!this->has_room(extra))
            throw DbBlockNoRoomError("not enough room for new record");
        this->slide(loc, loc - extra);
        memmove(this->address(loc - extra), data.get_data(), new_size);
    }
    else
    {
        memmove(this->address(loc), data.get_data(), loc + size);
        this->slide(loc + new_size, loc + size);
    }

    this->get_header(size, loc, record_id);
    this->put_header(record_id, new_size, loc);
}

/**
 * Delete a record from this block.
 * @param record_id  which record to delete
 */
void SlottedPage::del(RecordID record_id)
{
    u_int16_t size, loc;
    this->get_header(size, loc, record_id);
    this->slide(loc, loc + size);
    this->put_header(record_id, 0, 0);
}

/**
 * Get all the record ids in this block (excluding deleted ones).
 * @returns  pointer to list of record ids (freed by caller)
 */
RecordIDs *SlottedPage::ids(void)
{
    RecordIDs *recordIDs = new RecordIDs();
    for (int id = 1; id <= this->num_records; id++)
    {
        u_int16_t size, loc;
        this->get_header(size, loc, id);
        if (!loc && !size)
            continue;
        recordIDs->push_back(id);
    }

    return recordIDs;
}

bool SlottedPage::has_room(u_int16_t size)
{
    u_int16_t available = this->end_free - (this->num_records + 2) * 4;
    return size <= available;
}

/**
 * Move data within the block to make room for new or expanded data or to close up space after slicing data.
 * If start < end, then remove data from offset start up to but not including offset end by sliding data that is to the left of start to the right.
 * If start > end, then make room for extra data from end to start by sliding data that is to the left of start to the left.
 * Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left shift (end < start).
 */
void SlottedPage::slide(u_int16_t start, u_int16_t end)
{
    // start 294, end 290
    // shift = -4
    u_int16_t shift = end - start;
    if (shift == 0)
        return;

    u_int16_t data_loc = this->end_free + 1;
    memmove(this->address(data_loc + shift), address(data_loc), start - data_loc);
    for (RecordID &id : *ids())
    {
        u_int16_t size, loc;
        this->get_header(size, loc, id);
        if (loc <= start)
            this->put_header(id, size, loc + shift);
    }
    this->end_free += shift;
    this->put_header();
}

// Get 2-byte integer at given offset in block.
u_int16_t SlottedPage::get_n(u_int16_t offset)
{
    return *(u_int16_t *)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u_int16_t offset, u_int16_t n)
{
    *(u_int16_t *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u_int16_t offset)
{
    return (void *)((char *)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc)
{
    if (id == 0)
    { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id)
{
    if (id == 0)
    { // called the get_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
///////////////////////// H E A P - F I L E //////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

void HeapFile::create(void)
{
    printf("HeapFile::create()\n");
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *page = this->get_new(); // get a new block to start the file
    this->put(page);
}

void HeapFile::drop(void)
{
    printf("HeapFile::drop()\n");
    open();
    close();
    _DB_ENV->dbremove(NULL, dbfilename.c_str(), NULL, 0);
    last = 0;
}

void HeapFile::open(void)
{
    printf("HeapFile::open()\n");
    db_open(DB_CREATE | DB_EXCL);
}

void HeapFile::close(void)
{
    printf("HeapFile::close()\n");
    if (closed)
        return;
    db.close(0);
    closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage *HeapFile::get_new(void)
{
    printf("HeapFile::get_new()\n");

    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    BlockID block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);

    return page;
}

SlottedPage *HeapFile::get(BlockID block_id)
{
    printf("HeapFile::get(%d)\n", block_id);

    char block[DbBlock::BLOCK_SZ];

    Dbt data(block, sizeof(block));
    Dbt key(&block_id, sizeof(block_id));

    this->db.get(nullptr, &key, &data, 0);

    return new SlottedPage(data, this->last);
}

void HeapFile::put(DbBlock *block)
{
    printf("HeapFile::put(%d)\n", block->get_block_id());

    BlockID block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));

    db.put(nullptr, &key, block->get_block(), DB_APPEND);
}

BlockIDs *HeapFile::block_ids()
{
    BlockIDs *blockIds = new BlockIDs(last);
    for (RecordID i = 1; i <= last; i++)
        blockIds->push_back(i);

    return blockIds;
}

void HeapFile::db_open(uint flags)
{
    printf("HeapFile::db_open()\n");
    if (!closed)
        return;
    dbfilename = name + ".db";

    db.open(NULL, dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);
    closed = false;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
///////////////////////// H E A P  T A B L E /////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name)
{
}

void HeapTable::create()
{
    this->file.create();
}

void HeapTable::create_if_not_exists()
{
    try
    {
        this->file.open();
    }
    catch (DbException &e)
    { // DBNoSuchFileError
        this->create();
    }
}

void HeapTable::drop()
{
    this->file.drop();
}

void HeapTable::open()
{
    this->file.open();
}

void HeapTable::close()
{
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row)
{
    return Handle();
}

void HeapTable::update(const Handle handle, const ValueDict *new_values)
{
}

void HeapTable::del(const Handle handle)
{
}

Handles *HeapTable::select()
{
    return nullptr;
}

Handles *HeapTable::select(const ValueDict *where)
{
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id : *block_ids)
    {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict *HeapTable::project(Handle handle)
{
    return nullptr;
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    return nullptr;
}

ValueDict *HeapTable::validate(const ValueDict *row)
{
    return nullptr;
}

Handle HeapTable::append(const ValueDict *row)
{
    return Handle();
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt *HeapTable::marshal(const ValueDict *row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            *(int32_t *)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            uint size = value.s.length();
            *(u_int16_t *)(bytes + offset) = size;
            offset += sizeof(u_int16_t);
            memmove(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        }
        else
        {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memmove(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict *HeapTable::unmarshal(Dbt *data)
{
    return nullptr;
}