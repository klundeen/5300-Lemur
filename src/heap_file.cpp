////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
/////////////////////// H E A P - F I L E //////////////////////////////////
////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
#pragma once
#include "heap_storage.h"
#include <cstring>

void HeapFile::create(void)
{
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *page = this->get_new(); // get a new block to start the file
    this->put(page);
    delete page;
    closed = false;
}

void HeapFile::drop(void)
{
    close();
    _DB_ENV->dbremove(NULL, dbfilename.c_str(), NULL, 0);
    last = 0;
}

void HeapFile::open(void)
{
    if (!closed)
        return;
    db_open();
    closed = false;
}

void HeapFile::close(void)
{
    if (closed)
        return;
    db.close(0);
    closed = true;
}

SlottedPage *HeapFile::get_new(void)
{
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
    Dbt data;
    Dbt key(&block_id, sizeof(block_id));
    this->db.get(nullptr, &key, &data, 0);

    return new SlottedPage(data, block_id);
}

void HeapFile::put(DbBlock *block)
{
    BlockID block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));

    db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs *HeapFile::block_ids()
{
    BlockIDs *blockIds = new BlockIDs();
    for (RecordID i = 1; i <= this->last; i++)
        blockIds->push_back(i);

    return blockIds;
}

void HeapFile::db_open(uint flags)
{
    this->db.set_message_stream(_DB_ENV->get_message_stream());
    this->db.set_error_stream(_DB_ENV->get_error_stream());
    this->db.set_re_len(DbBlock::BLOCK_SZ);

    this->dbfilename = this->name + ".db";
    db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);

    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    uint32_t bt_ndata = stat->bt_ndata;
    this->last = bt_ndata;
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//////////////////////////////// T E S T //////////////////////////////////////
////////////////////////// H E A P - F I L E //////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
bool test_heap_file()
{
    HeapFile file("test_table");

    file.create();
    printf("Create file\n");

    file.open();
    printf("Open file\n");

    if (file.get_last_block_id() != 1)
    {
        return false;
    }

    file.get_new();
    printf("Get new block\n");
    if (file.get_last_block_id() != 2)
    {
        return false;
    }

    SlottedPage *block2 = file.get(2);
    printf("Get existing block\n");

    void *before = malloc(DbBlock::BLOCK_SZ);
    memcpy(before, file.get(1)->get_block(), DbBlock::BLOCK_SZ);

    SlottedPage block1_new(*block2->get_block(), 1, true);
    char value[] = "cpsc5300";
    Dbt data(&value, sizeof(value));
    block1_new.add(&data);
    file.put(&block1_new);
    printf("Put block\n");
    void *after = file.get(1)->get_block();
    if (memcmp(before, after, DbBlock::BLOCK_SZ) == 0)
    {
        return false;
    }

    BlockIDs *blockIds = file.block_ids();
    if (blockIds->size() != 2)
    {
        return false;
    }

    delete blockIds;
    file.close();
    printf("Close file\n");

    file.drop();
    printf("Drop file\n");

    return true;
}