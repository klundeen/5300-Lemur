////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
/////////////////////// H E A P - F I L E //////////////////////////////////
////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////
#pragma once
#include "heap_storage.h"
#include <cstring>

void HeapFile::create(void)
{
    printf("HeapFile::create()\n");
    db_open(DB_CREATE | DB_TRUNCATE);
    SlottedPage *page = this->get_new(); // get a new block to start the file
    this->put(page);
    closed = false;
}

void HeapFile::drop(void)
{
    printf("HeapFile::drop()\n");
    // open();
    close();
    _DB_ENV->dbremove(NULL, dbfilename.c_str(), NULL, 0);
    last = 0;
}

void HeapFile::open(void)
{
    printf("HeapFile::open()\n");
    if (!closed)
        return;
    db_open(0);
    DB_BTREE_STAT stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    this->last = stat.bt_ndata;
}

void HeapFile::close(void)
{
    printf("HeapFile::close()\n");
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
    if (this->dbfilename.empty())
        this->dbfilename = this->name + ".db";
    db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);
}

void test_heap_file()
{
    printf("test_heap_file\n");
    HeapFile file("test_table");

    file.create();
    file.open();
    assert(file.get_last_block_id() == 1);
    file.get_new();
    assert(file.get_last_block_id() == 2);

    SlottedPage *block1 = file.get(1);
    SlottedPage *block2 = file.get(2);

    void *before = malloc(DbBlock::BLOCK_SZ);
    memcpy(before, file.get(1)->get_data(), DbBlock::BLOCK_SZ);

    SlottedPage block1_new(*block2->get_block(), 1, false);
    char value[] = "cpsc5300";
    Dbt data(&value, sizeof(value));
    block1_new.add(&data);
    file.put(&block1_new); // put a copy of block2 into block1
    void *after = file.get(1)->get_data();
    assert(memcmp(before, after, DbBlock::BLOCK_SZ) != 0);

    BlockIDs *blockIds = file.block_ids();
    assert(blockIds->size() == 2);
    delete blockIds;

    file.close();
    file.drop();
}