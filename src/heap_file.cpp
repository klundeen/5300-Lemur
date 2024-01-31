/**
 * @file heap_file.cpp - Implementation of HeapFile class
 * Operations: Create, Drop, Open, Close, Get New, Get, Put, Block IDs
 * @author Duc Vo
 *
 * @see "Seattle University, CPSC 5300, Winter Quarter 2024"
 */
#include <string.h>

#include <cstring>

#include "db_cxx.h"
#include "heap_storage.h"

void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *page = this->get_new();  // get a new block to start the file
    this->put(page);
    delete page;
    this->closed = false;
}

void HeapFile::drop(void) {
    this->close();
    _DB_ENV->dbremove(NULL, dbfilename.c_str(), NULL, 0);
    this->last = 0;
}

void HeapFile::open(void) {
    if (!this->closed) return;
    this->db_open();
    this->closed = false;
}

void HeapFile::close(void) {
    if (this->closed) return;
    db.close(0);
    this->closed = true;
}

SlottedPage *HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    BlockID block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    this->db.put(nullptr, &key, &data, 0);
    this->db.get(nullptr, &key, &data, 0);

    return new SlottedPage(data, this->last, true);
}

SlottedPage *HeapFile::get(BlockID block_id) {
    Dbt data;
    Dbt key(&block_id, sizeof(block_id));
    this->db.get(nullptr, &key, &data, 0);

    return new SlottedPage(data, block_id);
}

void HeapFile::put(DbBlock *block) {
    BlockID block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));

    db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs *HeapFile::block_ids() {
    BlockIDs *blockIds = new BlockIDs();
    for (RecordID i = 1; i <= this->last; i++) blockIds->push_back(i);

    return blockIds;
}

void HeapFile::db_open(uint flags) {
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

bool test_heap_file() {
    const char *fileName = "lemur";
    HeapFile file(fileName);
    file.create();
    printf("File Creation OK\n");

    file.open();
    printf("File Open OK\n");

    if (file.get_last_block_id() != 1) return false;

    SlottedPage *page = file.get_new();
    if (file.get_last_block_id() != 2) return false;
    delete page;
    printf("File Get New Block OK\n");

    page = file.get(2);
    delete page;
    printf("File Get Existing Block OK\n");

    page = file.get(1);
    char value[] = "cpsc5300";
    Dbt data(&value, sizeof(value));
    page->add(&data);
    file.put(page);
    delete page;
    printf("File Put Block OK\n");

    page = file.get(1);
    delete page;

    BlockIDs *blockIds = file.block_ids();
    if (blockIds->size() != 2) return false;
    delete blockIds;
    printf("File Block IDs OK\n");

    file.close();
    printf("File Close OK\n");

    file.drop();
    printf("File Drop OK\n");

    return true;
}
