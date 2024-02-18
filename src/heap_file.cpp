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

// #define DEBUG_ENABLED
#include "debug.h"

using namespace std;

HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
    this->dbfilename = this->name + ".db";
}

void HeapFile::create(void) {
    DEBUG_OUT("HeapFile::create() - begin\n");
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *page = this->get_new();  // get a new block to start the file
    this->put(page);
    delete page;
    this->closed = false;
    DEBUG_OUT("HeapFile::create() - end\n");
}

void HeapFile::drop(void) {
    DEBUG_OUT("HeapFile::drop() - begin\n");
    this->close();
    _DB_ENV->dbremove(NULL, dbfilename.c_str(), NULL, 0);
    this->last = 0;
    DEBUG_OUT("HeapFile::drop() - end\n");
}

void HeapFile::open(void) {
    DEBUG_OUT("HeapFile::open() - begin\n");
    if (!this->closed){
        return;
    }
    this->db_open();
    this->closed = false;
    DEBUG_OUT("HeapFile::open() - end\n");
}

void HeapFile::close(void) {
    if (this->closed) {
        return;
    }
    db.close(0);
    this->closed = true;
}

SlottedPage *HeapFile::get_new(void) {
    DEBUG_OUT("HeapFile::get_new() - begin\n");
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    BlockID block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0);
    delete page;
    this->db.get(nullptr, &key, &data, 0);

    DEBUG_OUT("HeapFile::get_new() - end\n");
    return new SlottedPage(data, this->last, true);
}

SlottedPage *HeapFile::get(BlockID block_id) {
    DEBUG_OUT("HeapFile::get() - begin\n");
    Dbt data;
    Dbt key(&block_id, sizeof(block_id));
    this->db.get(nullptr, &key, &data, 0);

    DEBUG_OUT("HeapFile::get() - end\n");
    return new SlottedPage(data, block_id);
}

void HeapFile::put(DbBlock *block) {
    DEBUG_OUT("HeapFile::put() - begin\n");
    BlockID block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));

    db.put(nullptr, &key, block->get_block(), 0);
    DEBUG_OUT("HeapFile::put() - end\n");
}

BlockIDs *HeapFile::block_ids() const {
    BlockIDs *blockIds = new BlockIDs();
    for (RecordID i = 1; i <= this->last; i++) blockIds->push_back(i);

    return blockIds;
}

uint32_t HeapFile::get_block_count() {
    DEBUG_OUT("HeapFile::get_block_count() - begin\n");
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    uint32_t bt_ndata = stat->bt_ndata;
    free(stat);
    DEBUG_OUT("HeapFile::get_block_count() - end\n");
    return bt_ndata;
}

void HeapFile::db_open(uint flags) {
    DEBUG_OUT("HeapFile::db_open() - begin\n");
    if (!this->closed)
    {
        DEBUG_OUT("HeapFile::db_open() - !this->closed\n");
        return;
    }

    DEBUG_OUT_VAR("HeapFile::db_open() - attempting open on %s\n", this->dbfilename.c_str());
    this->db.set_re_len(DbBlock::BLOCK_SZ); // record length - will be ignored if file already exists
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);

    this->last = flags ? 0 : get_block_count();
    this->closed = false;
    DEBUG_OUT("HeapFile::db_open() - end\n");
}
