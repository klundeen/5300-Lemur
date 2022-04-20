#include "storage_engine.h"

DbBlock::DbBlock(Dbt &block, BlockID block_id, bool is_new = false) : block(block), block_id(block_id) {
    
}

DbBlock::~DbBlock() {

}

RecordID DbBlock::add(const Dbt *data) {

}

Dbt *DbBlock::get(RecordID record_id) {

}

void DbBlock::del(RecordID record_id) {

}

void DbBlock::put(RecordID record_id, const Dbt &data) {

}

RecordIDs *DbBlock::ids(){

}


DbFile::DbFile(std::string name) : name(name) {

}

void DbFile::create() {

}

void DbFile::drop() {
    
}

void DbFile::open() {

}

void DbFile::close() {

}

DbBlock *DbFile::get_new() {

}

DbBlock *DbFile::get(BlockID block_id) {

}


void DbFile::put(DbBlock *block) {

}

BlockIDs *DbFile::block_ids() {
    
}

DbRelation::DbRelation(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : table_name(
            table_name), column_names(column_names), column_attributes(column_attributes) {

}

DbRelation::~DbRelation() {

}

void DbRelation::create() {
    
}

void DbRelation::create_if_not_exists() {
    
}

void DbRelation::drop() {

}

void DbRelation::open() {
    
}

void DbRelation::close() {
    
}

Handle DbRelation::insert(const ValueDict *row) {
    
}

void DbRelation::update(const Handle handle, const ValueDict *new_values) {
    
}

void DbRelation::del(const Handle handle) {
    
}

Handles *DbRelation::select() {
    
}

Handles *DbRelation::select(const ValueDict *where) {
    
}

ValueDict *DbRelation::project(Handle handle) {
    
}

ValueDict *DbRelation::project(Handle handle, const ColumnNames *column_names) {
    
}