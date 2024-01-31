#include <cstring>

#include "heap_storage.h"

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new)
    : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

RecordID SlottedPage::add(const Dbt *data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u_int16_t id = ++this->num_records;
    u_int16_t size = (u_int16_t)data->get_size();
    this->end_free -= size;
    u_int16_t loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memmove(this->address(loc), data->get_data(), size);
    return id;
}

Dbt *SlottedPage::get(RecordID record_id) {
    u_int16_t size, loc;
    get_header(size, loc, record_id);
    return new Dbt(this->address(loc), size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u_int16_t size, loc;
    this->get_header(size, loc, record_id);
    u_int16_t new_size = (u_int16_t)data.get_size();
    int16_t extra = new_size - size;

    if (extra > 0) {
        if (!this->has_room(extra))
            throw DbBlockNoRoomError("not enough room for new record");
        this->slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        this->slide(loc + new_size, loc + size);
    }

    this->get_header(size, loc, record_id);
    this->put_header(record_id, new_size, loc);
}

/**
 * Delete a record from this block.
 * @param record_id  which record to delete
 */
void SlottedPage::del(RecordID record_id) {
    u_int16_t size, loc;
    this->get_header(size, loc, record_id);
    this->slide(loc, loc + size);
    this->put_header(record_id, 0, 0);
}

/**
 * Get all the record ids in this block (excluding deleted ones).
 * @returns  pointer to list of record ids (freed by caller)
 */
RecordIDs *SlottedPage::ids(void) {
    RecordIDs *recordIDs = new RecordIDs();
    for (int id = 1; id <= this->num_records; id++) {
        u_int16_t size, loc;
        this->get_header(size, loc, id);
        if (!loc && !size) continue;
        recordIDs->push_back(id);
    }

    return recordIDs;
}

bool SlottedPage::has_room(u_int16_t size) {
    u_int16_t available = this->end_free - (this->num_records + 2) * 4;
    return size <= available;
}

/**
 * Move data within the block to make room for new or expanded data or to close
 * up space after slicing data. If start < end, then remove data from offset
 * start up to but not including offset end by sliding data that is to the left
 * of start to the right. If start > end, then make room for extra data from end
 * to start by sliding data that is to the left of start to the left. Also fix
 * up any record headers whose data has slid. Assumes there is enough room if it
 * is a left shift (end < start).
 */
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    // start 294, end 290
    // shift = -4
    int16_t shift = end - start;
    if (shift == 0) return;

    u_int16_t data_loc = this->end_free + 1;
    memmove(this->address(data_loc + shift), address(data_loc), abs(shift));
    RecordIDs *ids = this->ids();
    for (RecordID &id : *ids) {
        u_int16_t size, loc;
        this->get_header(size, loc, id);
        if (loc <= start) this->put_header(id, size, loc + shift);
    }
    delete ids;
    this->end_free += shift;
    this->put_header();
}

// Get 2-byte integer at given offset in block.
u_int16_t SlottedPage::get_n(u_int16_t offset) {
    return *(u_int16_t *)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {
    *(u_int16_t *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u_int16_t offset) {
    return (void *)((char *)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block
// header.
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
    if (id ==
        0) {  // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    if (id ==
        0) {  // called the get_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

bool test_slotted_page() {
    auto createDbt = [](u_int16_t len) {
        char bytes[len];
        Dbt data(&bytes, len);
        return data;
    };

    // Initialize SlottedPage
    char bytes[DbBlock::BLOCK_SZ];
    memset(bytes, 0, sizeof(bytes));
    Dbt block(bytes, sizeof(bytes));
    SlottedPage page(block, 1, true);

    // Test Addition
    Dbt data = createDbt(42);
    page.add(&data);
    printf("Insert record #1 of size 42\n");

    data = createDbt(100);
    RecordID id2 = page.add(&data);
    printf("Insert record #2 of size 100\n");

    data = createDbt(59);
    RecordID id3 = page.add(&data);
    printf("Insert record #3 of size 59\n");

    // Test Deletion
    page.del(id2);
    printf("Delete record #2\n");

    data = createDbt(14);
    RecordID id4 = page.add(&data);
    printf("Insert record #4 of size 14\n");

    data = createDbt(77);
    page.add(&data);
    printf("Insert record #5 of size 77\n");

    // Test Update (shrink)
    data = createDbt(50);
    page.put(id3, data);
    printf("Update record #3 changing size to 50\n");

    // Test Update (expand)
    data = createDbt(18);
    page.put(id4, data);
    printf("Update record #4 changing size to 18\n");

    // Test IDs
    RecordIDs *recordIDs = page.ids();
    if (recordIDs->size() != 4) {
        return false;
    };

    Dbt *tombstoneRecord = page.get(id2);
    if (tombstoneRecord->get_size() != 0) {
        return false;
    }
    delete tombstoneRecord;
    printf("Records: ");
    u_int32_t expected_sizes[] = {0, 42, 0, 50, 18, 77};
    for (RecordID &id : *recordIDs) {
        Dbt *record = page.get(id);
        u_int32_t recordSize = record->get_size();
        delete record;
        printf("[%d:%d]", id, recordSize);
        if (recordSize != expected_sizes[id]) {
            return false;
        }
    }
    printf("\n");

    delete recordIDs;

    return true;
}
