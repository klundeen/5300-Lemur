/**
 * @file: test_heap_storage.cpp - Test entry points for HeapFile, HeapTable, and SlottedPage
 * @author Duc Vo
 * 
 * @see "Seattle University, CPSC 5300, Winter Quarter 2024"
*/
#include "heap_storage.h"

#include <stdio.h>
#include <stdlib.h>
#include <cstring>

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
    if (nullptr != tombstoneRecord) {
        return false;
    }

    delete tombstoneRecord;
    printf("Records: ");
    u_int32_t expected_sizes[] = {0, 42, 0, 50, 18, 77};
    for (RecordID &id : *recordIDs) {
        Dbt *record = page.get(id);
        if (nullptr == record) {
            continue;
        }
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

bool test_heap_table() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "Table Creation OK" << std::endl;
    table1.drop();
    std::cout << "Table Drop OK" << std::endl;
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "Table Create If Not Exist OK" << std::endl;
    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    table.insert(&row);
    std::cout << "Table Insertion OK" << std::endl;
    Handles *handles = table.select();
    std::cout << "Table Selection OK" << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "Table Projection OK" << std::endl;

    delete handles;

    int32_t n = (*result)["a"].n;
    std::string s = (*result)["b"].s;
    if (n != 12 || s != "Hello!") return false;
    delete result;

    return true;
}


bool test_heap_storage() {
    char s[] = "==============================";
    printf("%s\nTESTING SLOTTED PAGE...\n", s);
    bool passed = true;
    passed &= test_slotted_page();
    printf("PASSED!\n");

    printf("%s\nTESTING HEAP FILE...\n", s);
    passed &= test_heap_file();
    printf("PASSED!\n");

    printf("%s\nTESTING HEAP TABLE...\n", s);
    passed &= test_heap_table();
    printf("PASSED!\n");

    return passed;
}
