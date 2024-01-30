/**
 * @file heap_storage.cpp - Test driver for heap_storage
 * @author Duc Vo
 */
#include "heap_storage.h"

#include <cstring>
#include <map>
#include <string>

#include "heap_file.cpp"
#include "heap_table.cpp"
#include "slotted_page.cpp"

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
