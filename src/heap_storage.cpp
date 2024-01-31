/**
 * @file: heap_storage.cpp - Test entry points for HeapFile, HeapTable, and SlottedPage
 * @author Duc Vo
 * 
 * @see "Seattle University, CPSC 5300, Winter Quarter 2024"
*/
#include "heap_storage.h"

#include <stdio.h>
#include <stdlib.h>

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
