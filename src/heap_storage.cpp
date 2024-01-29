#include "heap_storage.h"
#include "slotted_page.cpp"
#include "heap_file.cpp"
#include "heap_table.cpp"

bool test_heap_storage()
{
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
