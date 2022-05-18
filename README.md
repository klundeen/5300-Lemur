# 5300-Lemur

Milestone 1:

    Contributors 
        Erika Skornia-Olsen
        Diego Hoyos

    a program written in C++ that runs from the command line and prompts the user for SQL statements and then executes them one at a time.  For now, all it does is take SQL statements, parse them, and print out the parse tree.

    To build: $ make

    to run: $./sql5300 ~/cpsc5300/data

Milestone 2:

    Contributors 
        Erika Skornia-Olsen
        Diego Hoyos
    
    a storage engine based on a heap file organization.  SlottedPage class, HeapFile class, and HeapTable class has been implemented.

    To build: $ make

    to run: $./sql5300 ~/cpsc5300/data

    to test: type test when prompted

Milestone 3 & 4: Hand-off video: https://youtu.be/5IkYaAGlpvw

    Contributors 
        Luan (Remi) Ta
        Preedhi Garg
    
    SQL parser underlined by a heap-file storage system - supporting SQL commands: CREATE TABLE, DROP TABLE, SHOW TABLE, SHOW COLUMNS, CREATE INDEX, DROP INDEX, SHOW INDEX.

    Clients of this program must expect leaks in the underlying storage solution as such leaks are part of the provided code we implemented our parser upon.

    Clients of this program should draw attention to changes detailed in our hand-off video. Specifically, a deviation from the provided header files of milestone 4 includes keeping the table singleton rather than implementing it as a class of static methods.

    Clients of this program must not deallocate COLUMN_NAMES and COLUMN_ATTRIBUTES. Accept the "leaks" since the lifetime of these objects are the same as the lifetime of the program. However, as they are static instances, deallocation will prove troublesome and is not recommended.

    To build: $ make

    to run: $./sql5300 ~/cpsc5300/data

    to test: use provided test cases in assignment prompt



    
