# 5300-Lemur
Kyle Fraser and Anh Tran's DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2022

Usage (argument is database directory):
<pre>
$ ./sql5300 ~/cpsc5300/data
</pre>

## Tags
- <code>Milestone5</code> has the instructor-provided files for Milestone5.
We use this to get started. We implemented 
in SQLExec.cpp the select functionality, 
insert functionality, and delete functionality.
- <code>Milestone6</code> has the instructor-provided files for Milestone6. We
used the functions from Milestone 5 to further
implement Milestone 6. We implemented the 
lookup function in btree.cpp. The insert 
function was provided for us. 

Issues:
- If an exception is thrown during the creation process the index is not correctly deleted. This is not due to our implementation, but an issue with the Milestone 6 prep. 

- Due to the limited main memory of CS1 server we were only able to run 65,000 nodes into the B+-tree and not the intended 100,000.

Changes:
- We changed the find_eq() function in BTreeLeaf implementation to use map_key.find()
to check if key was present before using map_key.at() to find the data. 

## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
```

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

## Usage (Linux)
To build the program, enter:
<br />
$ make

To run the tests for this program, enter: 
<br />
$ ./sql5300 ~/cpsc5300/data 

To perform a clean run, execute: 
<br />
$ make clean
<br />

