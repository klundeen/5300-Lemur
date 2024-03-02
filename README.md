# 5300-Lemur
Student DB Relation Manager project for CPSC5300 at Seattle U, Winter 2024

## Installation
**Requirements:** [Berkeley DB](https://docs.oracle.com/cd/E17076_05/html/api_reference/CXX/frame_main.html), [SQLParser](https://github.com/klundeen/sql-parser)   
*You will need to build these requirements on your local machine or use existing one running on Seattle Univeristy CS1 server*

**Clone 5300-Lemur DB Relation Manager Project**
```bash
git clone https://github.com/klundeen/5300-Lemur.git
```

Build and run the program using makefile

```bash
make
./sql5300 <dbenv-dir-path>
```
*Make sure you have the db enviroment directory created.   
For example, ``~/cpsc5300/data``



## Usage
The program will start a SQL shell where you can run SQL commands to create, select records.
At this point, the program will parsed the input SQL commands using SQLParser 
and return the valid parsed SQL statsment.

```sql
SQL> create table foo (a text, b integer, c double)
>>>> CREATE TABLE foo (a TEXT, b INT, c DOUBLE)

SQL> select a,b,g.c from foo as f, goo as g
>>>> SELECT a, b, g.c FROM goo AS g, foo AS f
```

To run automated test cases, use the following command. Both heap storage class 
and shell sql parser will be tested.

```bash
SQL> test

TESTING SQL PARSER...
SQL> select * from foo left join goober on foo.x=goober.x
>>>> SELECT * FROM foo LEFT JOIN goober ON foo.x = goober.x
...

TESTING SLOTTED PAGE...
Insert record #1 of size 42
Insert record #2 of size 100
Insert record #3 of size 59
Delete record #2
Insert record #4 of size 14
Insert record #5 of size 77
Update record #3 changing size to 50
Update record #4 changing size to 18
Records: [1:42][3:50][4:18][5:77]
PASSED!
==============================
TESTING HEAP FILE...
File Creation OK
File Open OK
File Get New Block OK
File Get Existing Block OK
File Put Block OK
File Block IDs OK
File Close OK
File Drop OK
PASSED!
==============================
TESTING HEAP TABLE...
Table Creation OK
Table Drop OK
Table Create If Not Exist OK
Table Insertion OK
Table Selection OK9
Table Projection OK
PASSED!
test_heap_storage: OK
```

To exit the program, type `quit` and press enter.

```sql
SQL> quit 
```

To test Milestone 3 and 4, run through the example that professor gave in the Milestone 4 assignment page. Requirements 1-4 are satisfied in this project.

## Tools

<a href="https://github.com/klundeen/5300-Lemur">
	<img src="https://skillicons.dev/icons?i=cpp,git,bash" />
</a>
