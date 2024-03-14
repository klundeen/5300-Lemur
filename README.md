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
test_heap_storage:
slotted page tests ok
create ok
drop ok
create_if_not_exists ok
insert ok
select/project ok 1
many inserts/select/projects ok
del ok
ok
test_btree: splitting leaf 2, new sibling 3 starting at value 211
new root: (interior block 4): 2|211|3
splitting leaf 3, new sibling 5 starting at value 324
splitting leaf 5, new sibling 6 starting at value 437
splitting leaf 6, new sibling 7 starting at value 550
splitting leaf 7, new sibling 8 starting at value 663
splitting leaf 8, new sibling 9 starting at value 776
splitting leaf 9, new sibling 10 starting at value 889
splitting leaf 10, new sibling 11 starting at value 1002
splitting leaf 11, new sibling 12 starting at value 1115
splitting leaf 12, new sibling 13 starting at value 1228
splitting leaf 13, new sibling 14 starting at value 1341
splitting leaf 14, new sibling 15 starting at value 1454
splitting leaf 15, new sibling 16 starting at value 1567
splitting leaf 16, new sibling 17 starting at value 1680
splitting leaf 17, new sibling 18 starting at value 1793
splitting leaf 18, new sibling 19 starting at value 1906
splitting leaf 19, new sibling 20 starting at value 2019
splitting leaf 20, new sibling 21 starting at value 2132
splitting leaf 21, new sibling 22 starting at value 2245
splitting leaf 22, new sibling 23 starting at value 2358
splitting leaf 23, new sibling 24 starting at value 2471
splitting leaf 24, new sibling 25 starting at value 2584
splitting leaf 25, new sibling 26 starting at value 2697
splitting leaf 26, new sibling 27 starting at value 2810
splitting leaf 27, new sibling 28 starting at value 2923
splitting leaf 28, new sibling 29 starting at value 3036
splitting leaf 29, new sibling 30 starting at value 3149
splitting leaf 30, new sibling 31 starting at value 3262
splitting leaf 31, new sibling 32 starting at value 3375
splitting leaf 32, new sibling 33 starting at value 3488
splitting leaf 33, new sibling 34 starting at value 3601
splitting leaf 34, new sibling 35 starting at value 3714
splitting leaf 35, new sibling 36 starting at value 3827
splitting leaf 36, new sibling 37 starting at value 3940
splitting leaf 37, new sibling 38 starting at value 4053
splitting leaf 38, new sibling 39 starting at value 4166
splitting leaf 39, new sibling 40 starting at value 4279
splitting leaf 40, new sibling 41 starting at value 4392
splitting leaf 41, new sibling 42 starting at value 4505
splitting leaf 42, new sibling 43 starting at value 4618
splitting leaf 43, new sibling 44 starting at value 4731
splitting leaf 44, new sibling 45 starting at value 4844
splitting leaf 45, new sibling 46 starting at value 4957
splitting leaf 46, new sibling 47 starting at value 5070
splitting leaf 47, new sibling 48 starting at value 5183
splitting leaf 48, new sibling 49 starting at value 5296
splitting leaf 49, new sibling 50 starting at value 5409
splitting leaf 50, new sibling 51 starting at value 5522
splitting leaf 51, new sibling 52 starting at value 5635
splitting leaf 52, new sibling 53 starting at value 5748
splitting leaf 53, new sibling 54 starting at value 5861
splitting leaf 54, new sibling 55 starting at value 5974
splitting leaf 55, new sibling 56 starting at value 6087
splitting leaf 56, new sibling 57 starting at value 6200
splitting leaf 57, new sibling 58 starting at value 6313
splitting leaf 58, new sibling 59 starting at value 6426
splitting leaf 59, new sibling 60 starting at value 6539
splitting leaf 60, new sibling 61 starting at value 6652
splitting leaf 61, new sibling 62 starting at value 6765
splitting leaf 62, new sibling 63 starting at value 6878
splitting leaf 63, new sibling 64 starting at value 6991
splitting leaf 64, new sibling 65 starting at value 7104
splitting leaf 65, new sibling 66 starting at value 7217
splitting leaf 66, new sibling 67 starting at value 7330
splitting leaf 67, new sibling 68 starting at value 7443
splitting leaf 68, new sibling 69 starting at value 7556
splitting leaf 69, new sibling 70 starting at value 7669
splitting leaf 70, new sibling 71 starting at value 7782
splitting leaf 71, new sibling 72 starting at value 7895
splitting leaf 72, new sibling 73 starting at value 8008
splitting leaf 73, new sibling 74 starting at value 8121
splitting leaf 74, new sibling 75 starting at value 8234
splitting leaf 75, new sibling 76 starting at value 8347
splitting leaf 76, new sibling 77 starting at value 8460
splitting leaf 77, new sibling 78 starting at value 8573
splitting leaf 78, new sibling 79 starting at value 8686
splitting leaf 79, new sibling 80 starting at value 8799
splitting leaf 80, new sibling 81 starting at value 8912
splitting leaf 81, new sibling 82 starting at value 9025
splitting leaf 82, new sibling 83 starting at value 9138
splitting leaf 83, new sibling 84 starting at value 9251
splitting leaf 84, new sibling 85 starting at value 9364
splitting leaf 85, new sibling 86 starting at value 9477
splitting leaf 86, new sibling 87 starting at value 9590
splitting leaf 87, new sibling 88 starting at value 9703
splitting leaf 88, new sibling 89 starting at value 9816
splitting leaf 89, new sibling 90 starting at value 9929
lookup test passed!
ok
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
