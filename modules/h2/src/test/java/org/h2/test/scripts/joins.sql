-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table a(a int) as select 1;
> ok

create table b(b int) as select 1;
> ok

create table c(c int) as select x from system_range(1, 2);
> ok

select * from a inner join b on a=b right outer join c on c=a;
> C A    B
> - ---- ----
> 1 1    1
> 2 null null
> rows: 2

select * from c left outer join (a inner join b on b=a) on c=a;
> C A    B
> - ---- ----
> 1 1    1
> 2 null null
> rows: 2

select * from c left outer join a on c=a inner join b on b=a;
> C A B
> - - -
> 1 1 1
> rows: 1

drop table a, b, c;
> ok

create table test(a int, b int) as select x, x from system_range(1, 100);
> ok

-- the table t1 should be processed first
explain select * from test t2, test t1 where t1.a=1 and t1.b = t2.b;
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T2.A, T2.B, T1.A, T1.B FROM PUBLIC.TEST T1 /* PUBLIC.TEST.tableScan */ /* WHERE T1.A = 1 */ INNER JOIN PUBLIC.TEST T2 /* PUBLIC.TEST.tableScan */ ON 1=1 WHERE (T1.A = 1) AND (T1.B = T2.B)
> rows: 1

explain select * from test t1, test t2 where t1.a=1 and t1.b = t2.b;
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.A, T1.B, T2.A, T2.B FROM PUBLIC.TEST T1 /* PUBLIC.TEST.tableScan */ /* WHERE T1.A = 1 */ INNER JOIN PUBLIC.TEST T2 /* PUBLIC.TEST.tableScan */ ON 1=1 WHERE (T1.A = 1) AND (T1.B = T2.B)
> rows: 1

drop table test;
> ok

create table test(id identity) as select x from system_range(1, 4);
> ok

select a.id from test a inner join test b on a.id > b.id and b.id < 3 group by a.id;
> ID
> --
> 2
> 3
> 4
> rows: 3

drop table test;
> ok

select * from system_range(1, 3) t1 inner join system_range(2, 3) t2 inner join system_range(1, 2) t3 on t3.x=t2.x on t1.x=t2.x;
> X X X
> - - -
> 2 2 2
> rows: 1

CREATE TABLE PARENT(ID INT PRIMARY KEY);
> ok

CREATE TABLE CHILD(ID INT PRIMARY KEY);
> ok

INSERT INTO PARENT VALUES(1);
> update count: 1

SELECT * FROM PARENT P LEFT OUTER JOIN CHILD C ON C.PARENTID=P.ID;
> exception

DROP TABLE PARENT, CHILD;
> ok

create table t1 (i int);
> ok

create table t2 (i int);
> ok

create table t3 (i int);
> ok

select a.i from t1 a inner join (select a.i from t2 a inner join (select i from t3) b on a.i=b.i) b on a.i=b.i;
> I
> -
> rows: 0

insert into t1 values (1);
> update count: 1

insert into t2 values (1);
> update count: 1

insert into t3 values (1);
> update count: 1

select a.i from t1 a inner join (select a.i from t2 a inner join (select i from t3) b on a.i=b.i) b on a.i=b.i;
> I
> -
> 1
> rows: 1

drop table t1, t2, t3;
> ok

CREATE TABLE TESTA(ID IDENTITY);
> ok

CREATE TABLE TESTB(ID IDENTITY);
> ok

explain SELECT TESTA.ID A, TESTB.ID B FROM TESTA, TESTB ORDER BY TESTA.ID, TESTB.ID;
> PLAN
> ------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TESTA.ID AS A, TESTB.ID AS B FROM PUBLIC.TESTA /* PUBLIC.TESTA.tableScan */ INNER JOIN PUBLIC.TESTB /* PUBLIC.TESTB.tableScan */ ON 1=1 ORDER BY 1, 2
> rows (ordered): 1

DROP TABLE IF EXISTS TESTA, TESTB;
> ok

create table one (id int primary key);
> ok

create table two (id int primary key, val date);
> ok

insert into one values(0);
> update count: 1

insert into one values(1);
> update count: 1

insert into one values(2);
> update count: 1

insert into two values(0, null);
> update count: 1

insert into two values(1, DATE'2006-01-01');
> update count: 1

insert into two values(2, DATE'2006-07-01');
> update count: 1

insert into two values(3, null);
> update count: 1

select * from one;
> ID
> --
> 0
> 1
> 2
> rows: 3

select * from two;
> ID VAL
> -- ----------
> 0  null
> 1  2006-01-01
> 2  2006-07-01
> 3  null
> rows: 4

-- Query #1: should return one row
-- okay
select * from one natural join two left join two three on
one.id=three.id left join one four on two.id=four.id where three.val
is null;
> ID VAL  ID VAL  ID
> -- ---- -- ---- --
> 0  null 0  null 0
> rows: 1

-- Query #2: should return one row
-- okay
select * from one natural join two left join two three on
one.id=three.id left join one four on two.id=four.id where
three.val>=DATE'2006-07-01';
> ID VAL        ID VAL        ID
> -- ---------- -- ---------- --
> 2  2006-07-01 2  2006-07-01 2
> rows: 1

-- Query #3: should return the union of #1 and #2
select * from one natural join two left join two three on
one.id=three.id left join one four on two.id=four.id where three.val
is null or three.val>=DATE'2006-07-01';
> ID VAL        ID VAL        ID
> -- ---------- -- ---------- --
> 0  null       0  null       0
> 2  2006-07-01 2  2006-07-01 2
> rows: 2

explain select * from one natural join two left join two three on
one.id=three.id left join one four on two.id=four.id where three.val
is null or three.val>=DATE'2006-07-01';
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT ONE.ID, TWO.VAL, THREE.ID, THREE.VAL, FOUR.ID FROM PUBLIC.ONE /* PUBLIC.ONE.tableScan */ INNER JOIN PUBLIC.TWO /* PUBLIC.PRIMARY_KEY_14: ID = PUBLIC.ONE.ID AND ID = PUBLIC.ONE.ID */ ON 1=1 /* WHERE PUBLIC.ONE.ID = PUBLIC.TWO.ID */ LEFT OUTER JOIN PUBLIC.TWO THREE /* PUBLIC.PRIMARY_KEY_14: ID = ONE.ID */ ON ONE.ID = THREE.ID LEFT OUTER JOIN PUBLIC.ONE FOUR /* PUBLIC.PRIMARY_KEY_1: ID = TWO.ID */ ON TWO.ID = FOUR.ID WHERE (PUBLIC.ONE.ID = PUBLIC.TWO.ID) AND ((THREE.VAL IS NULL) OR (THREE.VAL >= DATE '2006-07-01'))
> rows: 1

-- Query #4: same as #3, but the joins have been manually re-ordered
-- Correct result set, same as expected for #3.
select * from one natural join two left join one four on
two.id=four.id left join two three on one.id=three.id where three.val
is null or three.val>=DATE'2006-07-01';
> ID VAL        ID ID VAL
> -- ---------- -- -- ----------
> 0  null       0  0  null
> 2  2006-07-01 2  2  2006-07-01
> rows: 2

drop table one;
> ok

drop table two;
> ok

create table test1 (id int primary key);
> ok

create table test2 (id int primary key);
> ok

create table test3 (id int primary key);
> ok

insert into test1 values(1);
> update count: 1

insert into test2 values(1);
> update count: 1

insert into test3 values(1);
> update count: 1

select * from test1
inner join test2 on test1.id=test2.id left
outer join test3 on test2.id=test3.id
where test3.id is null;
> ID ID ID
> -- -- --
> rows: 0

explain select * from test1
inner join test2 on test1.id=test2.id left
outer join test3 on test2.id=test3.id
where test3.id is null;
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST1.ID, TEST2.ID, TEST3.ID FROM PUBLIC.TEST2 /* PUBLIC.TEST2.tableScan */ LEFT OUTER JOIN PUBLIC.TEST3 /* PUBLIC.PRIMARY_KEY_4C0: ID = TEST2.ID */ ON TEST2.ID = TEST3.ID INNER JOIN PUBLIC.TEST1 /* PUBLIC.PRIMARY_KEY_4: ID = TEST2.ID */ ON 1=1 WHERE (TEST3.ID IS NULL) AND (TEST1.ID = TEST2.ID)
> rows: 1

insert into test1 select x from system_range(2, 1000);
> update count: 999

select * from test1
inner join test2 on test1.id=test2.id
left outer join test3 on test2.id=test3.id
where test3.id is null;
> ID ID ID
> -- -- --
> rows: 0

explain select * from test1
inner join test2 on test1.id=test2.id
left outer join test3 on test2.id=test3.id
where test3.id is null;
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST1.ID, TEST2.ID, TEST3.ID FROM PUBLIC.TEST2 /* PUBLIC.TEST2.tableScan */ LEFT OUTER JOIN PUBLIC.TEST3 /* PUBLIC.PRIMARY_KEY_4C0: ID = TEST2.ID */ ON TEST2.ID = TEST3.ID INNER JOIN PUBLIC.TEST1 /* PUBLIC.PRIMARY_KEY_4: ID = TEST2.ID */ ON 1=1 WHERE (TEST3.ID IS NULL) AND (TEST1.ID = TEST2.ID)
> rows: 1

SELECT TEST1.ID, TEST2.ID, TEST3.ID
FROM TEST2
LEFT OUTER JOIN TEST3 ON TEST2.ID = TEST3.ID
INNER JOIN TEST1
WHERE TEST3.ID IS NULL AND TEST1.ID = TEST2.ID;
> ID ID ID
> -- -- --
> rows: 0

drop table test1;
> ok

drop table test2;
> ok

drop table test3;
> ok

create table left_hand (id int primary key);
> ok

create table right_hand (id int primary key);
> ok

insert into left_hand values(0);
> update count: 1

insert into left_hand values(1);
> update count: 1

insert into right_hand values(0);
> update count: 1

-- h2, postgresql, mysql, derby, hsqldb: 2
select * from left_hand left outer join right_hand on left_hand.id=right_hand.id;
> ID ID
> -- ----
> 0  0
> 1  null
> rows: 2

-- h2, postgresql, mysql, derby, hsqldb: 2
select * from left_hand left join right_hand on left_hand.id=right_hand.id;
> ID ID
> -- ----
> 0  0
> 1  null
> rows: 2

-- h2: 1 (2 cols); postgresql, mysql: 1 (1 col); derby, hsqldb: no natural join
select * from left_hand natural join right_hand;
> ID
> --
> 0
> rows: 1

-- h2, postgresql, mysql, derby, hsqldb: 1
select * from left_hand left outer join right_hand on left_hand.id=right_hand.id where left_hand.id=1;
> ID ID
> -- ----
> 1  null
> rows: 1

-- h2, postgresql, mysql, derby, hsqldb: 1
select * from left_hand left join right_hand on left_hand.id=right_hand.id where left_hand.id=1;
> ID ID
> -- ----
> 1  null
> rows: 1

-- h2: 0 (2 cols); postgresql, mysql: 0 (1 col); derby, hsqldb: no natural join
select * from left_hand natural join right_hand where left_hand.id=1;
> ID
> --
> rows: 0

-- !!! h2: 1; postgresql, mysql, hsqldb: 0; derby: exception
select * from left_hand left outer join right_hand on left_hand.id=right_hand.id where left_hand.id=1 having right_hand.id=2;
> ID ID
> -- --
> rows: 0

-- !!! h2: 1; postgresql, mysql, hsqldb: 0; derby: exception
select * from left_hand left join right_hand on left_hand.id=right_hand.id where left_hand.id=1 having right_hand.id=2;
> ID ID
> -- --
> rows: 0

-- h2: 0 (2 cols); postgresql: 0 (1 col), mysql: exception; derby, hsqldb: no natural join
select * from left_hand natural join right_hand where left_hand.id=1 having right_hand.id=2;
> exception

-- h2, mysql, hsqldb: 0 rows; postgresql, derby: exception
select * from left_hand left outer join right_hand on left_hand.id=right_hand.id where left_hand.id=1 group by left_hand.id having right_hand.id=2;
> ID ID
> -- --
> rows: 0

-- h2, mysql, hsqldb: 0 rows; postgresql, derby: exception
select * from left_hand left join right_hand on left_hand.id=right_hand.id where left_hand.id=1 group by left_hand.id having right_hand.id=2;
> ID ID
> -- --
> rows: 0

-- h2: 0 rows; postgresql, mysql: exception; derby, hsqldb: no natural join
select * from left_hand natural join right_hand where left_hand.id=1 group by left_hand.id having right_hand.id=2;
> ID
> --
> rows: 0

drop table right_hand;
> ok

drop table left_hand;
> ok

--- complex join ---------------------------------------------------------------------------------------------
CREATE TABLE T1(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE TABLE T2(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE TABLE T3(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO T1 VALUES(1, 'Hello');
> update count: 1

INSERT INTO T1 VALUES(2, 'World');
> update count: 1

INSERT INTO T1 VALUES(3, 'Peace');
> update count: 1

INSERT INTO T2 VALUES(1, 'Hello');
> update count: 1

INSERT INTO T2 VALUES(2, 'World');
> update count: 1

INSERT INTO T3 VALUES(1, 'Hello');
> update count: 1

SELECT * FROM t1 left outer join t2 on t1.id=t2.id;
> ID NAME  ID   NAME
> -- ----- ---- -----
> 1  Hello 1    Hello
> 2  World 2    World
> 3  Peace null null
> rows: 3

SELECT * FROM t1 left outer join t2 on t1.id=t2.id left outer join t3 on t1.id=t3.id;
> ID NAME  ID   NAME  ID   NAME
> -- ----- ---- ----- ---- -----
> 1  Hello 1    Hello 1    Hello
> 2  World 2    World null null
> 3  Peace null null  null null
> rows: 3

SELECT * FROM t1 left outer join t2 on t1.id=t2.id inner join t3 on t1.id=t3.id;
> ID NAME  ID NAME  ID NAME
> -- ----- -- ----- -- -----
> 1  Hello 1  Hello 1  Hello
> rows: 1

drop table t1;
> ok

drop table t2;
> ok

drop table t3;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, parent int, sid int);
> ok

create index idx_p on test(sid);
> ok

insert into test select x, x, x from system_range(0,20);
> update count: 21

select * from test l0 inner join test l1 on l0.sid=l1.sid, test l3 where l0.sid=l3.parent;
> ID PARENT SID ID PARENT SID ID PARENT SID
> -- ------ --- -- ------ --- -- ------ ---
> 0  0      0   0  0      0   0  0      0
> 1  1      1   1  1      1   1  1      1
> 10 10     10  10 10     10  10 10     10
> 11 11     11  11 11     11  11 11     11
> 12 12     12  12 12     12  12 12     12
> 13 13     13  13 13     13  13 13     13
> 14 14     14  14 14     14  14 14     14
> 15 15     15  15 15     15  15 15     15
> 16 16     16  16 16     16  16 16     16
> 17 17     17  17 17     17  17 17     17
> 18 18     18  18 18     18  18 18     18
> 19 19     19  19 19     19  19 19     19
> 2  2      2   2  2      2   2  2      2
> 20 20     20  20 20     20  20 20     20
> 3  3      3   3  3      3   3  3      3
> 4  4      4   4  4      4   4  4      4
> 5  5      5   5  5      5   5  5      5
> 6  6      6   6  6      6   6  6      6
> 7  7      7   7  7      7   7  7      7
> 8  8      8   8  8      8   8  8      8
> 9  9      9   9  9      9   9  9      9
> rows: 21

select * from
test l0
inner join test l1 on l0.sid=l1.sid
inner join test l2 on l0.sid=l2.id,
test l5
inner join test l3 on l5.sid=l3.sid
inner join test l4 on l5.sid=l4.id
where l2.id is not null
and l0.sid=l5.parent;
> ID PARENT SID ID PARENT SID ID PARENT SID ID PARENT SID ID PARENT SID ID PARENT SID
> -- ------ --- -- ------ --- -- ------ --- -- ------ --- -- ------ --- -- ------ ---
> 0  0      0   0  0      0   0  0      0   0  0      0   0  0      0   0  0      0
> 1  1      1   1  1      1   1  1      1   1  1      1   1  1      1   1  1      1
> 10 10     10  10 10     10  10 10     10  10 10     10  10 10     10  10 10     10
> 11 11     11  11 11     11  11 11     11  11 11     11  11 11     11  11 11     11
> 12 12     12  12 12     12  12 12     12  12 12     12  12 12     12  12 12     12
> 13 13     13  13 13     13  13 13     13  13 13     13  13 13     13  13 13     13
> 14 14     14  14 14     14  14 14     14  14 14     14  14 14     14  14 14     14
> 15 15     15  15 15     15  15 15     15  15 15     15  15 15     15  15 15     15
> 16 16     16  16 16     16  16 16     16  16 16     16  16 16     16  16 16     16
> 17 17     17  17 17     17  17 17     17  17 17     17  17 17     17  17 17     17
> 18 18     18  18 18     18  18 18     18  18 18     18  18 18     18  18 18     18
> 19 19     19  19 19     19  19 19     19  19 19     19  19 19     19  19 19     19
> 2  2      2   2  2      2   2  2      2   2  2      2   2  2      2   2  2      2
> 20 20     20  20 20     20  20 20     20  20 20     20  20 20     20  20 20     20
> 3  3      3   3  3      3   3  3      3   3  3      3   3  3      3   3  3      3
> 4  4      4   4  4      4   4  4      4   4  4      4   4  4      4   4  4      4
> 5  5      5   5  5      5   5  5      5   5  5      5   5  5      5   5  5      5
> 6  6      6   6  6      6   6  6      6   6  6      6   6  6      6   6  6      6
> 7  7      7   7  7      7   7  7      7   7  7      7   7  7      7   7  7      7
> 8  8      8   8  8      8   8  8      8   8  8      8   8  8      8   8  8      8
> 9  9      9   9  9      9   9  9      9   9  9      9   9  9      9   9  9      9
> rows: 21

DROP TABLE IF EXISTS TEST;
> ok

--- joins ----------------------------------------------------------------------------------------------------
create table t1(id int, name varchar);
> ok

insert into t1 values(1, 'hi'), (2, 'world');
> update count: 2

create table t2(id int, name varchar);
> ok

insert into t2 values(1, 'Hallo'), (3, 'Welt');
> update count: 2

select * from t1 join t2 on t1.id=t2.id;
> ID NAME ID NAME
> -- ---- -- -----
> 1  hi   1  Hallo
> rows: 1

select * from t1 left join t2 on t1.id=t2.id;
> ID NAME  ID   NAME
> -- ----- ---- -----
> 1  hi    1    Hallo
> 2  world null null
> rows: 2

select * from t1 right join t2 on t1.id=t2.id;
> ID NAME  ID   NAME
> -- ----- ---- ----
> 1  Hallo 1    hi
> 3  Welt  null null
> rows: 2

select * from t1 cross join t2;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  hi    1  Hallo
> 1  hi    3  Welt
> 2  world 1  Hallo
> 2  world 3  Welt
> rows: 4

select * from t1 natural join t2;
> ID NAME
> -- ----
> rows: 0

explain select * from t1 natural join t2;
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.T2 /* PUBLIC.T2.tableScan */ INNER JOIN PUBLIC.T1 /* PUBLIC.T1.tableScan */ ON 1=1 WHERE (PUBLIC.T1.ID = PUBLIC.T2.ID) AND (PUBLIC.T1.NAME = PUBLIC.T2.NAME)
> rows: 1

drop table t1;
> ok

drop table t2;
> ok

create table customer(customerid int, customer_name varchar);
> ok

insert into customer values(0, 'Acme');
> update count: 1

create table invoice(customerid int, invoiceid int, invoice_text varchar);
> ok

insert into invoice values(0, 1, 'Soap'), (0, 2, 'More Soap');
> update count: 2

create table INVOICE_LINE(line_id int, invoiceid int, customerid int, line_text varchar);
> ok

insert into INVOICE_LINE values(10, 1, 0, 'Super Soap'), (20, 1, 0, 'Regular Soap');
> update count: 2

select c.*, i.*, l.* from customer c natural join invoice i natural join INVOICE_LINE l;
> CUSTOMERID CUSTOMER_NAME INVOICEID INVOICE_TEXT LINE_ID LINE_TEXT
> ---------- ------------- --------- ------------ ------- ------------
> 0          Acme          1         Soap         10      Super Soap
> 0          Acme          1         Soap         20      Regular Soap
> rows: 2

explain select c.*, i.*, l.* from customer c natural join invoice i natural join INVOICE_LINE l;
> PLAN
> ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT C.CUSTOMERID, C.CUSTOMER_NAME, I.INVOICEID, I.INVOICE_TEXT, L.LINE_ID, L.LINE_TEXT FROM PUBLIC.INVOICE I /* PUBLIC.INVOICE.tableScan */ INNER JOIN PUBLIC.INVOICE_LINE L /* PUBLIC.INVOICE_LINE.tableScan */ ON 1=1 /* WHERE (PUBLIC.I.CUSTOMERID = PUBLIC.L.CUSTOMERID) AND (PUBLIC.I.INVOICEID = PUBLIC.L.INVOICEID) */ INNER JOIN PUBLIC.CUSTOMER C /* PUBLIC.CUSTOMER.tableScan */ ON 1=1 WHERE (PUBLIC.C.CUSTOMERID = PUBLIC.I.CUSTOMERID) AND ((PUBLIC.I.CUSTOMERID = PUBLIC.L.CUSTOMERID) AND (PUBLIC.I.INVOICEID = PUBLIC.L.INVOICEID))
> rows: 1

drop table customer;
> ok

drop table invoice;
> ok

drop table INVOICE_LINE;
> ok

--- outer joins ----------------------------------------------------------------------------------------------
CREATE TABLE PARENT(ID INT, NAME VARCHAR(20));
> ok

CREATE TABLE CHILD(ID INT, PARENTID INT, NAME VARCHAR(20));
> ok

INSERT INTO PARENT VALUES(1, 'Sue');
> update count: 1

INSERT INTO PARENT VALUES(2, 'Joe');
> update count: 1

INSERT INTO CHILD VALUES(100, 1, 'Simon');
> update count: 1

INSERT INTO CHILD VALUES(101, 1, 'Sabine');
> update count: 1

SELECT * FROM PARENT P INNER JOIN CHILD C ON P.ID = C.PARENTID;
> ID NAME ID  PARENTID NAME
> -- ---- --- -------- ------
> 1  Sue  100 1        Simon
> 1  Sue  101 1        Sabine
> rows: 2

SELECT * FROM PARENT P LEFT OUTER JOIN CHILD C ON P.ID = C.PARENTID;
> ID NAME ID   PARENTID NAME
> -- ---- ---- -------- ------
> 1  Sue  100  1        Simon
> 1  Sue  101  1        Sabine
> 2  Joe  null null     null
> rows: 3

SELECT * FROM CHILD C RIGHT OUTER JOIN PARENT P ON P.ID = C.PARENTID;
> ID NAME ID   PARENTID NAME
> -- ---- ---- -------- ------
> 1  Sue  100  1        Simon
> 1  Sue  101  1        Sabine
> 2  Joe  null null     null
> rows: 3

DROP TABLE PARENT;
> ok

DROP TABLE CHILD;
> ok

CREATE TABLE A(A1 INT, A2 INT);
> ok

INSERT INTO A VALUES (1, 2);
> update count: 1

CREATE TABLE B(B1 INT, B2 INT);
> ok

INSERT INTO B VALUES (1, 2);
> update count: 1

CREATE TABLE C(B1 INT, C1 INT);
> ok

INSERT INTO C VALUES (1, 2);
> update count: 1

SELECT * FROM A LEFT JOIN B ON TRUE;
> A1 A2 B1 B2
> -- -- -- --
> 1  2  1  2
> rows: 1

SELECT A.A1, A.A2, B.B1, B.B2 FROM A RIGHT JOIN B ON TRUE;
> A1 A2 B1 B2
> -- -- -- --
> 1  2  1  2
> rows: 1

-- this syntax without ON or USING in not standard
SELECT * FROM A LEFT JOIN B;
> A1 A2 B1 B2
> -- -- -- --
> 1  2  1  2
> rows: 1

-- this syntax without ON or USING in not standard
SELECT A.A1, A.A2, B.B1, B.B2 FROM A RIGHT JOIN B;
> A1 A2 B1 B2
> -- -- -- --
> 1  2  1  2
> rows: 1

SELECT * FROM A LEFT JOIN B ON TRUE NATURAL JOIN C;
> A1 A2 B1 B2 C1
> -- -- -- -- --
> 1  2  1  2  2
> rows: 1

SELECT A.A1, A.A2, B.B1, B.B2, C.C1 FROM A RIGHT JOIN B ON TRUE NATURAL JOIN C;
> A1 A2 B1 B2 C1
> -- -- -- -- --
> 1  2  1  2  2
> rows: 1

-- this syntax without ON or USING in not standard
SELECT * FROM A LEFT JOIN B NATURAL JOIN C;
> A1 A2 B1 B2 C1
> -- -- -- -- --
> 1  2  1  2  2
> rows: 1

-- this syntax without ON or USING in not standard
SELECT A.A1, A.A2, B.B1, B.B2, C.C1 FROM A RIGHT JOIN B NATURAL JOIN C;
> A1 A2 B1 B2 C1
> -- -- -- -- --
> 1  2  1  2  2
> rows: 1

DROP TABLE A;
> ok

DROP TABLE B;
> ok

DROP TABLE C;
> ok

CREATE TABLE T1(X1 INT);
CREATE TABLE T2(X2 INT);
CREATE TABLE T3(X3 INT);
CREATE TABLE T4(X4 INT);
CREATE TABLE T5(X5 INT);

INSERT INTO T1 VALUES (1);
INSERT INTO T1 VALUES (NULL);
INSERT INTO T2 VALUES (1);
INSERT INTO T2 VALUES (NULL);
INSERT INTO T3 VALUES (1);
INSERT INTO T3 VALUES (NULL);
INSERT INTO T4 VALUES (1);
INSERT INTO T4 VALUES (NULL);
INSERT INTO T5 VALUES (1);
INSERT INTO T5 VALUES (NULL);

SELECT T1.X1, T2.X2, T3.X3, T4.X4, T5.X5 FROM (
    T1 INNER JOIN (
        T2 LEFT OUTER JOIN (
            T3 INNER JOIN T4 ON T3.X3 = T4.X4
        ) ON T2.X2 = T4.X4
    ) ON T1.X1 = T2.X2
) INNER JOIN T5 ON T2.X2 = T5.X5;
> X1 X2 X3 X4 X5
> -- -- -- -- --
> 1  1  1  1  1
> rows: 1
