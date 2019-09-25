-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
--- special grammar and test cases ---------------------------------------------------------------------------------------------
create table test(id int) as select 1;
> ok

select * from test where id in (select id from test order by 'x');
> ID
> --
> 1
> rows (ordered): 1

drop table test;
> ok

select x, x in(2, 3) i from system_range(1, 2) group by x;
> X I
> - -----
> 1 FALSE
> 2 TRUE
> rows: 2

select * from dual join(select x from dual) on 1=1;
> X X
> - -
> 1 1
> rows: 1

select 0 as x from system_range(1, 2) d group by d.x;
> X
> -
> 0
> 0
> rows: 2

select 1 "a", count(*) from dual group by "a" order by "a";
> a COUNT(*)
> - --------
> 1 1
> rows (ordered): 1

create table results(eventId int, points int, studentId int);
> ok

insert into results values(1, 10, 1), (2, 20, 1), (3, 5, 1);
> update count: 3

insert into results values(1, 10, 2), (2, 20, 2), (3, 5, 2);
> update count: 3

insert into results values(1, 10, 3), (2, 20, 3), (3, 5, 3);
> update count: 3

SELECT SUM(points) FROM RESULTS
WHERE eventID IN
(SELECT eventID FROM RESULTS
WHERE studentID = 2
ORDER BY points DESC
LIMIT 2 )
AND studentID = 2;
> SUM(POINTS)
> -----------
> 30
> rows (ordered): 1

SELECT eventID X FROM RESULTS
WHERE studentID = 2
ORDER BY points DESC
LIMIT 2;
> X
> -
> 2
> 1
> rows (ordered): 2

SELECT SUM(r.points) FROM RESULTS r,
(SELECT eventID FROM RESULTS
WHERE studentID = 2
ORDER BY points DESC
LIMIT 2 ) r2
WHERE r2.eventID = r.eventId
AND studentID = 2;
> SUM(R.POINTS)
> -------------
> 30
> rows (ordered): 1

drop table results;
> ok

create table test(a int, b int);
> ok

insert into test values(1, 1);
> update count: 1

create index on test(a, b desc);
> ok

select * from test where a = 1;
> A B
> - -
> 1 1
> rows: 1

drop table test;
> ok

create table test(id int, name varchar) as select 1, 'a';
> ok

(select id from test order by id) union (select id from test order by name);
> ID
> --
> 1
> rows (ordered): 1

drop table test;
> ok

create sequence seq;
> ok

select case seq.nextval when 2 then 'two' when 3 then 'three' when 1 then 'one' else 'other' end result from dual;
> RESULT
> ------
> one
> rows: 1

drop sequence seq;
> ok

create table test(x int);
> ok

create hash index on test(x);
> ok

select 1 from test group by x;
> 1
> -
> rows: 0

drop table test;
> ok

select * from dual where x = x + 1 or x in(2, 0);
> X
> -
> rows: 0

select * from system_range(1,1) order by x limit 3 offset 3;
> X
> -
> rows (ordered): 0

select * from dual where cast('a' || x as varchar_ignorecase) in ('A1', 'B1');
> X
> -
> 1
> rows: 1

create sequence seq start with 65 increment by 1;
> ok

select char(nextval('seq')) as x;
> X
> -
> A
> rows: 1

select char(nextval('seq')) as x;
> X
> -
> B
> rows: 1

drop sequence seq;
> ok

create table test(id int, name varchar);
> ok

insert into test values(5, 'b'), (5, 'b'), (20, 'a');
> update count: 3

select id from test where name in(null, null);
> ID
> --
> rows: 0

select * from (select * from test order by name limit 1) where id < 10;
> ID NAME
> -- ----
> rows (ordered): 0

drop table test;
> ok

create table test (id int not null, pid int);
> ok

create index idx_test_pid on test (pid);
> ok

alter table test add constraint fk_test foreign key (pid)
references test (id) index idx_test_pid;
> ok

insert into test values (2, null);
> update count: 1

update test set pid = 1 where id = 2;
> exception

drop table test;
> ok

create table test(name varchar(255));
> ok

select * from test union select * from test order by test.name;
> exception

insert into test values('a'), ('b'), ('c');
> update count: 3

select name from test where name > all(select name from test where name<'b');
> NAME
> ----
> b
> c
> rows: 2

select count(*) from (select name from test where name > all(select name from test where name<'b')) x;
> COUNT(*)
> --------
> 2
> rows: 1

drop table test;
> ok

create table test(id int) as select 1;
> ok

select * from test where id >= all(select id from test where 1=0);
> ID
> --
> 1
> rows: 1

select * from test where id = all(select id from test where 1=0);
> ID
> --
> 1
> rows: 1

select * from test where id = all(select id from test union all select id from test);
> ID
> --
> 1
> rows: 1

select * from test where null >= all(select id from test where 1=0);
> ID
> --
> 1
> rows: 1

select * from test where null = all(select id from test where 1=0);
> ID
> --
> 1
> rows: 1

select * from test where null = all(select id from test union all select id from test);
> ID
> --
> rows: 0

select * from test where id >= all(select cast(null as int) from test);
> ID
> --
> rows: 0

select * from test where id = all(select null from test union all select id from test);
> ID
> --
> rows: 0

select * from test where null >= all(select cast(null as int) from test);
> ID
> --
> rows: 0

select * from test where null = all(select null from test union all select id from test);
> ID
> --
> rows: 0

drop table test;
> ok

select x from dual order by y.x;
> exception

create table test(id int primary key, name varchar(255), row_number int);
> ok

insert into test values(1, 'hello', 10), (2, 'world', 20);
> update count: 2

select row_number() over(), id, name from test order by id;
> ROWNUM() ID NAME
> -------- -- -----
> 1        1  hello
> 2        2  world
> rows (ordered): 2

select row_number() over(), id, name from test order by name;
> ROWNUM() ID NAME
> -------- -- -----
> 1        1  hello
> 2        2  world
> rows (ordered): 2

select row_number() over(), id, name from test order by name desc;
> ROWNUM() ID NAME
> -------- -- -----
> 2        2  world
> 1        1  hello
> rows (ordered): 2

update test set (id)=(id);
> update count: 2

drop table test;
> ok

create table test(x int) as select x from system_range(1, 2);
> ok

select * from (select rownum r from test) where r in (1, 2);
> R
> -
> 1
> 2
> rows: 2

select * from (select rownum r from test) where r = 1 or r = 2;
> R
> -
> 1
> 2
> rows: 2

drop table test;
> ok

select 2^2;
> exception

select * from dual where x in (select x from dual group by x order by max(x));
> X
> -
> 1
> rows (ordered): 1

create table test(d decimal(1, 2));
> exception

call truncate_value('Test 123', 4, false);
> 'Test'
> ------
> Test
> rows: 1

call truncate_value(1234567890.123456789, 4, false);
> exception

call truncate_value(1234567890.123456789, 4, true);
> 1234567890.1234567
> ------------------
> 1234567890.1234567
> rows: 1

select * from dual where cast('xx' as varchar_ignorecase(1)) = 'X' and cast('x x ' as char(2)) = 'x';
> X
> -
> 1
> rows: 1

explain select -cast(0 as real), -cast(0 as double);
> PLAN
> ----------------------------------------------------------------
> SELECT 0.0, 0.0 FROM SYSTEM_RANGE(1, 1) /* PUBLIC.RANGE_INDEX */
> rows: 1

select () empty;
> EMPTY
> -----
> ()
> rows: 1

select (1,) one_element;
> ONE_ELEMENT
> -----------
> (1)
> rows: 1

select (1) one;
> ONE
> ---
> 1
> rows: 1

create table test(id int);
> ok

insert into test values(1), (2), (4);
> update count: 3

select * from test order by id limit -1;
> ID
> --
> 1
> 2
> 4
> rows (ordered): 3

select * from test order by id limit 0;
> ID
> --
> rows (ordered): 0

select * from test order by id limit 1;
> ID
> --
> 1
> rows (ordered): 1

select * from test order by id limit 1+1;
> ID
> --
> 1
> 2
> rows (ordered): 2

select * from test order by id limit null;
> ID
> --
> 1
> 2
> 4
> rows (ordered): 3

select a.id, a.id in(select 4) x  from test a, test b where a.id in (b.id, b.id - 1);
> ID X
> -- -----
> 1  FALSE
> 1  FALSE
> 2  FALSE
> 4  TRUE
> rows: 4

select a.id, a.id in(select 4) x  from test a, test b where a.id in (b.id, b.id - 1) group by a.id;
> ID X
> -- -----
> 1  FALSE
> 2  FALSE
> 4  TRUE
> rows: 3

select a.id, 4 in(select a.id) x  from test a, test b where a.id in (b.id, b.id - 1) group by a.id;
> ID X
> -- -----
> 1  FALSE
> 2  FALSE
> 4  TRUE
> rows: 3

delete from test limit 0;
> ok

delete from test limit 1;
> update count: 1

delete from test limit -1;
> update count: 2

drop table test;
> ok

create domain x as int not null;
> ok

create table test(id x);
> ok

insert into test values(null);
> exception

drop table test;
> ok

drop domain x;
> ok

create table test(id int primary key);
> ok

insert into test(id) direct sorted select x from system_range(1, 100);
> update count: 100

explain insert into test(id) direct sorted select x from system_range(1, 100);
> PLAN
> -----------------------------------------------------------------------------------------------------
> INSERT INTO PUBLIC.TEST(ID) DIRECT SORTED SELECT X FROM SYSTEM_RANGE(1, 100) /* PUBLIC.RANGE_INDEX */
> rows: 1

explain select * from test limit 10 sample_size 10;
> PLAN
> -----------------------------------------------------------------------------------
> SELECT TEST.ID FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ LIMIT 10 SAMPLE_SIZE 10
> rows: 1

drop table test;
> ok

create table test(id int primary key);
> ok

insert into test values(1), (2), (3), (4);
> update count: 4

explain analyze select * from test where id is null;
> PLAN
> ----------------------------------------------------------------------------------------------------------
> SELECT TEST.ID FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID IS NULL */ /* scanCount: 1 */ WHERE ID IS NULL
> rows: 1

drop table test;
> ok

explain analyze select 1;
> PLAN
> ----------------------------------------------------------------------------
> SELECT 1 FROM SYSTEM_RANGE(1, 1) /* PUBLIC.RANGE_INDEX */ /* scanCount: 2 */
> rows: 1

create table folder(id int primary key, name varchar(255), parent int);
> ok

insert into folder values(1, null, null), (2, 'bin', 1), (3, 'docs', 1), (4, 'html', 3), (5, 'javadoc', 3), (6, 'ext', 1), (7, 'service', 1), (8, 'src', 1), (9, 'docsrc', 8), (10, 'installer', 8), (11, 'main', 8), (12, 'META-INF', 11), (13, 'org', 11), (14, 'h2', 13), (15, 'test', 8), (16, 'tools', 8);
> update count: 16

with link(id, name, level) as (select id, name, 0 from folder where parent is null union all select folder.id, ifnull(link.name || '/', '') || folder.name, level + 1 from link inner join folder on link.id = folder.parent) select name from link where name is not null order by cast(id as int);
> NAME
> -----------------
> bin
> docs
> docs/html
> docs/javadoc
> ext
> service
> src
> src/docsrc
> src/installer
> src/main
> src/main/META-INF
> src/main/org
> src/main/org/h2
> src/test
> src/tools
> rows (ordered): 15

drop table folder;
> ok

create table test(id int);
> ok

create view x as select * from test;
> ok

drop table test restrict;
> exception

drop table test cascade;
> ok

select 1, 2 from (select * from dual) union all select 3, 4 from dual;
> 1 2
> - -
> 1 2
> 3 4
> rows: 2

select 3 from (select * from dual) union all select 2 from dual;
> 3
> -
> 2
> 3
> rows: 2

create table a(x int, y int);
> ok

create unique index a_xy on a(x, y);
> ok

create table b(x int, y int, foreign key(x, y) references a(x, y));
> ok

insert into a values(null, null), (null, 0), (0, null), (0, 0);
> update count: 4

insert into b values(null, null), (null, 0), (0, null), (0, 0);
> update count: 4

delete from a where x is null and y is null;
> update count: 1

delete from a where x is null and y = 0;
> update count: 1

delete from a where x = 0 and y is null;
> update count: 1

delete from a where x = 0 and y = 0;
> exception

drop table b;
> ok

drop table a;
> ok

select * from (select null as x) where x=1;
> X
> -
> rows: 0

create table test(a int primary key, b int references(a));
> ok

merge into test values(1, 2);
> exception

drop table test;
> ok

create table test(id int primary key, d int);
> ok

insert into test values(1,1), (2, 1);
> update count: 2

select id from test where id in (1, 2) and d = 1;
> ID
> --
> 1
> 2
> rows: 2

drop table test;
> ok

create table test(id decimal(10, 2) primary key) as select 0;
> ok

select * from test where id = 0.00;
> ID
> ----
> 0.00
> rows: 1

select * from test where id = 0.0;
> ID
> ----
> 0.00
> rows: 1

drop table test;
> ok

select count(*) from (select 1 union (select 2 intersect select 2)) x;
> COUNT(*)
> --------
> 2
> rows: 1

create table test(id varchar(1) primary key) as select 'X';
> ok

select count(*) from (select 1 from dual where x in ((select 1 union select 1))) a;
> COUNT(*)
> --------
> 1
> rows: 1

insert into test ((select 1 union select 2) union select 3);
> update count: 3

select count(*) from test where id = 'X1';
> COUNT(*)
> --------
> 0
> rows: 1

drop table test;
> ok

create table test(id int primary key, name varchar(255), x int);
> ok

create unique index idx_name1 on test(name);
> ok

create unique index idx_name2 on test(name);
> ok

show columns from test;
> FIELD TYPE         NULL KEY DEFAULT
> ----- ------------ ---- --- -------
> ID    INTEGER(10)  NO   PRI NULL
> NAME  VARCHAR(255) YES  UNI NULL
> X     INTEGER(10)  YES      NULL
> rows: 3

show columns from catalogs from information_schema;
> FIELD        TYPE                NULL KEY DEFAULT
> ------------ ------------------- ---- --- -------
> CATALOG_NAME VARCHAR(2147483647) YES      NULL
> rows: 1

show columns from information_schema.catalogs;
> FIELD        TYPE                NULL KEY DEFAULT
> ------------ ------------------- ---- --- -------
> CATALOG_NAME VARCHAR(2147483647) YES      NULL
> rows: 1

drop table test;
> ok

create table test(id int, constraint pk primary key(id), constraint x unique(id));
> ok

select constraint_name from information_schema.indexes where table_name = 'TEST';
> CONSTRAINT_NAME
> ---------------
> PK
> rows: 1

drop table test;
> ok

create table parent(id int primary key);
> ok

create table child(id int, parent_id int, constraint child_parent foreign key (parent_id) references parent(id));
> ok

select constraint_name from information_schema.indexes where table_name = 'CHILD';
> CONSTRAINT_NAME
> ---------------
> CHILD_PARENT
> rows: 1

drop table parent, child;
> ok

create table test(id int, name varchar(max));
> ok

alter table test alter column id identity;
> ok

drop table test;
> ok

create table test(id int primary key, name varchar);
> ok

alter table test alter column id int auto_increment;
> ok

create table otherTest(id int primary key, name varchar);
> ok

alter table otherTest add constraint fk foreign key(id) references test(id);
> ok

alter table otherTest drop foreign key fk;
> ok

create unique index idx on otherTest(name);
> ok

alter table otherTest drop index idx;
> ok

drop table otherTest;
> ok

insert into test(id) values(1);
> update count: 1

alter table test change column id id2 int;
> ok

select id2 from test;
> ID2
> ---
> 1
> rows: 1

drop table test;
> ok

create table test(id identity);
> ok

set password test;
> exception

alter user sa set password test;
> exception

comment on table test is test;
> exception

select 1 from test a where 1 in(select 1 from test b where b.id in(select 1 from test c where c.id=a.id));
> 1
> -
> rows: 0

drop table test;
> ok

select @n := case when x = 1 then 1 else @n * x end f from system_range(1, 4);
> F
> --
> 1
> 2
> 24
> 6
> rows: 4

select * from (select "x" from dual);
> exception

select * from(select 1 from system_range(1, 2) group by sin(x) order by sin(x));
> 1
> -
> 1
> 1
> rows (ordered): 2

create table parent as select 1 id, 2 x;
> ok

create table child(id int references parent(id)) as select 1;
> ok

delete from parent;
> exception

drop table parent, child;
> ok

create domain integer as varchar;
> exception

create domain int as varchar;
> ok

create memory table test(id int);
> ok

script nodata nopasswords nosettings;
> SCRIPT
> -----------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE DOMAIN INT AS VARCHAR;
> CREATE MEMORY TABLE PUBLIC.TEST( ID VARCHAR );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 4

drop table test;
> ok

drop domain int;
> ok

create table test(id identity, parent bigint, foreign key(parent) references(id));
> ok

insert into test values(0, 0), (1, NULL), (2, 1), (3, 3), (4, 3);
> update count: 5

delete from test where id = 3;
> exception

delete from test where id = 0;
> update count: 1

delete from test where id = 1;
> exception

drop table test;
> ok

select iso_week('2006-12-31') w, iso_year('2007-12-31') y, iso_day_of_week('2007-12-31') w;
> W  Y    W
> -- ---- -
> 52 2008 1
> rows: 1

create schema a;
> ok

set autocommit false;
> ok

set schema a;
> ok

create table t1 ( k int, v varchar(10) );
> ok

insert into t1 values ( 1, 't1' );
> update count: 1

create table t2 ( k int, v varchar(10) );
> ok

insert into t2 values ( 2, 't2' );
> update count: 1

create view v_test(a, b, c, d) as select t1.*, t2.* from t1 join t2 on ( t1.k = t2.k );
> ok

select * from v_test;
> A B C D
> - - - -
> rows: 0

set schema public;
> ok

drop schema a cascade;
> ok

set autocommit true;
> ok

select x/3 as a, count(*) c from system_range(1, 10) group by a having c>2;
> A C
> - -
> 1 3
> 2 3
> rows: 2

create table test(id int);
> ok

insert into test values(1), (2);
> update count: 2

select id+1 as x, count(*) from test group by x;
> X COUNT(*)
> - --------
> 2 1
> 3 1
> rows: 2

select 1 as id, id as b, count(*)  from test group by id;
> ID B COUNT(*)
> -- - --------
> 1  1 1
> 1  2 1
> rows: 2

select id+1 as x, count(*) from test group by -x;
> exception

select id+1 as x, count(*) from test group by x having x>2;
> exception

select id+1 as x, count(*) from test group by 1;
> exception

drop table test;
> ok

create table test(t0 timestamp(0), t1 timestamp(1), t4 timestamp(4));
> ok

select column_name, numeric_scale from information_schema.columns c where c.table_name = 'TEST' order by column_name;
> COLUMN_NAME NUMERIC_SCALE
> ----------- -------------
> T0          0
> T1          1
> T4          4
> rows (ordered): 3

drop table test;
> ok

create table test(id int);
> ok

insert into test values(null), (1);
> update count: 2

select * from test where id not in (select id from test where 1=0);
> ID
> ----
> 1
> null
> rows: 2

select * from test where null not in (select id from test where 1=0);
> ID
> ----
> 1
> null
> rows: 2

select * from test where not (id in (select id from test where 1=0));
> ID
> ----
> 1
> null
> rows: 2

select * from test where not (null in (select id from test where 1=0));
> ID
> ----
> 1
> null
> rows: 2

drop table test;
> ok

create table test(a int);
> ok

insert into test values(1), (2);
> update count: 2

select -test.a a from test order by test.a;
> A
> --
> -1
> -2
> rows (ordered): 2

select -test.a from test order by test.a;
> - TEST.A
> --------
> -1
> -2
> rows (ordered): 2

select -test.a aa from test order by a;
> AA
> --
> -1
> -2
> rows (ordered): 2

select -test.a aa from test order by aa;
> AA
> --
> -2
> -1
> rows (ordered): 2

select -test.a a from test order by a;
> A
> --
> -2
> -1
> rows (ordered): 2

drop table test;
> ok

CREATE TABLE table_a(a_id INT PRIMARY KEY, left_id INT, right_id INT);
> ok

CREATE TABLE table_b(b_id INT PRIMARY KEY, a_id INT);
> ok

CREATE TABLE table_c(left_id INT, right_id INT, center_id INT);
> ok

CREATE VIEW view_a AS
SELECT table_c.center_id, table_a.a_id, table_b.b_id
FROM table_c
INNER JOIN table_a ON table_c.left_id = table_a.left_id
AND table_c.right_id = table_a.right_id
LEFT JOIN table_b ON table_b.a_id = table_a.a_id;
> ok

SELECT * FROM table_c INNER JOIN view_a
ON table_c.center_id = view_a.center_id;
> LEFT_ID RIGHT_ID CENTER_ID CENTER_ID A_ID B_ID
> ------- -------- --------- --------- ---- ----
> rows: 0

drop view view_a;
> ok

drop table table_a, table_b, table_c;
> ok

create table t (pk int primary key, attr int);
> ok

insert into t values (1, 5), (5, 1);
> update count: 2

select t1.pk from t t1, t t2 where t1.pk = t2.attr order by t1.pk;
> PK
> --
> 1
> 5
> rows (ordered): 2

drop table t;
> ok

CREATE ROLE TEST_A;
> ok

GRANT TEST_A TO TEST_A;
> exception

CREATE ROLE TEST_B;
> ok

GRANT TEST_A TO TEST_B;
> ok

GRANT TEST_B TO TEST_A;
> exception

DROP ROLE TEST_A;
> ok

DROP ROLE TEST_B;
> ok

CREATE ROLE PUBLIC2;
> ok

GRANT PUBLIC2 TO SA;
> ok

GRANT PUBLIC2 TO SA;
> ok

REVOKE PUBLIC2 FROM SA;
> ok

REVOKE PUBLIC2 FROM SA;
> ok

DROP ROLE PUBLIC2;
> ok

create table test(id int primary key, lastname varchar, firstname varchar, parent int references(id));
> ok

alter table test add constraint name unique (lastname, firstname);
> ok

SELECT CONSTRAINT_NAME, UNIQUE_INDEX_NAME, COLUMN_LIST FROM INFORMATION_SCHEMA.CONSTRAINTS ;
> CONSTRAINT_NAME UNIQUE_INDEX_NAME COLUMN_LIST
> --------------- ----------------- ------------------
> CONSTRAINT_2    PRIMARY_KEY_2     ID
> CONSTRAINT_27   PRIMARY_KEY_2     PARENT
> NAME            NAME_INDEX_2      LASTNAME,FIRSTNAME
> rows: 3

drop table test;
> ok

alter table information_schema.help rename to information_schema.help2;
> exception

help abc;
> ID SECTION TOPIC SYNTAX TEXT
> -- ------- ----- ------ ----
> rows: 0

CREATE TABLE test (id int(25) NOT NULL auto_increment, name varchar NOT NULL, PRIMARY KEY  (id,name));
> ok

drop table test;
> ok

CREATE TABLE test (id bigserial NOT NULL primary key);
> ok

drop table test;
> ok

CREATE TABLE test (id serial NOT NULL primary key);
> ok

drop table test;
> ok

CREATE MEMORY TABLE TEST(ID INT, D DOUBLE, F FLOAT);
> ok

insert into test values(0, POWER(0, -1), POWER(0, -1)), (1, -POWER(0, -1), -POWER(0, -1)), (2, SQRT(-1), SQRT(-1));
> update count: 3

select * from test order by id;
> ID D         F
> -- --------- ---------
> 0  Infinity  Infinity
> 1  -Infinity -Infinity
> 2  NaN       NaN
> rows (ordered): 3

script nopasswords nosettings;
> SCRIPT
> -----------------------------------------------------------------------------------------------------------------------------------------
> -- 3 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT, D DOUBLE, F FLOAT );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.TEST(ID, D, F) VALUES (0, POWER(0, -1), POWER(0, -1)), (1, (-POWER(0, -1)), (-POWER(0, -1))), (2, SQRT(-1), SQRT(-1));
> rows: 4

DROP TABLE TEST;
> ok

create schema a;
> ok

create table a.x(ax int);
> ok

create schema b;
> ok

create table b.x(bx int);
> ok

select * from a.x, b.x;
> AX BX
> -- --
> rows: 0

drop schema a cascade;
> ok

drop schema b cascade;
> ok

create table t1 (id int primary key);
> ok

create table t2 (id int primary key);
> ok

insert into t1 select x from system_range(1, 1000);
> update count: 1000

insert into t2 select x from system_range(1, 1000);
> update count: 1000

explain select count(*) from t1 where t1.id in ( select t2.id from t2 );
> PLAN
> ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT COUNT(*) FROM PUBLIC.T1 /* PUBLIC.PRIMARY_KEY_A: ID IN(SELECT T2.ID FROM PUBLIC.T2 /++ PUBLIC.T2.tableScan ++/) */ WHERE T1.ID IN( SELECT T2.ID FROM PUBLIC.T2 /* PUBLIC.T2.tableScan */)
> rows: 1

select count(*) from t1 where t1.id in ( select t2.id from t2 );
> COUNT(*)
> --------
> 1000
> rows: 1

drop table t1, t2;
> ok

CREATE TABLE p(d date);
> ok

INSERT INTO p VALUES('-1-01-01'), ('0-01-01'), ('0001-01-01');
> update count: 3

select d, year(d), extract(year from d), cast(d as timestamp) from p;
> D          YEAR(D) EXTRACT(YEAR FROM D) CAST(D AS TIMESTAMP)
> ---------- ------- -------------------- --------------------
> -1-01-01   -1      -1                   -1-01-01 00:00:00
> 0-01-01    0       0                    0-01-01 00:00:00
> 0001-01-01 1       1                    0001-01-01 00:00:00
> rows: 3

drop table p;
> ok

(SELECT X FROM DUAL ORDER BY X+2) UNION SELECT X FROM DUAL;
> X
> -
> 1
> rows (ordered): 1

create table test(a int, b int default 1);
> ok

insert into test values(1, default), (2, 2), (3, null);
> update count: 3

select * from test;
> A B
> - ----
> 1 1
> 2 2
> 3 null
> rows: 3

update test set b = default where a = 2;
> update count: 1

explain update test set b = default where a = 2;
> PLAN
> --------------------------------------------------------------------------
> UPDATE PUBLIC.TEST /* PUBLIC.TEST.tableScan */ SET B = DEFAULT WHERE A = 2
> rows: 1

select * from test;
> A B
> - ----
> 1 1
> 2 1
> 3 null
> rows: 3

update test set a=default;
> update count: 3

drop table test;
> ok

CREATE ROLE X;
> ok

GRANT X TO X;
> exception

CREATE ROLE Y;
> ok

GRANT Y TO X;
> ok

DROP ROLE Y;
> ok

DROP ROLE X;
> ok

select top sum(1) 0 from dual;
> exception

create table test(id int primary key, name varchar) as select 1, 'Hello World';
> ok

select * from test;
> ID NAME
> -- -----------
> 1  Hello World
> rows: 1

drop table test;
> ok

select rtrim() from dual;
> exception

CREATE TABLE COUNT(X INT);
> ok

CREATE FORCE TRIGGER T_COUNT BEFORE INSERT ON COUNT CALL "com.Unknown";
> ok

INSERT INTO COUNT VALUES(NULL);
> exception

DROP TRIGGER T_COUNT;
> ok

CREATE TABLE ITEMS(ID INT CHECK ID < SELECT MAX(ID) FROM COUNT);
> ok

insert into items values(DEFAULT);
> update count: 1

DROP TABLE COUNT;
> exception

insert into items values(DEFAULT);
> update count: 1

drop table items, count;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, LABEL CHAR(20), LOOKUP CHAR(30));
> ok

INSERT INTO TEST VALUES (1, 'Mouse', 'MOUSE'), (2, 'MOUSE', 'Mouse');
> update count: 2

SELECT * FROM TEST;
> ID LABEL LOOKUP
> -- ----- ------
> 1  Mouse MOUSE
> 2  MOUSE Mouse
> rows: 2

DROP TABLE TEST;
> ok

call 'a' regexp 'Ho.*\';
> exception

set @t = 0;
> ok

call set(1, 2);
> exception

select x, set(@t, ifnull(@t, 0) + x) from system_range(1, 3);
> X SET(@T, (IFNULL(@T, 0) + X))
> - ----------------------------
> 1 1
> 2 3
> 3 6
> rows: 3

select * from system_range(1, 2) a,
(select * from system_range(1, 2) union select * from system_range(1, 2)
union select * from system_range(1, 1)) v where a.x = v.x;
> X X
> - -
> 1 1
> 2 2
> rows: 2

create table test(id int);
> ok

select * from ((select * from test) union (select * from test)) where id = 0;
> ID
> --
> rows: 0

select * from ((test d1 inner join test d2 on d1.id = d2.id) inner join test d3 on d1.id = d3.id) inner join test d4 on d4.id = d1.id;
> ID ID ID ID
> -- -- -- --
> rows: 0

drop table test;
> ok

select count(*) from system_range(1, 2) where x in(1, 1, 1);
> COUNT(*)
> --------
> 1
> rows: 1

create table person(id bigint auto_increment, name varchar(100));
> ok

insert into person(name) values ('a'), ('b'), ('c');
> update count: 3

select * from person order by id;
> ID NAME
> -- ----
> 1  a
> 2  b
> 3  c
> rows (ordered): 3

select * from person order by id limit 2;
> ID NAME
> -- ----
> 1  a
> 2  b
> rows (ordered): 2

select * from person order by id limit 2 offset 1;
> ID NAME
> -- ----
> 2  b
> 3  c
> rows (ordered): 2

select * from person order by id limit 2147483647 offset 1;
> ID NAME
> -- ----
> 2  b
> 3  c
> rows (ordered): 2

select * from person order by id limit 2147483647-1 offset 1;
> ID NAME
> -- ----
> 2  b
> 3  c
> rows (ordered): 2

select * from person order by id limit 2147483647-1 offset 2;
> ID NAME
> -- ----
> 3  c
> rows (ordered): 1

select * from person order by id limit 2147483647-2 offset 2;
> ID NAME
> -- ----
> 3  c
> rows (ordered): 1

drop table person;
> ok

CREATE TABLE TEST(ID INTEGER NOT NULL, ID2 INTEGER DEFAULT 0);
> ok

ALTER TABLE test ALTER COLUMN ID2 RENAME TO ID;
> exception

drop table test;
> ok

create table test(id int primary key, data array);
> ok

insert into test values(1, (1, 1)), (2, (1, 2)), (3, (1, 1, 1));
> update count: 3

select * from test order by data;
> ID DATA
> -- ---------
> 1  (1, 1)
> 3  (1, 1, 1)
> 2  (1, 2)
> rows (ordered): 3

drop table test;
> ok

CREATE TABLE FOO (A CHAR(10));
> ok

CREATE TABLE BAR AS SELECT * FROM FOO;
> ok

select table_name, numeric_precision from information_schema.columns where column_name = 'A';
> TABLE_NAME NUMERIC_PRECISION
> ---------- -----------------
> BAR        10
> FOO        10
> rows: 2

DROP TABLE FOO, BAR;
> ok

create table multi_pages(dir_num int, bh_id int);
> ok

insert into multi_pages values(1, 1), (2, 2), (3, 3);
> update count: 3

create table b_holding(id int primary key, site varchar(255));
> ok

insert into b_holding values(1, 'Hello'), (2, 'Hello'), (3, 'Hello');
> update count: 3

select * from (select dir_num, count(*) as cnt from multi_pages  t, b_holding bh
where t.bh_id=bh.id and bh.site='Hello' group by dir_num) as x
where cnt < 1000 order by dir_num asc;
> DIR_NUM CNT
> ------- ---
> 1       1
> 2       1
> 3       1
> rows (ordered): 3

explain select * from (select dir_num, count(*) as cnt from multi_pages  t, b_holding bh
where t.bh_id=bh.id and bh.site='Hello' group by dir_num) as x
where cnt < 1000 order by dir_num asc;
> PLAN
> ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT X.DIR_NUM, X.CNT FROM ( SELECT DIR_NUM, COUNT(*) AS CNT FROM PUBLIC.MULTI_PAGES T INNER JOIN PUBLIC.B_HOLDING BH ON 1=1 WHERE (BH.SITE = 'Hello') AND (T.BH_ID = BH.ID) GROUP BY DIR_NUM ) X /* SELECT DIR_NUM, COUNT(*) AS CNT FROM PUBLIC.MULTI_PAGES T /++ PUBLIC.MULTI_PAGES.tableScan ++/ INNER JOIN PUBLIC.B_HOLDING BH /++ PUBLIC.PRIMARY_KEY_3: ID = T.BH_ID ++/ ON 1=1 WHERE (BH.SITE = 'Hello') AND (T.BH_ID = BH.ID) GROUP BY DIR_NUM HAVING COUNT(*) <= ?1: CNT < 1000 */ WHERE CNT < 1000 ORDER BY 1
> rows (ordered): 1

select dir_num, count(*) as cnt from multi_pages  t, b_holding bh
where t.bh_id=bh.id and bh.site='Hello' group by dir_num
having count(*) < 1000 order by dir_num asc;
> DIR_NUM CNT
> ------- ---
> 1       1
> 2       1
> 3       1
> rows (ordered): 3

drop table multi_pages, b_holding;
> ok

select * from dual where x = 1000000000000000000000;
> X
> -
> rows: 0

select * from dual where x = 'Hello';
> exception

create table test(id smallint primary key);
> ok

insert into test values(1), (2), (3);
> update count: 3

explain select * from test where id = 1;
> PLAN
> -------------------------------------------------------------------------------
> SELECT TEST.ID FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE ID = 1
> rows: 1

EXPLAIN SELECT * FROM TEST WHERE ID = (SELECT MAX(ID) FROM TEST);
> PLAN
> ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = (SELECT MAX(ID) FROM PUBLIC.TEST /++ PUBLIC.TEST.tableScan ++/ /++ direct lookup ++/) */ WHERE ID = (SELECT MAX(ID) FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ /* direct lookup */)
> rows: 1

drop table test;
> ok

create table test(id tinyint primary key);
> ok

insert into test values(1), (2), (3);
> update count: 3

explain select * from test where id = 3;
> PLAN
> -------------------------------------------------------------------------------
> SELECT TEST.ID FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = 3 */ WHERE ID = 3
> rows: 1

explain select * from test where id = 255;
> PLAN
> -----------------------------------------------------------------------------------
> SELECT TEST.ID FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = 255 */ WHERE ID = 255
> rows: 1

drop table test;
> ok

create table test(id int primary key);
> ok

insert into test values(1), (2), (3);
> update count: 3

explain select * from test where id in(1, 2, null);
> PLAN
> -----------------------------------------------------------------------------------------------------
> SELECT TEST.ID FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID IN(1, 2, NULL) */ WHERE ID IN(1, 2, NULL)
> rows: 1

drop table test;
> ok

create alias "SYSDATE" for "java.lang.Integer.parseInt(java.lang.String)";
> exception

create alias "MIN" for "java.lang.Integer.parseInt(java.lang.String)";
> exception

create alias "CAST" for "java.lang.Integer.parseInt(java.lang.String)";
> exception

CREATE TABLE PARENT(A INT, B INT, PRIMARY KEY(A, B));
> ok

CREATE TABLE CHILD(A INT, B INT, CONSTRAINT CP FOREIGN KEY(A, B) REFERENCES PARENT(A, B));
> ok

INSERT INTO PARENT VALUES(1, 2);
> update count: 1

INSERT INTO CHILD VALUES(2, NULL), (NULL, 3), (NULL, NULL), (1, 2);
> update count: 4

set autocommit false;
> ok

ALTER TABLE CHILD SET REFERENTIAL_INTEGRITY FALSE;
> ok

ALTER TABLE CHILD SET REFERENTIAL_INTEGRITY TRUE CHECK;
> ok

set autocommit true;
> ok

DROP TABLE CHILD, PARENT;
> ok

CREATE TABLE TEST(BIRTH TIMESTAMP);
> ok

INSERT INTO TEST VALUES('2006-04-03 10:20:30'), ('2006-04-03 10:20:31'), ('2006-05-05 00:00:00'), ('2006-07-03 22:30:00'), ('2006-07-03 22:31:00');
> update count: 5

SELECT * FROM (SELECT CAST(BIRTH AS DATE) B
FROM TEST GROUP BY CAST(BIRTH AS DATE)) A
WHERE A.B >= '2006-05-05';
> B
> ----------
> 2006-05-05
> 2006-07-03
> rows: 2

DROP TABLE TEST;
> ok

CREATE TABLE Parent(ID INT PRIMARY KEY, Name VARCHAR);
> ok

CREATE TABLE Child(ID INT);
> ok

ALTER TABLE Child ADD FOREIGN KEY(ID) REFERENCES Parent(ID);
> ok

INSERT INTO Parent VALUES(1,  '0'), (2,  '0'), (3,  '0');
> update count: 3

INSERT INTO Child VALUES(1);
> update count: 1

ALTER TABLE Parent ALTER COLUMN Name BOOLEAN NULL;
> ok

DELETE FROM Parent WHERE ID=3;
> update count: 1

DROP TABLE Parent, Child;
> ok

set autocommit false;
> ok

CREATE TABLE A(ID INT PRIMARY KEY, SK INT);
> ok

ALTER TABLE A ADD CONSTRAINT AC FOREIGN KEY(SK) REFERENCES A(ID);
> ok

INSERT INTO A VALUES(1, 1);
> update count: 1

INSERT INTO A VALUES(-2, NULL);
> update count: 1

ALTER TABLE A SET REFERENTIAL_INTEGRITY FALSE;
> ok

ALTER TABLE A SET REFERENTIAL_INTEGRITY TRUE CHECK;
> ok

ALTER TABLE A SET REFERENTIAL_INTEGRITY FALSE;
> ok

INSERT INTO A VALUES(2, 3);
> update count: 1

ALTER TABLE A SET REFERENTIAL_INTEGRITY TRUE;
> ok

ALTER TABLE A SET REFERENTIAL_INTEGRITY FALSE;
> ok

ALTER TABLE A SET REFERENTIAL_INTEGRITY TRUE CHECK;
> exception

DROP TABLE A;
> ok

set autocommit true;
> ok

CREATE TABLE PARENT(ID INT);
> ok

CREATE TABLE CHILD(PID INT);
> ok

INSERT INTO PARENT VALUES(1);
> update count: 1

INSERT INTO CHILD VALUES(2);
> update count: 1

ALTER TABLE CHILD ADD CONSTRAINT CP FOREIGN KEY(PID) REFERENCES PARENT(ID);
> exception

UPDATE CHILD SET PID=1;
> update count: 1

ALTER TABLE CHILD ADD CONSTRAINT CP FOREIGN KEY(PID) REFERENCES PARENT(ID);
> ok

DROP TABLE CHILD, PARENT;
> ok

CREATE TABLE A(ID INT PRIMARY KEY, SK INT);
> ok

INSERT INTO A VALUES(1, 2);
> update count: 1

ALTER TABLE A ADD CONSTRAINT AC FOREIGN KEY(SK) REFERENCES A(ID);
> exception

DROP TABLE A;
> ok

CREATE TABLE TEST(ID INT);
> ok

INSERT INTO TEST VALUES(0), (1), (100);
> update count: 3

ALTER TABLE TEST ADD CONSTRAINT T CHECK ID<100;
> exception

UPDATE TEST SET ID=20 WHERE ID=100;
> update count: 1

ALTER TABLE TEST ADD CONSTRAINT T CHECK ID<100;
> ok

DROP TABLE TEST;
> ok

create table test(id int);
> ok

set autocommit false;
> ok

insert into test values(1);
> update count: 1

prepare commit tx1;
> ok

commit transaction tx1;
> ok

rollback;
> ok

select * from test;
> ID
> --
> 1
> rows: 1

drop table test;
> ok

set autocommit true;
> ok

SELECT 'Hello' ~ 'He.*' T1, 'HELLO' ~ 'He.*' F2, CAST('HELLO' AS VARCHAR_IGNORECASE) ~ 'He.*' T3;
> T1   F2    T3
> ---- ----- ----
> TRUE FALSE TRUE
> rows: 1

SELECT 'Hello' ~* 'He.*' T1, 'HELLO' ~* 'He.*' T2, 'hallo' ~* 'He.*' F3;
> T1   T2   F3
> ---- ---- -----
> TRUE TRUE FALSE
> rows: 1

SELECT 'Hello' !~* 'Ho.*' T1, 'HELLO' !~* 'He.*' F2, 'hallo' !~* 'Ha.*' F3;
> T1   F2    F3
> ---- ----- -----
> TRUE FALSE FALSE
> rows: 1

create table test(parent int primary key, child int, foreign key(child) references (parent));
> ok

insert into test values(1, 1);
> update count: 1

insert into test values(2, 3);
> exception

set autocommit false;
> ok

set referential_integrity false;
> ok

insert into test values(4, 4);
> update count: 1

insert into test values(5, 6);
> update count: 1

set referential_integrity true;
> ok

insert into test values(7, 7), (8, 9);
> exception

set autocommit true;
> ok

drop table test;
> ok

create table test as select 1, space(10) from dual where 1=0 union all select x, cast(space(100) as varchar(101)) d from system_range(1, 100);
> ok

drop table test;
> ok

explain select * from system_range(1, 2) where x=x+1 and x=1;
> PLAN
> ---------------------------------------------------------------------------------------------------------------------------------
> SELECT SYSTEM_RANGE.X FROM SYSTEM_RANGE(1, 2) /* PUBLIC.RANGE_INDEX: X = 1 */ WHERE ((X = 1) AND (X = (X + 1))) AND (1 = (X + 1))
> rows: 1

explain select * from system_range(1, 2) where not (x = 1 and x*2 = 2);
> PLAN
> -------------------------------------------------------------------------------------------------------
> SELECT SYSTEM_RANGE.X FROM SYSTEM_RANGE(1, 2) /* PUBLIC.RANGE_INDEX */ WHERE (X <> 1) OR ((X * 2) <> 2)
> rows: 1

explain select * from system_range(1, 10) where (NOT x >= 5);
> PLAN
> ------------------------------------------------------------------------------------------
> SELECT SYSTEM_RANGE.X FROM SYSTEM_RANGE(1, 10) /* PUBLIC.RANGE_INDEX: X < 5 */ WHERE X < 5
> rows: 1

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (-1, '-1');
> update count: 2

select * from test where name = -1 and name = id;
> ID NAME
> -- ----
> -1 -1
> rows: 1

explain select * from test where name = -1 and name = id;
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = -1 */ WHERE ((NAME = -1) AND (NAME = ID)) AND (ID = -1)
> rows: 1

DROP TABLE TEST;
> ok

select * from system_range(1, 2) where x=x+1 and x=1;
> X
> -
> rows: 0

CREATE TABLE A as select 6 a;
> ok

CREATE TABLE B(B INT PRIMARY KEY);
> ok

CREATE VIEW V(V) AS (SELECT A FROM A UNION SELECT B FROM B);
> ok

create table C as select * from table(c int = (0,6));
> ok

select * from V, C where V.V  = C.C;
> V C
> - -
> 6 6
> rows: 1

drop table A, B, C, V cascade;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, FLAG BOOLEAN, NAME VARCHAR);
> ok

CREATE INDEX IDX_FLAG ON TEST(FLAG, NAME);
> ok

INSERT INTO TEST VALUES(1, TRUE, 'Hello'), (2, FALSE, 'World');
> update count: 2

EXPLAIN SELECT * FROM TEST WHERE FLAG;
> PLAN
> ---------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.FLAG, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.IDX_FLAG: FLAG = TRUE */ WHERE FLAG
> rows: 1

EXPLAIN SELECT * FROM TEST WHERE FLAG AND NAME>'I';
> PLAN
> -----------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.FLAG, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.IDX_FLAG: FLAG = TRUE AND NAME > 'I' */ WHERE FLAG AND (NAME > 'I')
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE test_table (first_col varchar(20), second_col integer);
> ok

insert into test_table values('a', 10), ('a', 4), ('b', 30), ('b', 3);
> update count: 4

CREATE VIEW test_view AS SELECT first_col AS renamed_col, MIN(second_col) AS also_renamed FROM test_table GROUP BY first_col;
> ok

SELECT * FROM test_view WHERE renamed_col = 'a';
> RENAMED_COL ALSO_RENAMED
> ----------- ------------
> a           4
> rows: 1

drop view test_view;
> ok

drop table test_table;
> ok

create table test(id int);
> ok

explain select id+1 a from test group by id+1;
> PLAN
> ---------------------------------------------------------------------------------
> SELECT (ID + 1) AS A FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ GROUP BY ID + 1
> rows: 1

drop table test;
> ok

set autocommit off;
> ok

set search_path = public, information_schema;
> ok

select table_name from tables where 1=0;
> TABLE_NAME
> ----------
> rows: 0

set search_path = public;
> ok

set autocommit on;
> ok

create table script.public.x(a int);
> ok

select * from script.PUBLIC.x;
> A
> -
> rows: 0

create index script.public.idx on script.public.x(a);
> ok

drop table script.public.x;
> ok

create table d(d double, r real);
> ok

insert into d(d, d, r) values(1.1234567890123456789, 1.1234567890123456789, 3);
> exception

insert into d values(1.1234567890123456789, 1.1234567890123456789);
> update count: 1

select r+d, r+r, d+d from d;
> R + D             R + R     D + D
> ----------------- --------- ------------------
> 2.246913624759111 2.2469137 2.2469135780246914
> rows: 1

drop table d;
> ok

create table test(id int, c char(5), v varchar(5));
> ok

insert into test set id = 1, c = 'a', v = 'a';
> update count: 1

insert into test set id = 2, c = 'a ', v = 'a ';
> update count: 1

insert into test set id = 3, c = 'abcde      ', v = 'abcde';
> update count: 1

select distinct length(c) from test order by length(c);
> LENGTH(C)
> ---------
> 1
> 5
> rows (ordered): 2

select id, c, v, length(c), length(v) from test order by id;
> ID C     V     LENGTH(C) LENGTH(V)
> -- ----- ----- --------- ---------
> 1  a     a     1         1
> 2  a     a     1         2
> 3  abcde abcde 5         5
> rows (ordered): 3

select id from test where c='a' order by id;
> ID
> --
> 1
> 2
> rows (ordered): 2

select id from test where c='a ' order by id;
> ID
> --
> 1
> 2
> rows (ordered): 2

select id from test where c=v order by id;
> ID
> --
> 1
> 2
> 3
> rows (ordered): 3

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255), C INT);
> ok

INSERT INTO TEST VALUES(1, '10', NULL), (2, '0', NULL);
> update count: 2

SELECT LEAST(ID, C, NAME), GREATEST(ID, C, NAME), LEAST(NULL, C), GREATEST(NULL, NULL), ID FROM TEST ORDER BY ID;
> LEAST(ID, C, NAME) GREATEST(ID, C, NAME) LEAST(NULL, C) NULL ID
> ------------------ --------------------- -------------- ---- --
> 1                  10                    null           null 1
> 0                  2                     null           null 2
> rows (ordered): 2

DROP TABLE IF EXISTS TEST;
> ok

create table people (family varchar(1) not null, person varchar(1) not null);
> ok

create table cars (family varchar(1) not null, car varchar(1) not null);
> ok

insert into people values(1, 1), (2, 1), (2, 2), (3, 1), (5, 1);
> update count: 5

insert into cars values(2, 1), (2, 2), (3, 1), (3, 2), (3, 3), (4, 1);
> update count: 6

select family, (select count(car) from cars where cars.family = people.family) as x
from people group by family order by family;
> FAMILY X
> ------ -
> 1      0
> 2      2
> 3      3
> 5      0
> rows (ordered): 4

drop table people, cars;
> ok

select (1, 2);
> 1, 2
> ------
> (1, 2)
> rows: 1

create table array_test(x array);
> ok

insert into array_test values((1, 2, 3)), ((2, 3, 4));
> update count: 2

select * from array_test where x = (1, 2, 3);
> X
> ---------
> (1, 2, 3)
> rows: 1

drop table array_test;
> ok

select * from (select 1), (select 2);
> 1 2
> - -
> 1 2
> rows: 1

create table t1(c1 int, c2 int);
> ok

create table t2(c1 int, c2 int);
> ok

insert into t1 values(1, null), (2, 2), (3, 3);
> update count: 3

insert into t2 values(1, 1), (1, 2), (2, null), (3, 3);
> update count: 4

select * from t2 where c1 not in(select c2 from t1);
> C1 C2
> -- --
> rows: 0

select * from t2 where c1 not in(null, 2, 3);
> C1 C2
> -- --
> rows: 0

select * from t1 where c2 not in(select c1 from t2);
> C1 C2
> -- --
> rows: 0

select * from t1 where not exists(select * from t2 where t1.c2=t2.c1);
> C1 C2
> -- ----
> 1  null
> rows: 1

drop table t1;
> ok

drop table t2;
> ok

create constant abc value 1;
> ok

call abc;
> 1
> -
> 1
> rows: 1

drop all objects;
> ok

call abc;
> exception

create table FOO(id integer primary key);
> ok

create table BAR(fooId integer);
> ok

alter table bar add foreign key (fooId) references foo (id);
> ok

truncate table bar;
> ok

truncate table foo;
> exception

drop table bar, foo;
> ok

CREATE TABLE test (family_name VARCHAR_IGNORECASE(63) NOT NULL);
> ok

INSERT INTO test VALUES('Smith'), ('de Smith'), ('el Smith'), ('von Smith');
> update count: 4

SELECT * FROM test WHERE family_name IN ('de Smith', 'Smith');
> FAMILY_NAME
> -----------
> Smith
> de Smith
> rows: 2

SELECT * FROM test WHERE family_name BETWEEN 'D' AND 'T';
> FAMILY_NAME
> -----------
> Smith
> de Smith
> el Smith
> rows: 3

CREATE INDEX family_name ON test(family_name);
> ok

SELECT * FROM test WHERE family_name IN ('de Smith', 'Smith');
> FAMILY_NAME
> -----------
> Smith
> de Smith
> rows: 2

drop table test;
> ok

create memory table test(id int primary key, data clob);
> ok

insert into test values(1, 'abc' || space(20));
> update count: 1

script nopasswords nosettings blocksize 10;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CALL SYSTEM_COMBINE_BLOB(-1);
> CREATE ALIAS IF NOT EXISTS SYSTEM_COMBINE_BLOB FOR "org.h2.command.dml.ScriptCommand.combineBlob";
> CREATE ALIAS IF NOT EXISTS SYSTEM_COMBINE_CLOB FOR "org.h2.command.dml.ScriptCommand.combineClob";
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, DATA CLOB );
> CREATE PRIMARY KEY SYSTEM_LOB_STREAM_PRIMARY_KEY ON SYSTEM_LOB_STREAM(ID, PART);
> CREATE TABLE IF NOT EXISTS SYSTEM_LOB_STREAM(ID INT NOT NULL, PART INT NOT NULL, CDATA VARCHAR, BDATA BINARY);
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> DROP ALIAS IF EXISTS SYSTEM_COMBINE_BLOB;
> DROP ALIAS IF EXISTS SYSTEM_COMBINE_CLOB;
> DROP TABLE IF EXISTS SYSTEM_LOB_STREAM;
> INSERT INTO PUBLIC.TEST(ID, DATA) VALUES (1, SYSTEM_COMBINE_CLOB(0));
> INSERT INTO SYSTEM_LOB_STREAM VALUES(0, 0, 'abc ', NULL);
> INSERT INTO SYSTEM_LOB_STREAM VALUES(0, 1, ' ', NULL);
> INSERT INTO SYSTEM_LOB_STREAM VALUES(0, 2, ' ', NULL);
> rows: 16

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');
> update count: 2

SELECT DISTINCT * FROM TEST ORDER BY ID;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

DROP TABLE TEST;
> ok

create table Foo (A varchar(20), B integer);
> ok

insert into Foo (A, B) values ('abcd', 1), ('abcd', 2);
> update count: 2

select * from Foo where A like 'abc%' escape '\' AND B=1;
> A    B
> ---- -
> abcd 1
> rows: 1

drop table Foo;
> ok

create table test(id int, d timestamp);
> ok

insert into test values(1, '2006-01-01 12:00:00.000');
> update count: 1

insert into test values(1, '1999-12-01 23:59:00.000');
> update count: 1

select * from test where d= '1999-12-01 23:59:00.000';
> ID D
> -- -------------------
> 1  1999-12-01 23:59:00
> rows: 1

select * from test where d= timestamp '2006-01-01 12:00:00.000';
> ID D
> -- -------------------
> 1  2006-01-01 12:00:00
> rows: 1

drop table test;
> ok

create table test(id int, b binary);
> ok

insert into test values(1, 'face');
> update count: 1

select * from test where b = 'FaCe';
> ID B
> -- ----
> 1  face
> rows: 1

drop table test;
> ok

create sequence main_seq;
> ok

create schema "TestSchema";
> ok

create sequence "TestSchema"."TestSeq";
> ok

create sequence "TestSchema"."ABC";
> ok

select currval('main_seq'), currval('TestSchema', 'TestSeq'), nextval('TestSchema', 'ABC');
> CURRVAL('main_seq') CURRVAL('TestSchema', 'TestSeq') NEXTVAL('TestSchema', 'ABC')
> ------------------- -------------------------------- ----------------------------
> 0                   0                                1
> rows: 1

set autocommit off;
> ok

set schema "TestSchema";
> ok

select nextval('abc'), currval('Abc'), nextval('TestSchema', 'ABC');
> NEXTVAL('abc') CURRVAL('Abc') NEXTVAL('TestSchema', 'ABC')
> -------------- -------------- ----------------------------
> 2              2              3
> rows: 1

set schema public;
> ok

drop schema "TestSchema" cascade;
> ok

drop sequence main_seq;
> ok

create sequence "test";
> ok

select nextval('test');
> NEXTVAL('test')
> ---------------
> 1
> rows: 1

drop sequence "test";
> ok

set autocommit on;
> ok

CREATE TABLE parent(id int PRIMARY KEY);
> ok

CREATE TABLE child(parentid int REFERENCES parent);
> ok

select * from INFORMATION_SCHEMA.CROSS_REFERENCES;
> PKTABLE_CATALOG PKTABLE_SCHEMA PKTABLE_NAME PKCOLUMN_NAME FKTABLE_CATALOG FKTABLE_SCHEMA FKTABLE_NAME FKCOLUMN_NAME ORDINAL_POSITION UPDATE_RULE DELETE_RULE FK_NAME      PK_NAME       DEFERRABILITY
> --------------- -------------- ------------ ------------- --------------- -------------- ------------ ------------- ---------------- ----------- ----------- ------------ ------------- -------------
> SCRIPT          PUBLIC         PARENT       ID            SCRIPT          PUBLIC         CHILD        PARENTID      1                1           1           CONSTRAINT_3 PRIMARY_KEY_8 7
> rows: 1

ALTER TABLE parent ADD COLUMN name varchar;
> ok

select * from INFORMATION_SCHEMA.CROSS_REFERENCES;
> PKTABLE_CATALOG PKTABLE_SCHEMA PKTABLE_NAME PKCOLUMN_NAME FKTABLE_CATALOG FKTABLE_SCHEMA FKTABLE_NAME FKCOLUMN_NAME ORDINAL_POSITION UPDATE_RULE DELETE_RULE FK_NAME      PK_NAME        DEFERRABILITY
> --------------- -------------- ------------ ------------- --------------- -------------- ------------ ------------- ---------------- ----------- ----------- ------------ -------------- -------------
> SCRIPT          PUBLIC         PARENT       ID            SCRIPT          PUBLIC         CHILD        PARENTID      1                1           1           CONSTRAINT_3 PRIMARY_KEY_82 7
> rows: 1

drop table parent, child;
> ok

create table test(id int);
> ok

create schema TEST_SCHEMA;
> ok

set autocommit false;
> ok

set schema TEST_SCHEMA;
> ok

create table test(id int, name varchar);
> ok

explain select * from test;
> PLAN
> --------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME FROM TEST_SCHEMA.TEST /* TEST_SCHEMA.TEST.tableScan */
> rows: 1

explain select * from public.test;
> PLAN
> -----------------------------------------------------------
> SELECT TEST.ID FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */
> rows: 1

drop schema TEST_SCHEMA cascade;
> ok

set autocommit true;
> ok

set schema public;
> ok

select * from test;
> ID
> --
> rows: 0

drop table test;
> ok

create table content(thread_id int, parent_id int);
> ok

alter table content add constraint content_parent_id check (parent_id = thread_id) or (parent_id is null) or ( parent_id in (select thread_id from content));
> ok

create index content_thread_id ON content(thread_id);
> ok

insert into content values(0, 0), (0, 0);
> update count: 2

insert into content values(0, 1);
> exception

insert into content values(1, 1), (2, 2);
> update count: 2

insert into content values(2, 1);
> update count: 1

insert into content values(2, 3);
> exception

drop table content;
> ok

select x/10 y from system_range(1, 100) group by x/10;
> Y
> --
> 0
> 1
> 10
> 2
> 3
> 4
> 5
> 6
> 7
> 8
> 9
> rows: 11

select timestamp '2001-02-03T10:30:33';
> TIMESTAMP '2001-02-03 10:30:33'
> -------------------------------
> 2001-02-03 10:30:33
> rows: 1

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');
> update count: 2

select * from test where id in (select id from test);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

select * from test where id in ((select id from test));
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

select * from test where id in (((select id from test)));
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

DROP TABLE TEST;
> ok

create table test(id int);
> ok

insert into test (select x from system_range(1, 100));
> update count: 100

select id/1000 from test group by id/1000;
> ID / 1000
> ---------
> 0
> rows: 1

select id/(10*100) from test group by id/(10*100);
> ID / 1000
> ---------
> 0
> rows: 1

select id/1000 from test group by id/100;
> exception

drop table test;
> ok

select (x/10000) from system_range(10, 20) group by (x/10000);
> X / 10000
> ---------
> 0
> rows: 1

select sum(x), (x/10) from system_range(10, 100) group by (x/10);
> SUM(X) X / 10
> ------ ------
> 100    10
> 145    1
> 245    2
> 345    3
> 445    4
> 545    5
> 645    6
> 745    7
> 845    8
> 945    9
> rows: 10

CREATE FORCE VIEW ADDRESS_VIEW AS SELECT * FROM ADDRESS;
> ok

CREATE memory TABLE ADDRESS(ID INT);
> ok

alter view address_view recompile;
> ok

alter view if exists address_view recompile;
> ok

alter view if exists does_not_exist recompile;
> ok

select * from ADDRESS_VIEW;
> ID
> --
> rows: 0

drop view address_view;
> ok

drop table address;
> ok

CREATE ALIAS PARSE_INT2 FOR "java.lang.Integer.parseInt(java.lang.String, int)";
> ok

select min(SUBSTRING(random_uuid(), 15,1)='4') from system_range(1, 10);
> MIN(SUBSTRING(RANDOM_UUID(), 15, 1) = '4')
> ------------------------------------------
> TRUE
> rows: 1

select min(8=bitand(12, PARSE_INT2(SUBSTRING(random_uuid(), 20,1), 16))) from system_range(1, 10);
> MIN(8 = BITAND(12, PUBLIC.PARSE_INT2(SUBSTRING(RANDOM_UUID(), 20, 1), 16)))
> ---------------------------------------------------------------------------
> TRUE
> rows: 1

select BITGET(x, 0) AS IS_SET from system_range(1, 2);
> IS_SET
> ------
> FALSE
> TRUE
> rows: 2

drop alias PARSE_INT2;
> ok

create memory table test(name varchar check(name = upper(name)));
> ok

insert into test values(null);
> update count: 1

insert into test values('aa');
> exception

insert into test values('AA');
> update count: 1

script nodata nopasswords nosettings;
> SCRIPT
> ---------------------------------------------------------------------------
> -- 2 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE PUBLIC.TEST( NAME VARCHAR CHECK (NAME = UPPER(NAME)) );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 3

drop table test;
> ok

create domain email as varchar(200) check (position('@' in value) > 1);
> ok

create domain gmail as email default '@gmail.com' check (position('gmail' in value) > 1);
> ok

create memory table address(id int primary key, name email, name2 gmail);
> ok

insert into address(id, name, name2) values(1, 'test@abc', 'test@gmail.com');
> update count: 1

insert into address(id, name, name2) values(2, 'test@abc', 'test@acme');
> exception

insert into address(id, name, name2) values(3, 'test_abc', 'test@gmail');
> exception

insert into address2(name) values('test@abc');
> exception

CREATE DOMAIN STRING AS VARCHAR(255) DEFAULT '' NOT NULL;
> ok

CREATE DOMAIN IF NOT EXISTS STRING AS VARCHAR(255) DEFAULT '' NOT NULL;
> ok

CREATE DOMAIN STRING1 AS VARCHAR NULL;
> ok

CREATE DOMAIN STRING2 AS VARCHAR NOT NULL;
> ok

CREATE DOMAIN STRING3 AS VARCHAR DEFAULT '<empty>';
> ok

create domain string_x as string3;
> ok

create memory table test(a string, b string1, c string2, d string3);
> ok

insert into test(c) values('x');
> update count: 1

select * from test;
> A B    C D
> - ---- - -------
>   null x <empty>
> rows: 1

select DOMAIN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, PRECISION, SCALE, TYPE_NAME, SELECTIVITY, CHECK_CONSTRAINT, REMARKS, SQL from information_schema.domains;
> DOMAIN_NAME COLUMN_DEFAULT IS_NULLABLE DATA_TYPE PRECISION  SCALE TYPE_NAME SELECTIVITY CHECK_CONSTRAINT                                                REMARKS SQL
> ----------- -------------- ----------- --------- ---------- ----- --------- ----------- --------------------------------------------------------------- ------- ------------------------------------------------------------------------------------------------------------------------------
> EMAIL       null           YES         12        200        0     VARCHAR   50          (POSITION('@', VALUE) > 1)                                              CREATE DOMAIN EMAIL AS VARCHAR(200) CHECK (POSITION('@', VALUE) > 1)
> GMAIL       '@gmail.com'   YES         12        200        0     VARCHAR   50          ((POSITION('@', VALUE) > 1) AND (POSITION('gmail', VALUE) > 1))         CREATE DOMAIN GMAIL AS VARCHAR(200) DEFAULT '@gmail.com' CHECK ((POSITION('@', VALUE) > 1) AND (POSITION('gmail', VALUE) > 1))
> STRING      ''             NO          12        255        0     VARCHAR   50                                                                                  CREATE DOMAIN STRING AS VARCHAR(255) DEFAULT '' NOT NULL
> STRING1     null           YES         12        2147483647 0     VARCHAR   50                                                                                  CREATE DOMAIN STRING1 AS VARCHAR
> STRING2     null           NO          12        2147483647 0     VARCHAR   50                                                                                  CREATE DOMAIN STRING2 AS VARCHAR NOT NULL
> STRING3     '<empty>'      YES         12        2147483647 0     VARCHAR   50                                                                                  CREATE DOMAIN STRING3 AS VARCHAR DEFAULT '<empty>'
> STRING_X    '<empty>'      YES         12        2147483647 0     VARCHAR   50                                                                                  CREATE DOMAIN STRING_X AS VARCHAR DEFAULT '<empty>'
> rows: 7

script nodata nopasswords nosettings;
> SCRIPT
> ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.ADDRESS;
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.ADDRESS ADD CONSTRAINT PUBLIC.CONSTRAINT_E PRIMARY KEY(ID);
> CREATE DOMAIN EMAIL AS VARCHAR(200) CHECK (POSITION('@', VALUE) > 1);
> CREATE DOMAIN GMAIL AS VARCHAR(200) DEFAULT '@gmail.com' CHECK ((POSITION('@', VALUE) > 1) AND (POSITION('gmail', VALUE) > 1));
> CREATE DOMAIN STRING AS VARCHAR(255) DEFAULT '' NOT NULL;
> CREATE DOMAIN STRING1 AS VARCHAR;
> CREATE DOMAIN STRING2 AS VARCHAR NOT NULL;
> CREATE DOMAIN STRING3 AS VARCHAR DEFAULT '<empty>';
> CREATE DOMAIN STRING_X AS VARCHAR DEFAULT '<empty>';
> CREATE MEMORY TABLE PUBLIC.ADDRESS( ID INT NOT NULL, NAME VARCHAR(200) CHECK (POSITION('@', NAME) > 1), NAME2 VARCHAR(200) DEFAULT '@gmail.com' CHECK ((POSITION('@', NAME2) > 1) AND (POSITION('gmail', NAME2) > 1)) );
> CREATE MEMORY TABLE PUBLIC.TEST( A VARCHAR(255) DEFAULT '' NOT NULL, B VARCHAR, C VARCHAR NOT NULL, D VARCHAR DEFAULT '<empty>' );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 13

drop table test;
> ok

drop domain string;
> ok

drop domain string1;
> ok

drop domain string2;
> ok

drop domain string3;
> ok

drop domain string_x;
> ok

drop table address;
> ok

drop domain email;
> ok

drop domain gmail;
> ok

create force view address_view as select * from address;
> ok

create table address(id identity, name varchar check instr(value, '@') > 1);
> exception

create table address(id identity, name varchar check instr(name, '@') > 1);
> ok

drop view if exists address_view;
> ok

drop table address;
> ok

create memory table a(k10 blob(10k), m20 blob(20m), g30 clob(30g));
> ok

script NODATA NOPASSWORDS NOSETTINGS drop;
> SCRIPT
> -------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.A;
> CREATE MEMORY TABLE PUBLIC.A( K10 BLOB(10240), M20 BLOB(20971520), G30 CLOB(32212254720) );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> DROP TABLE IF EXISTS PUBLIC.A CASCADE;
> rows: 4

create table b();
> ok

create table c();
> ok

drop table information_schema.columns;
> exception

create table columns as select * from information_schema.columns;
> ok

create table tables as select * from information_schema.tables where false;
> ok

create table dual2 as select 1 from dual;
> ok

select * from dual2;
> 1
> -
> 1
> rows: 1

drop table dual2, columns, tables;
> ok

drop table a, a;
> ok

drop table b, c;
> ok

CREATE SCHEMA CONST;
> ok

CREATE CONSTANT IF NOT EXISTS ONE VALUE 1;
> ok

COMMENT ON CONSTANT ONE IS 'Eins';
> ok

CREATE CONSTANT IF NOT EXISTS ONE VALUE 1;
> ok

CREATE CONSTANT CONST.ONE VALUE 1;
> ok

SELECT CONSTANT_SCHEMA, CONSTANT_NAME, DATA_TYPE, REMARKS, SQL FROM INFORMATION_SCHEMA.CONSTANTS;
> CONSTANT_SCHEMA CONSTANT_NAME DATA_TYPE REMARKS SQL
> --------------- ------------- --------- ------- ---
> CONST           ONE           4                 1
> PUBLIC          ONE           4         Eins    1
> rows: 2

SELECT ONE, CONST.ONE FROM DUAL;
> 1 1
> - -
> 1 1
> rows: 1

COMMENT ON CONSTANT ONE IS NULL;
> ok

DROP SCHEMA CONST CASCADE;
> ok

SELECT CONSTANT_SCHEMA, CONSTANT_NAME, DATA_TYPE, REMARKS, SQL FROM INFORMATION_SCHEMA.CONSTANTS;
> CONSTANT_SCHEMA CONSTANT_NAME DATA_TYPE REMARKS SQL
> --------------- ------------- --------- ------- ---
> PUBLIC          ONE           4                 1
> rows: 1

DROP CONSTANT ONE;
> ok

DROP CONSTANT IF EXISTS ONE;
> ok

DROP CONSTANT IF EXISTS ONE;
> ok

CREATE TABLE A (ID_A int primary key);
> ok

CREATE TABLE B (ID_B int primary key);
> ok

CREATE TABLE C (ID_C int primary key);
> ok

insert into A values (1);
> update count: 1

insert into A values (2);
> update count: 1

insert into B values (1);
> update count: 1

insert into C values (1);
> update count: 1

SELECT * FROM C WHERE NOT EXISTS ((SELECT ID_A FROM A) EXCEPT (SELECT ID_B FROM B));
> ID_C
> ----
> rows: 0

(SELECT ID_A FROM A) EXCEPT (SELECT ID_B FROM B);
> ID_A
> ----
> 2
> rows: 1

drop table a;
> ok

drop table b;
> ok

drop table c;
> ok

CREATE TABLE X (ID INTEGER PRIMARY KEY);
> ok

insert into x values(0), (1), (10);
> update count: 3

SELECT t1.ID, (SELECT t1.id || ':' || AVG(t2.ID) FROM X t2) FROM X t1;
> ID SELECT ((T1.ID || ':') || AVG(T2.ID)) FROM PUBLIC.X T2 /* PUBLIC.X.tableScan */ /* scanCount: 4 */
> -- --------------------------------------------------------------------------------------------------
> 0  0:3
> 1  1:3
> 10 10:3
> rows: 3

drop table x;
> ok

select (select t1.x from system_range(1,1) t2) from system_range(1,1) t1;
> SELECT T1.X FROM SYSTEM_RANGE(1, 1) T2 /* PUBLIC.RANGE_INDEX */ /* scanCount: 2 */
> ----------------------------------------------------------------------------------
> 1
> rows: 1

create table test(id int primary key, name varchar);
> ok

insert into test values(rownum, '11'), (rownum, '22'), (rownum, '33');
> update count: 3

select * from test order by id;
> ID NAME
> -- ----
> 1  11
> 2  22
> 3  33
> rows (ordered): 3

select rownum, (select count(*) from test), rownum from test;
> ROWNUM() SELECT COUNT(*) FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ /* direct lookup */ ROWNUM()
> -------- -------------------------------------------------------------------------------- --------
> 1        3                                                                                1
> 2        3                                                                                2
> 3        3                                                                                3
> rows: 3

delete from test t0 where rownum<2;
> update count: 1

select rownum, * from (select * from test where id>1 order by id desc);
> ROWNUM() ID NAME
> -------- -- ----
> 1        3  33
> 2        2  22
> rows (ordered): 2

update test set name='x' where rownum<2;
> update count: 1

select * from test;
> ID NAME
> -- ----
> 2  x
> 3  33
> rows: 2

merge into test values(2, 'r' || rownum), (10, rownum), (11, rownum);
> update count: 3

select * from test;
> ID NAME
> -- ----
> 10 2
> 11 3
> 2  r1
> 3  33
> rows: 4

call rownum;
> ROWNUM()
> --------
> 1
> rows: 1

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

create index idx_test_name on test(name);
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

set ignorecase true;
> ok

CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

create unique index idx_test2_name on test2(name);
> ok

INSERT INTO TEST2 VALUES(1, 'HElLo');
> update count: 1

INSERT INTO TEST2 VALUES(2, 'World');
> update count: 1

INSERT INTO TEST2 VALUES(3, 'WoRlD');
> exception

drop index idx_test2_name;
> ok

select * from test where name='HELLO';
> ID NAME
> -- ----
> rows: 0

select * from test2 where name='HELLO';
> ID NAME
> -- -----
> 1  HElLo
> rows: 1

select * from test where name like 'HELLO';
> ID NAME
> -- ----
> rows: 0

select * from test2 where name like 'HELLO';
> ID NAME
> -- -----
> 1  HElLo
> rows: 1

explain plan for select * from test2, test where test2.name = test.name;
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST2.ID, TEST2.NAME, TEST.ID, TEST.NAME FROM PUBLIC.TEST2 /* PUBLIC.TEST2.tableScan */ INNER JOIN PUBLIC.TEST /* PUBLIC.IDX_TEST_NAME: NAME = TEST2.NAME */ ON 1=1 WHERE TEST2.NAME = TEST.NAME
> rows: 1

select * from test2, test where test2.name = test.name;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  HElLo 1  Hello
> 2  World 2  World
> rows: 2

explain plan for select * from test, test2 where test2.name = test.name;
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME, TEST2.ID, TEST2.NAME FROM PUBLIC.TEST2 /* PUBLIC.TEST2.tableScan */ INNER JOIN PUBLIC.TEST /* PUBLIC.IDX_TEST_NAME: NAME = TEST2.NAME */ ON 1=1 WHERE TEST2.NAME = TEST.NAME
> rows: 1

select * from test, test2 where test2.name = test.name;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  HElLo
> 2  World 2  World
> rows: 2

create index idx_test2_name on test2(name);
> ok

explain plan for select * from test2, test where test2.name = test.name;
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST2.ID, TEST2.NAME, TEST.ID, TEST.NAME FROM PUBLIC.TEST2 /* PUBLIC.TEST2.tableScan */ INNER JOIN PUBLIC.TEST /* PUBLIC.IDX_TEST_NAME: NAME = TEST2.NAME */ ON 1=1 WHERE TEST2.NAME = TEST.NAME
> rows: 1

select * from test2, test where test2.name = test.name;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  HElLo 1  Hello
> 2  World 2  World
> rows: 2

explain plan for select * from test, test2 where test2.name = test.name;
> PLAN
> ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME, TEST2.ID, TEST2.NAME FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ INNER JOIN PUBLIC.TEST2 /* PUBLIC.IDX_TEST2_NAME: NAME = TEST.NAME */ ON 1=1 WHERE TEST2.NAME = TEST.NAME
> rows: 1

select * from test, test2 where test2.name = test.name;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  HElLo
> 2  World 2  World
> rows: 2

DROP TABLE IF EXISTS TEST;
> ok

DROP TABLE IF EXISTS TEST2;
> ok

set ignorecase false;
> ok

create table test(f1 varchar, f2 varchar);
> ok

insert into test values('abc','222');
> update count: 1

insert into test values('abc','111');
> update count: 1

insert into test values('abc','333');
> update count: 1

SELECT t.f1, t.f2 FROM test t ORDER BY t.f2;
> F1  F2
> --- ---
> abc 111
> abc 222
> abc 333
> rows (ordered): 3

SELECT t1.f1, t1.f2, t2.f1, t2.f2 FROM test t1, test t2 ORDER BY t2.f2;
> F1  F2  F1  F2
> --- --- --- ---
> abc 222 abc 111
> abc 111 abc 111
> abc 333 abc 111
> abc 222 abc 222
> abc 111 abc 222
> abc 333 abc 222
> abc 222 abc 333
> abc 111 abc 333
> abc 333 abc 333
> rows (ordered): 9

drop table if exists test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

explain select t0.id, t1.id from test t0, test t1 order by t0.id, t1.id;
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T0.ID, T1.ID FROM PUBLIC.TEST T0 /* PUBLIC.TEST.tableScan */ INNER JOIN PUBLIC.TEST T1 /* PUBLIC.TEST.tableScan */ ON 1=1 ORDER BY 1, 2
> rows (ordered): 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT id, sum(id) FROM test GROUP BY id ORDER BY id*sum(id);
> ID SUM(ID)
> -- -------
> 1  1
> 2  2
> rows (ordered): 2

select *
from test t1
inner join test t2 on t2.id=t1.id
inner join test t3 on t3.id=t2.id
where exists (select 1 from test t4 where t2.id=t4.id);
> ID NAME  ID NAME  ID NAME
> -- ----- -- ----- -- -----
> 1  Hello 1  Hello 1  Hello
> 2  World 2  World 2  World
> rows: 2

explain select * from test t1 where id in(select id from test t2 where t1.id=t2.id);
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.TEST.tableScan */ WHERE ID IN( SELECT ID FROM PUBLIC.TEST T2 /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID */ WHERE T1.ID = T2.ID)
> rows: 1

select * from test t1 where id in(select id from test t2 where t1.id=t2.id);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(id, id+1);
> PLAN
> -----------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.TEST.tableScan */ WHERE ID IN(ID, (ID + 1))
> rows: 1

select * from test t1 where id in(id, id+1);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(id);
> PLAN
> -----------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.TEST.tableScan */ WHERE ID = ID
> rows: 1

select * from test t1 where id in(id);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(select id from test);
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.PRIMARY_KEY_2: ID IN(SELECT ID FROM PUBLIC.TEST /++ PUBLIC.TEST.tableScan ++/) */ WHERE ID IN( SELECT ID FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */)
> rows: 1

select * from test t1 where id in(select id from test);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(1, select max(id) from test);
> PLAN
> -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.PRIMARY_KEY_2: ID IN(1, (SELECT MAX(ID) FROM PUBLIC.TEST /++ PUBLIC.TEST.tableScan ++/ /++ direct lookup ++/)) */ WHERE ID IN(1, (SELECT MAX(ID) FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ /* direct lookup */))
> rows: 1

select * from test t1 where id in(1, select max(id) from test);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(1, select max(id) from test t2 where t1.id=t2.id);
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.TEST.tableScan */ WHERE ID IN(1, (SELECT MAX(ID) FROM PUBLIC.TEST T2 /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID */ WHERE T1.ID = T2.ID))
> rows: 1

select * from test t1 where id in(1, select max(id) from test t2 where t1.id=t2.id);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

DROP TABLE TEST;
> ok

create force view t1 as select * from t1;
> ok

select * from t1;
> exception

drop table t1;
> ok

CREATE TABLE TEST(id INT PRIMARY KEY, foo BIGINT);
> ok

INSERT INTO TEST VALUES(1, 100);
> update count: 1

INSERT INTO TEST VALUES(2, 123456789012345678);
> update count: 1

SELECT * FROM TEST WHERE foo = 123456789014567;
> ID FOO
> -- ---
> rows: 0

DROP TABLE IF EXISTS TEST;
> ok

create table test(v boolean);
> ok

insert into test values(null), (true), (false);
> update count: 3

SELECT CASE WHEN NOT (false IN (null)) THEN false END;
> NULL
> ----
> null
> rows: 1

select a.v as av, b.v as bv, a.v IN (b.v), not a.v IN (b.v) from test a, test b;
> AV    BV    A.V = B.V NOT (A.V = B.V)
> ----- ----- --------- ---------------
> FALSE FALSE TRUE      FALSE
> FALSE TRUE  FALSE     TRUE
> FALSE null  null      null
> TRUE  FALSE FALSE     TRUE
> TRUE  TRUE  TRUE      FALSE
> TRUE  null  null      null
> null  FALSE null      null
> null  TRUE  null      null
> null  null  null      null
> rows: 9

select a.v as av, b.v as bv, a.v IN (b.v, null), not a.v IN (b.v, null) from test a, test b;
> AV    BV    A.V IN(B.V, NULL) NOT (A.V IN(B.V, NULL))
> ----- ----- ----------------- -----------------------
> FALSE FALSE TRUE              FALSE
> FALSE TRUE  null              null
> FALSE null  null              null
> TRUE  FALSE null              null
> TRUE  TRUE  TRUE              FALSE
> TRUE  null  null              null
> null  FALSE null              null
> null  TRUE  null              null
> null  null  null              null
> rows: 9

drop table test;
> ok

SELECT CASE WHEN NOT (false IN (null)) THEN false END;
> NULL
> ----
> null
> rows: 1

create table test(id int);
> ok

insert into test values(1), (2), (3), (4);
> update count: 4

(select * from test a, test b) minus (select * from test a, test b);
> ID ID
> -- --
> rows: 0

drop table test;
> ok

call select 1.0/3.0*3.0, 100.0/2.0, -25.0/100.0, 0.0/3.0, 6.9/2.0, 0.72179425150347250912311550800000 / 5314251955.21;
> SELECT 0.999999999999999999999999990, 50, -0.25, 0, 3.45, 1.35822361752313607260107721120531135706133161972E-10 FROM SYSTEM_RANGE(1, 1) /* PUBLIC.RANGE_INDEX */ /* scanCount: 2 */
> -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> (0.999999999999999999999999990, 50, -0.25, 0, 3.45, 1.35822361752313607260107721120531135706133161972E-10)
> rows: 1

CALL 1 /* comment */ ;;
> 1
> -
> 1
> rows: 1

CALL 1 /* comment */ ;
> 1
> -
> 1
> rows: 1

call /* remark * / * /* ** // end */ 1;
> 1
> -
> 1
> rows: 1

call (select x from dual where x is null);
> SELECT X FROM SYSTEM_RANGE(1, 1) /* PUBLIC.RANGE_INDEX: X IS NULL */ /* scanCount: 1 */ WHERE X IS NULL
> -------------------------------------------------------------------------------------------------------
> null
> rows: 1

create sequence test_seq;
> ok

create table test(id int primary key, parent int);
> ok

create index ni on test(parent);
> ok

alter table test add constraint nu unique(parent);
> ok

alter table test add constraint fk foreign key(parent) references(id);
> ok

select TABLE_NAME, NON_UNIQUE, INDEX_NAME, ORDINAL_POSITION, COLUMN_NAME, CARDINALITY, PRIMARY_KEY from INFORMATION_SCHEMA.INDEXES;
> TABLE_NAME NON_UNIQUE INDEX_NAME    ORDINAL_POSITION COLUMN_NAME CARDINALITY PRIMARY_KEY
> ---------- ---------- ------------- ---------------- ----------- ----------- -----------
> TEST       FALSE      NU_INDEX_2    1                PARENT      0           FALSE
> TEST       FALSE      PRIMARY_KEY_2 1                ID          0           TRUE
> TEST       TRUE       NI            1                PARENT      0           FALSE
> rows: 3

select SEQUENCE_NAME, CURRENT_VALUE, INCREMENT, IS_GENERATED, REMARKS from INFORMATION_SCHEMA.SEQUENCES;
> SEQUENCE_NAME CURRENT_VALUE INCREMENT IS_GENERATED REMARKS
> ------------- ------------- --------- ------------ -------
> TEST_SEQ      0             1         FALSE
> rows: 1

drop table test;
> ok

drop sequence test_seq;
> ok

create table test(id int);
> ok

insert into test values(1), (2);
> update count: 2

select count(*) from test where id in ((select id from test where 1=0));
> COUNT(*)
> --------
> 0
> rows: 1

select count(*) from test where id = ((select id from test where 1=0)+1);
> COUNT(*)
> --------
> 0
> rows: 1

select count(*) from test where id = (select id from test where 1=0);
> COUNT(*)
> --------
> 0
> rows: 1

select count(*) from test where id in ((select id from test));
> COUNT(*)
> --------
> 2
> rows: 1

select count(*) from test where id = ((select id from test));
> exception

select count(*) from test where id = ((select id from test), 1);
> exception

select (select id from test where 1=0) from test;
> SELECT ID FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan: FALSE */ WHERE FALSE
> -------------------------------------------------------------------------
> null
> null
> rows: 2

drop table test;
> ok

select TRIM(' ' FROM '  abc   ') from dual;
> 'abc'
> -----
> abc
> rows: 1

create table test(id int primary key, a boolean);
> ok

insert into test values(1, 'Y');
> update count: 1

call select a from test order by id;
> SELECT A FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2 */ /* scanCount: 2 */ ORDER BY =ID /* index sorted */
> -------------------------------------------------------------------------------------------------------
> TRUE
> rows (ordered): 1

select select a from test order by id;
> SELECT A FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2 */ /* scanCount: 2 */ ORDER BY =ID /* index sorted */
> -------------------------------------------------------------------------------------------------------
> TRUE
> rows (ordered): 1

insert into test values(2, 'N');
> update count: 1

insert into test values(3, '1');
> update count: 1

insert into test values(4, '0');
> update count: 1

insert into test values(5, 'T');
> update count: 1

insert into test values(6, 'F');
> update count: 1

select max(id) from test where id = max(id) group by id;
> exception

select * from test where a=TRUE=a;
> ID A
> -- -----
> 1  TRUE
> 2  FALSE
> 3  TRUE
> 4  FALSE
> 5  TRUE
> 6  FALSE
> rows: 6

drop table test;
> ok

CREATE memory TABLE TEST(ID INT PRIMARY KEY, PARENT INT REFERENCES TEST);
> ok

CREATE memory TABLE s(S_NO VARCHAR(5) PRIMARY KEY, name VARCHAR(16), city VARCHAR(16));
> ok

CREATE memory TABLE p(p_no VARCHAR(5) PRIMARY KEY, descr VARCHAR(16), color VARCHAR(8));
> ok

CREATE memory TABLE sp1(S_NO VARCHAR(5) REFERENCES s, p_no VARCHAR(5) REFERENCES p, qty INT, PRIMARY KEY (S_NO, p_no));
> ok

CREATE memory TABLE sp2(S_NO VARCHAR(5), p_no VARCHAR(5), qty INT, constraint c1 FOREIGN KEY (S_NO) references s, PRIMARY KEY (S_NO, p_no));
> ok

script NOPASSWORDS NOSETTINGS;
> SCRIPT
> -------------------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.P;
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.S;
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.SP1;
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.SP2;
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.P ADD CONSTRAINT PUBLIC.CONSTRAINT_50_0 PRIMARY KEY(P_NO);
> ALTER TABLE PUBLIC.S ADD CONSTRAINT PUBLIC.CONSTRAINT_5 PRIMARY KEY(S_NO);
> ALTER TABLE PUBLIC.SP1 ADD CONSTRAINT PUBLIC.CONSTRAINT_1 FOREIGN KEY(S_NO) REFERENCES PUBLIC.S(S_NO) NOCHECK;
> ALTER TABLE PUBLIC.SP1 ADD CONSTRAINT PUBLIC.CONSTRAINT_14 FOREIGN KEY(P_NO) REFERENCES PUBLIC.P(P_NO) NOCHECK;
> ALTER TABLE PUBLIC.SP1 ADD CONSTRAINT PUBLIC.CONSTRAINT_141 PRIMARY KEY(S_NO, P_NO);
> ALTER TABLE PUBLIC.SP2 ADD CONSTRAINT PUBLIC.C1 FOREIGN KEY(S_NO) REFERENCES PUBLIC.S(S_NO) NOCHECK;
> ALTER TABLE PUBLIC.SP2 ADD CONSTRAINT PUBLIC.CONSTRAINT_1417 PRIMARY KEY(S_NO, P_NO);
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_27 FOREIGN KEY(PARENT) REFERENCES PUBLIC.TEST(ID) NOCHECK;
> CREATE MEMORY TABLE PUBLIC.P( P_NO VARCHAR(5) NOT NULL, DESCR VARCHAR(16), COLOR VARCHAR(8) );
> CREATE MEMORY TABLE PUBLIC.S( S_NO VARCHAR(5) NOT NULL, NAME VARCHAR(16), CITY VARCHAR(16) );
> CREATE MEMORY TABLE PUBLIC.SP1( S_NO VARCHAR(5) NOT NULL, P_NO VARCHAR(5) NOT NULL, QTY INT );
> CREATE MEMORY TABLE PUBLIC.SP2( S_NO VARCHAR(5) NOT NULL, P_NO VARCHAR(5) NOT NULL, QTY INT );
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, PARENT INT );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 20

drop table test;
> ok

drop table sp1;
> ok

drop table sp2;
> ok

drop table s;
> ok

drop table p;
> ok

create table test (id identity, value int not null);
> ok

create primary key on test(id);
> exception

alter table test drop primary key;
> ok

alter table test drop primary key;
> exception

create primary key on test(id, id, id);
> ok

alter table test drop primary key;
> ok

drop table test;
> ok

set autocommit off;
> ok

create local temporary table test (id identity, b int, foreign key(b) references(id));
> ok

drop table test;
> ok

script NOPASSWORDS NOSETTINGS drop;
> SCRIPT
> -----------------------------------------------
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 1

create local temporary table test1 (id identity);
> ok

create local temporary table test2 (id identity);
> ok

alter table test2 add constraint test2_test1 foreign key (id) references test1;
> ok

drop table test1;
> ok

drop table test2;
> ok

create local temporary table test1 (id identity);
> ok

create local temporary table test2 (id identity);
> ok

alter table test2 add constraint test2_test1 foreign key (id) references test1;
> ok

drop table test1;
> ok

drop table test2;
> ok

set autocommit on;
> ok

create table test(id int primary key, ref int, foreign key(ref) references(id));
> ok

insert into test values(1, 1), (2, 2);
> update count: 2

update test set ref=3-ref;
> update count: 2

alter table test add column dummy int;
> ok

insert into test values(4, 4, null);
> update count: 1

drop table test;
> ok

create table test(id int primary key);
> ok

explain select * from test a inner join test b left outer join test c on c.id = a.id;
> PLAN
> ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT A.ID, C.ID, B.ID FROM PUBLIC.TEST A /* PUBLIC.TEST.tableScan */ LEFT OUTER JOIN PUBLIC.TEST C /* PUBLIC.PRIMARY_KEY_2: ID = A.ID */ ON C.ID = A.ID INNER JOIN PUBLIC.TEST B /* PUBLIC.TEST.tableScan */ ON 1=1
> rows: 1

SELECT T.ID FROM TEST "T";
> ID
> --
> rows: 0

SELECT T."ID" FROM TEST "T";
> ID
> --
> rows: 0

SELECT "T".ID FROM TEST "T";
> ID
> --
> rows: 0

SELECT "T"."ID" FROM TEST "T";
> ID
> --
> rows: 0

SELECT T.ID FROM "TEST" T;
> ID
> --
> rows: 0

SELECT T."ID" FROM "TEST" T;
> ID
> --
> rows: 0

SELECT "T".ID FROM "TEST" T;
> ID
> --
> rows: 0

SELECT "T"."ID" FROM "TEST" T;
> ID
> --
> rows: 0

SELECT T.ID FROM "TEST" "T";
> ID
> --
> rows: 0

SELECT T."ID" FROM "TEST" "T";
> ID
> --
> rows: 0

SELECT "T".ID FROM "TEST" "T";
> ID
> --
> rows: 0

SELECT "T"."ID" FROM "TEST" "T";
> ID
> --
> rows: 0

select "TEST".id from test;
> ID
> --
> rows: 0

select test."ID" from test;
> ID
> --
> rows: 0

select test."id" from test;
> exception

select "TEST"."ID" from test;
> ID
> --
> rows: 0

select "test"."ID" from test;
> exception

select public."TEST".id from test;
> ID
> --
> rows: 0

select public.test."ID" from test;
> ID
> --
> rows: 0

select public."TEST"."ID" from test;
> ID
> --
> rows: 0

select public."test"."ID" from test;
> exception

select "PUBLIC"."TEST".id from test;
> ID
> --
> rows: 0

select "PUBLIC".test."ID" from test;
> ID
> --
> rows: 0

select public."TEST"."ID" from test;
> ID
> --
> rows: 0

select "public"."TEST"."ID" from test;
> exception

drop table test;
> ok

create schema s authorization sa;
> ok

create memory table s.test(id int);
> ok

create index if not exists idx_id on s.test(id);
> ok

create index if not exists idx_id on s.test(id);
> ok

alter index s.idx_id rename to s.x;
> ok

alter index if exists s.idx_id rename to s.x;
> ok

alter index if exists s.x rename to s.index_id;
> ok

alter sequence if exists s.seq restart with 10;
> ok

create sequence s.seq cache 0;
> ok

alter sequence if exists s.seq restart with 3;
> ok

select s.seq.nextval as x;
> X
> -
> 3
> rows: 1

drop sequence s.seq;
> ok

create sequence s.seq cache 0;
> ok

alter sequence s.seq restart with 10;
> ok

alter table s.test add constraint cu_id unique(id);
> ok

alter table s.test add name varchar;
> ok

alter table s.test drop column name;
> ok

alter table s.test drop constraint cu_id;
> ok

alter table s.test rename to testtab;
> ok

alter table s.testtab rename to test;
> ok

create trigger test_trigger before insert on s.test call "org.h2.test.db.TestTriggersConstraints";
> ok

script NOPASSWORDS NOSETTINGS drop;
> SCRIPT
> ---------------------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM S.TEST;
> CREATE FORCE TRIGGER S.TEST_TRIGGER BEFORE INSERT ON S.TEST QUEUE 1024 CALL "org.h2.test.db.TestTriggersConstraints";
> CREATE INDEX S.INDEX_ID ON S.TEST(ID);
> CREATE MEMORY TABLE S.TEST( ID INT );
> CREATE SCHEMA IF NOT EXISTS S AUTHORIZATION SA;
> CREATE SEQUENCE S.SEQ START WITH 10 CACHE 1;
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> DROP SEQUENCE IF EXISTS S.SEQ;
> DROP TABLE IF EXISTS S.TEST CASCADE;
> rows: 9

drop trigger s.test_trigger;
> ok

drop schema s cascade;
> ok

CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255), y int as id+1);
> ok

INSERT INTO TEST(id, name) VALUES(1, 'Hello');
> update count: 1

create index idx_n_id on test(name, id);
> ok

alter table test add constraint abc foreign key(id) references (id);
> ok

alter table test rename column id to i;
> ok

script NOPASSWORDS NOSETTINGS drop;
> SCRIPT
> ---------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.ABC FOREIGN KEY(I) REFERENCES PUBLIC.TEST(I) NOCHECK;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(I);
> CREATE INDEX PUBLIC.IDX_N_ID ON PUBLIC.TEST(NAME, I);
> CREATE MEMORY TABLE PUBLIC.TEST( I INT NOT NULL, NAME VARCHAR(255), Y INT AS (I + 1) );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> DROP TABLE IF EXISTS PUBLIC.TEST CASCADE;
> INSERT INTO PUBLIC.TEST(I, NAME, Y) VALUES (1, 'Hello', 2);
> rows: 8

INSERT INTO TEST(i, name) VALUES(2, 'World');
> update count: 1

SELECT * FROM TEST ORDER BY I;
> I NAME  Y
> - ----- -
> 1 Hello 2
> 2 World 3
> rows (ordered): 2

UPDATE TEST SET NAME='Hi' WHERE I=1;
> update count: 1

DELETE FROM TEST t0 WHERE t0.I=2;
> update count: 1

drop table test;
> ok

create table test(current int);
> ok

select current from test;
> CURRENT
> -------
> rows: 0

drop table test;
> ok

CREATE table my_table(my_int integer, my_char varchar);
> ok

INSERT INTO my_table VALUES(1, 'Testing');
> update count: 1

ALTER TABLE my_table ALTER COLUMN my_int RENAME to my_new_int;
> ok

SELECT my_new_int FROM my_table;
> MY_NEW_INT
> ----------
> 1
> rows: 1

UPDATE my_table SET my_new_int = 33;
> update count: 1

SELECT * FROM my_table;
> MY_NEW_INT MY_CHAR
> ---------- -------
> 33         Testing
> rows: 1

DROP TABLE my_table;
> ok

create sequence seq1;
> ok

create table test(ID INT default next value for seq1);
> ok

drop sequence seq1;
> exception

alter table test add column name varchar;
> ok

insert into test(name) values('Hello');
> update count: 1

select * from test;
> ID NAME
> -- -----
> 1  Hello
> rows: 1

drop table test;
> ok

drop sequence seq1;
> ok

create table test(a int primary key, b int, c int);
> ok

create unique index idx_ba on test(b, a);
> ok

alter table test add constraint abc foreign key(c, a) references test(b, a);
> ok

insert into test values(1, 1, null);
> update count: 1

drop table test;
> ok

create table ADDRESS (ADDRESS_ID int primary key, ADDRESS_TYPE int not null, SERVER_ID int not null);
> ok

create unique index idx_a on address(ADDRESS_TYPE, SERVER_ID);
> ok

create table SERVER (SERVER_ID int primary key, SERVER_TYPE int not null, ADDRESS_TYPE int);
> ok

alter table ADDRESS add constraint addr foreign key (SERVER_ID) references SERVER;
> ok

alter table SERVER add constraint server_const foreign key (ADDRESS_TYPE, SERVER_ID) references ADDRESS (ADDRESS_TYPE, SERVER_ID);
> ok

insert into SERVER (SERVER_ID, SERVER_TYPE) values (1, 1);
> update count: 1

drop table address;
> ok

drop table server;
> ok

CREATE TABLE PlanElements(id int primary key, name varchar, parent_id int, foreign key(parent_id) references(id) on delete cascade);
> ok

INSERT INTO PlanElements(id,name,parent_id) VALUES(1, '#1', null), (2, '#1-A', 1), (3, '#1-A-1', 2), (4, '#1-A-2', 2);
> update count: 4

INSERT INTO PlanElements(id,name,parent_id) VALUES(5, '#1-B', 1), (6, '#1-B-1', 5), (7, '#1-B-2', 5);
> update count: 3

INSERT INTO PlanElements(id,name,parent_id) VALUES(8, '#1-C', 1), (9, '#1-C-1', 8), (10, '#1-C-2', 8);
> update count: 3

INSERT INTO PlanElements(id,name,parent_id) VALUES(11, '#1-D', 1), (12, '#1-D-1', 11), (13, '#1-D-2', 11), (14, '#1-D-3', 11);
> update count: 4

INSERT INTO PlanElements(id,name,parent_id) VALUES(15, '#1-E', 1), (16, '#1-E-1', 15), (17, '#1-E-2', 15), (18, '#1-E-3', 15), (19, '#1-E-4', 15);
> update count: 5

DELETE FROM PlanElements WHERE id = 1;
> update count: 1

SELECT * FROM PlanElements;
> ID NAME PARENT_ID
> -- ---- ---------
> rows: 0

DROP TABLE PlanElements;
> ok

CREATE TABLE PARENT(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE TABLE CHILD(ID INT PRIMARY KEY, NAME VARCHAR(255), FOREIGN KEY(NAME) REFERENCES PARENT(ID));
> ok

INSERT INTO PARENT VALUES(1, '1');
> update count: 1

INSERT INTO CHILD VALUES(1, '1');
> update count: 1

INSERT INTO CHILD VALUES(2, 'Hello');
> exception

DROP TABLE IF EXISTS CHILD;
> ok

DROP TABLE IF EXISTS PARENT;
> ok

(SELECT * FROM DUAL) UNION ALL (SELECT * FROM DUAL);
> X
> -
> 1
> 1
> rows: 2

DECLARE GLOBAL TEMPORARY TABLE TEST(ID INT PRIMARY KEY);
> ok

SELECT * FROM TEST;
> ID
> --
> rows: 0

SELECT GROUP_CONCAT(ID) FROM TEST;
> GROUP_CONCAT(ID)
> ----------------
> null
> rows: 1

SELECT * FROM SESSION.TEST;
> ID
> --
> rows: 0

DROP TABLE TEST;
> ok

VALUES(1, 2);
> C1 C2
> -- --
> 1  2
> rows: 1

DROP TABLE IF EXISTS TEST;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT group_concat(name) FROM TEST group by id;
> GROUP_CONCAT(NAME)
> ------------------
> Hello
> World
> rows: 2

drop table test;
> ok

create table test(a int primary key, b int invisible, c int);
> ok

select * from test;
> A C
> - -
> rows: 0

select a, b, c from test;
> A B C
> - - -
> rows: 0

drop table test;
> ok

--- script drop ---------------------------------------------------------------------------------------------
create memory table test (id int primary key, im_ie varchar(10));
> ok

create sequence test_seq;
> ok

script NODATA NOPASSWORDS NOSETTINGS drop;
> SCRIPT
> ---------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, IM_IE VARCHAR(10) );
> CREATE SEQUENCE PUBLIC.TEST_SEQ START WITH 1;
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> DROP SEQUENCE IF EXISTS PUBLIC.TEST_SEQ;
> DROP TABLE IF EXISTS PUBLIC.TEST CASCADE;
> rows: 7

drop sequence test_seq;
> ok

drop table test;
> ok

--- constraints ---------------------------------------------------------------------------------------------
CREATE MEMORY TABLE TEST(ID IDENTITY(100, 10), NAME VARCHAR);
> ok

INSERT INTO TEST(NAME) VALUES('Hello'), ('World');
> update count: 2

SELECT * FROM TEST;
> ID  NAME
> --- -----
> 100 Hello
> 110 World
> rows: 2

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE TEST(ID BIGINT NOT NULL IDENTITY(10, 5), NAME VARCHAR);
> ok

INSERT INTO TEST(NAME) VALUES('Hello'), ('World');
> update count: 2

SELECT * FROM TEST;
> ID NAME
> -- -----
> 10 Hello
> 15 World
> rows: 2

DROP TABLE TEST;
> ok

CREATE CACHED TABLE account(
id INTEGER NOT NULL IDENTITY,
name VARCHAR NOT NULL,
mail_address VARCHAR NOT NULL,
UNIQUE(name),
PRIMARY KEY(id)
);
> ok

CREATE CACHED TABLE label(
id INTEGER NOT NULL IDENTITY,
parent_id INTEGER NOT NULL,
account_id INTEGER NOT NULL,
name VARCHAR NOT NULL,
PRIMARY KEY(id),
UNIQUE(parent_id, name),
UNIQUE(id, account_id),
FOREIGN KEY(account_id) REFERENCES account (id),
FOREIGN KEY(parent_id, account_id) REFERENCES label (id, account_id)
);
> ok

INSERT INTO account VALUES (0, 'example', 'example@example.com');
> update count: 1

INSERT INTO label VALUES ( 0, 0, 0, 'TEST');
> update count: 1

INSERT INTO label VALUES ( 1, 0, 0, 'TEST');
> exception

INSERT INTO label VALUES ( 1, 0, 0, 'TEST1');
> update count: 1

INSERT INTO label VALUES ( 2, 2, 1, 'TEST');
> exception

drop table label;
> ok

drop table account;
> ok

--- constraints and alter table add column ---------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT, PARENTID INT, FOREIGN KEY(PARENTID) REFERENCES(ID));
> ok

INSERT INTO TEST VALUES(0, 0);
> update count: 1

ALTER TABLE TEST ADD COLUMN CHILD_ID INT;
> ok

ALTER TABLE TEST ALTER COLUMN CHILD_ID VARCHAR;
> ok

ALTER TABLE TEST ALTER COLUMN PARENTID VARCHAR;
> ok

ALTER TABLE TEST DROP COLUMN PARENTID;
> ok

ALTER TABLE TEST DROP COLUMN CHILD_ID;
> ok

SELECT * FROM TEST;
> ID
> --
> 0
> rows: 1

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE A(X INT);
> ok

CREATE MEMORY TABLE B(XX INT, CONSTRAINT B2A FOREIGN KEY(XX) REFERENCES A(X));
> ok

CREATE MEMORY TABLE C(X_MASTER INT);
> ok

ALTER TABLE A ADD CONSTRAINT A2C FOREIGN KEY(X) REFERENCES C(X_MASTER);
> ok

insert into c values(1);
> update count: 1

insert into a values(1);
> update count: 1

insert into b values(1);
> update count: 1

ALTER TABLE A ADD COLUMN Y INT;
> ok

insert into c values(2);
> update count: 1

insert into a values(2, 2);
> update count: 1

insert into b values(2);
> update count: 1

DROP TABLE IF EXISTS A;
> ok

DROP TABLE IF EXISTS B;
> ok

DROP TABLE IF EXISTS C;
> ok

--- quoted keywords ---------------------------------------------------------------------------------------------
CREATE TABLE "CREATE"("SELECT" INT, "PRIMARY" INT, "KEY" INT, "INDEX" INT, "ROWNUM" INT, "NEXTVAL" INT, "FROM" INT);
> ok

INSERT INTO "CREATE" default values;
> update count: 1

INSERT INTO "CREATE" default values;
> update count: 1

SELECT "ROWNUM", ROWNUM, "SELECT" "AS", "PRIMARY" AS "X", "KEY", "NEXTVAL", "INDEX", "SELECT" "FROM" FROM "CREATE";
> ROWNUM ROWNUM() AS   X    KEY  NEXTVAL INDEX FROM
> ------ -------- ---- ---- ---- ------- ----- ----
> null   1        null null null null    null  null
> null   2        null null null null    null  null
> rows: 2

DROP TABLE "CREATE";
> ok

--- truncate table ---------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');
> update count: 2

TRUNCATE TABLE TEST;
> ok

SELECT * FROM TEST;
> ID NAME
> -- ----
> rows: 0

DROP TABLE TEST;
> ok

CREATE TABLE PARENT(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

CREATE TABLE CHILD(PARENTID INT, FOREIGN KEY(PARENTID) REFERENCES PARENT(ID), NAME VARCHAR);
> ok

TRUNCATE TABLE CHILD;
> ok

TRUNCATE TABLE PARENT;
> exception

DROP TABLE CHILD;
> ok

DROP TABLE PARENT;
> ok

--- test case for number like string ---------------------------------------------------------------------------------------------
CREATE TABLE test (one bigint primary key, two bigint, three bigint);
> ok

CREATE INDEX two ON test(two);
> ok

INSERT INTO TEST VALUES(1, 2, 3), (10, 20, 30), (100, 200, 300);
> update count: 3

INSERT INTO TEST VALUES(2, 6, 9), (20, 60, 90), (200, 600, 900);
> update count: 3

SELECT * FROM test WHERE one LIKE '2%';
> ONE TWO THREE
> --- --- -----
> 2   6   9
> 20  60  90
> 200 600 900
> rows: 3

SELECT * FROM test WHERE two LIKE '2%';
> ONE TWO THREE
> --- --- -----
> 1   2   3
> 10  20  30
> 100 200 300
> rows: 3

SELECT * FROM test WHERE three LIKE '2%';
> ONE TWO THREE
> --- --- -----
> rows: 0

DROP TABLE TEST;
> ok

--- merge (upsert) ---------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

EXPLAIN SELECT * FROM TEST WHERE ID=1;
> PLAN
> ------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE ID = 1
> rows: 1

EXPLAIN MERGE INTO TEST VALUES(1, 'Hello');
> PLAN
> ------------------------------------------------------------
> MERGE INTO PUBLIC.TEST(ID, NAME) KEY(ID) VALUES (1, 'Hello')
> rows: 1

MERGE INTO TEST VALUES(1, 'Hello');
> update count: 1

MERGE INTO TEST VALUES(1, 'Hi');
> update count: 1

MERGE INTO TEST VALUES(2, 'World');
> update count: 1

MERGE INTO TEST VALUES(2, 'World!');
> update count: 1

MERGE INTO TEST(ID, NAME) VALUES(3, 'How are you');
> update count: 1

EXPLAIN MERGE INTO TEST(ID, NAME) VALUES(3, 'How are you');
> PLAN
> ------------------------------------------------------------------
> MERGE INTO PUBLIC.TEST(ID, NAME) KEY(ID) VALUES (3, 'How are you')
> rows: 1

MERGE INTO TEST(ID, NAME) KEY(ID) VALUES(3, 'How do you do');
> update count: 1

EXPLAIN MERGE INTO TEST(ID, NAME) KEY(ID) VALUES(3, 'How do you do');
> PLAN
> --------------------------------------------------------------------
> MERGE INTO PUBLIC.TEST(ID, NAME) KEY(ID) VALUES (3, 'How do you do')
> rows: 1

MERGE INTO TEST(ID, NAME) KEY(NAME) VALUES(3, 'Fine');
> exception

MERGE INTO TEST(ID, NAME) KEY(NAME) VALUES(4, 'Fine!');
> update count: 1

MERGE INTO TEST(ID, NAME) KEY(NAME) VALUES(4, 'Fine! And you');
> exception

MERGE INTO TEST(ID, NAME) KEY(NAME, ID) VALUES(5, 'I''m ok');
> update count: 1

MERGE INTO TEST(ID, NAME) KEY(NAME, ID) VALUES(5, 'Oh, fine');
> exception

MERGE INTO TEST(ID, NAME) VALUES(6, 'Oh, fine.');
> update count: 1

SELECT * FROM TEST;
> ID NAME
> -- -------------
> 1  Hi
> 2  World!
> 3  How do you do
> 4  Fine!
> 5  I'm ok
> 6  Oh, fine.
> rows: 6

MERGE INTO TEST SELECT ID+4, NAME FROM TEST;
> update count: 6

SELECT * FROM TEST;
> ID NAME
> -- -------------
> 1  Hi
> 10 Oh, fine.
> 2  World!
> 3  How do you do
> 4  Fine!
> 5  Hi
> 6  World!
> 7  How do you do
> 8  Fine!
> 9  I'm ok
> rows: 10

DROP TABLE TEST;
> ok

CREATE TABLE PARENT(ID INT, NAME VARCHAR);
> ok

CREATE TABLE CHILD(ID INT, PARENTID INT, FOREIGN KEY(PARENTID) REFERENCES PARENT(ID));
> ok

INSERT INTO PARENT VALUES(1, 'Mary'), (2, 'John');
> update count: 2

INSERT INTO CHILD VALUES(10, 1), (11, 1), (20, 2), (21, 2);
> update count: 4

MERGE INTO PARENT KEY(ID) VALUES(1, 'Marcy');
> update count: 1

SELECT * FROM PARENT;
> ID NAME
> -- -----
> 1  Marcy
> 2  John
> rows: 2

SELECT * FROM CHILD;
> ID PARENTID
> -- --------
> 10 1
> 11 1
> 20 2
> 21 2
> rows: 4

DROP TABLE PARENT;
> ok

DROP TABLE CHILD;
> ok

---
create table STRING_TEST(label varchar(31), label2 varchar(255));
> ok

create table STRING_TEST_ic(label varchar_ignorecase(31), label2
varchar_ignorecase(255));
> ok

insert into STRING_TEST values('HELLO','Bye');
> update count: 1

insert into STRING_TEST values('HELLO','Hello');
> update count: 1

insert into STRING_TEST_ic select * from STRING_TEST;
> update count: 2

-- Expect rows of STRING_TEST_ic and STRING_TEST to be identical
select * from STRING_TEST;
> LABEL LABEL2
> ----- ------
> HELLO Bye
> HELLO Hello
> rows: 2

-- correct
select * from STRING_TEST_ic;
> LABEL LABEL2
> ----- ------
> HELLO Bye
> HELLO Hello
> rows: 2

drop table STRING_TEST;
> ok

drop table STRING_TEST_ic;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR_IGNORECASE);
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'hallo'), (4, 'hoi');
> update count: 4

SELECT * FROM TEST WHERE NAME = 'HELLO';
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT * FROM TEST WHERE NAME = 'HE11O';
> ID NAME
> -- ----
> rows: 0

SELECT * FROM TEST ORDER BY NAME;
> ID NAME
> -- -----
> 3  hallo
> 1  Hello
> 4  hoi
> 2  World
> rows (ordered): 4

DROP TABLE IF EXISTS TEST;
> ok

--- update with list ---------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT * FROM TEST ORDER BY ID;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

UPDATE TEST t0 SET t0.NAME='Hi' WHERE t0.ID=1;
> update count: 1

update test set (id, name)=(id+1, name || 'Hi');
> update count: 2

update test set (id, name)=(select id+1, name || 'Ho' from test t1 where test.id=t1.id);
> update count: 2

explain update test set (id, name)=(id+1, name || 'Hi');
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------
> UPDATE PUBLIC.TEST /* PUBLIC.TEST.tableScan */ SET ID = ARRAY_GET(((ID + 1), (NAME || 'Hi')), 1), NAME = ARRAY_GET(((ID + 1), (NAME || 'Hi')), 2)
> rows: 1

explain update test set (id, name)=(select id+1, name || 'Ho' from test t1 where test.id=t1.id);
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> UPDATE PUBLIC.TEST /* PUBLIC.TEST.tableScan */ SET ID = ARRAY_GET((SELECT (ID + 1), (NAME || 'Ho') FROM PUBLIC.TEST T1 /* PUBLIC.PRIMARY_KEY_2: ID = TEST.ID */ WHERE TEST.ID = T1.ID), 1), NAME = ARRAY_GET((SELECT (ID + 1), (NAME || 'Ho') FROM PUBLIC.TEST T1 /* PUBLIC.PRIMARY_KEY_2: ID = TEST.ID */ WHERE TEST.ID = T1.ID), 2)
> rows: 1

select * from test;
> ID NAME
> -- ---------
> 3  HiHiHo
> 4  WorldHiHo
> rows: 2

DROP TABLE IF EXISTS TEST;
> ok

--- script ---------------------------------------------------------------------------------------------
create memory table test(id int primary key, c clob, b blob);
> ok

insert into test values(0, null, null);
> update count: 1

insert into test values(1, '', '');
> update count: 1

insert into test values(2, 'Cafe', X'cafe');
> update count: 1

script simple nopasswords nosettings;
> SCRIPT
> ---------------------------------------------------------------------------
> -- 3 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, C CLOB, B BLOB );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.TEST(ID, C, B) VALUES(0, NULL, NULL);
> INSERT INTO PUBLIC.TEST(ID, C, B) VALUES(1, '', X'');
> INSERT INTO PUBLIC.TEST(ID, C, B) VALUES(2, 'Cafe', X'cafe');
> rows: 7

drop table test;
> ok

--- optimizer ---------------------------------------------------------------------------------------------
create table b(id int primary key, p int);
> ok

create index bp on b(p);
> ok

insert into b values(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9);
> update count: 10

insert into b select id+10, p+10 from b;
> update count: 10

explain select * from b b0, b b1, b b2 where b1.p = b0.id and b2.p = b1.id and b0.id=10;
> PLAN
> ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT B0.ID, B0.P, B1.ID, B1.P, B2.ID, B2.P FROM PUBLIC.B B0 /* PUBLIC.PRIMARY_KEY_4: ID = 10 */ /* WHERE B0.ID = 10 */ INNER JOIN PUBLIC.B B1 /* PUBLIC.BP: P = B0.ID */ ON 1=1 /* WHERE B1.P = B0.ID */ INNER JOIN PUBLIC.B B2 /* PUBLIC.BP: P = B1.ID */ ON 1=1 WHERE (B0.ID = 10) AND ((B1.P = B0.ID) AND (B2.P = B1.ID))
> rows: 1

explain select * from b b0, b b1, b b2, b b3 where b1.p = b0.id and b2.p = b1.id and b3.p = b2.id and b0.id=10;
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT B0.ID, B0.P, B1.ID, B1.P, B2.ID, B2.P, B3.ID, B3.P FROM PUBLIC.B B0 /* PUBLIC.PRIMARY_KEY_4: ID = 10 */ /* WHERE B0.ID = 10 */ INNER JOIN PUBLIC.B B1 /* PUBLIC.BP: P = B0.ID */ ON 1=1 /* WHERE B1.P = B0.ID */ INNER JOIN PUBLIC.B B2 /* PUBLIC.BP: P = B1.ID */ ON 1=1 /* WHERE B2.P = B1.ID */ INNER JOIN PUBLIC.B B3 /* PUBLIC.BP: P = B2.ID */ ON 1=1 WHERE (B0.ID = 10) AND ((B3.P = B2.ID) AND ((B1.P = B0.ID) AND (B2.P = B1.ID)))
> rows: 1

explain select * from b b0, b b1, b b2, b b3, b b4 where b1.p = b0.id and b2.p = b1.id and b3.p = b2.id and b4.p = b3.id and b0.id=10;
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT B0.ID, B0.P, B1.ID, B1.P, B2.ID, B2.P, B3.ID, B3.P, B4.ID, B4.P FROM PUBLIC.B B0 /* PUBLIC.PRIMARY_KEY_4: ID = 10 */ /* WHERE B0.ID = 10 */ INNER JOIN PUBLIC.B B1 /* PUBLIC.BP: P = B0.ID */ ON 1=1 /* WHERE B1.P = B0.ID */ INNER JOIN PUBLIC.B B2 /* PUBLIC.BP: P = B1.ID */ ON 1=1 /* WHERE B2.P = B1.ID */ INNER JOIN PUBLIC.B B3 /* PUBLIC.BP: P = B2.ID */ ON 1=1 /* WHERE B3.P = B2.ID */ INNER JOIN PUBLIC.B B4 /* PUBLIC.BP: P = B3.ID */ ON 1=1 WHERE (B0.ID = 10) AND ((B4.P = B3.ID) AND ((B3.P = B2.ID) AND ((B1.P = B0.ID) AND (B2.P = B1.ID))))
> rows: 1

analyze;
> ok

explain select * from b b0, b b1, b b2, b b3, b b4 where b1.p = b0.id and b2.p = b1.id and b3.p = b2.id and b4.p = b3.id and b0.id=10;
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT B0.ID, B0.P, B1.ID, B1.P, B2.ID, B2.P, B3.ID, B3.P, B4.ID, B4.P FROM PUBLIC.B B0 /* PUBLIC.PRIMARY_KEY_4: ID = 10 */ /* WHERE B0.ID = 10 */ INNER JOIN PUBLIC.B B1 /* PUBLIC.BP: P = B0.ID */ ON 1=1 /* WHERE B1.P = B0.ID */ INNER JOIN PUBLIC.B B2 /* PUBLIC.BP: P = B1.ID */ ON 1=1 /* WHERE B2.P = B1.ID */ INNER JOIN PUBLIC.B B3 /* PUBLIC.BP: P = B2.ID */ ON 1=1 /* WHERE B3.P = B2.ID */ INNER JOIN PUBLIC.B B4 /* PUBLIC.BP: P = B3.ID */ ON 1=1 WHERE (B0.ID = 10) AND ((B4.P = B3.ID) AND ((B3.P = B2.ID) AND ((B1.P = B0.ID) AND (B2.P = B1.ID))))
> rows: 1

drop table if exists b;
> ok

create table test(id int primary key, first_name varchar, name varchar, state int);
> ok

create index idx_first_name on test(first_name);
> ok

create index idx_name on test(name);
> ok

create index idx_state on test(state);
> ok

insert into test values
(0, 'Anne', 'Smith', 0), (1, 'Tom', 'Smith', 0),
(2, 'Tom', 'Jones', 0), (3, 'Steve', 'Johnson', 0),
(4, 'Steve', 'Martin', 0), (5, 'Jon', 'Jones', 0),
(6, 'Marc', 'Scott', 0), (7, 'Marc', 'Miller', 0),
(8, 'Susan', 'Wood', 0), (9, 'Jon', 'Bennet', 0);
> update count: 10

EXPLAIN SELECT * FROM TEST WHERE ID = 3;
> PLAN
> -----------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.FIRST_NAME, TEST.NAME, TEST.STATE FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = 3 */ WHERE ID = 3
> rows: 1

SELECT SELECTIVITY(ID), SELECTIVITY(FIRST_NAME),
SELECTIVITY(NAME), SELECTIVITY(STATE)
FROM TEST WHERE ROWNUM()<100000;
> SELECTIVITY(ID) SELECTIVITY(FIRST_NAME) SELECTIVITY(NAME) SELECTIVITY(STATE)
> --------------- ----------------------- ----------------- ------------------
> 100             60                      80                10
> rows: 1

explain select * from test where name='Smith' and first_name='Tom' and state=0;
> PLAN
> -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.FIRST_NAME, TEST.NAME, TEST.STATE FROM PUBLIC.TEST /* PUBLIC.IDX_FIRST_NAME: FIRST_NAME = 'Tom' */ WHERE (STATE = 0) AND ((NAME = 'Smith') AND (FIRST_NAME = 'Tom'))
> rows: 1

alter table test alter column name selectivity 100;
> ok

explain select * from test where name='Smith' and first_name='Tom' and state=0;
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.FIRST_NAME, TEST.NAME, TEST.STATE FROM PUBLIC.TEST /* PUBLIC.IDX_NAME: NAME = 'Smith' */ WHERE (STATE = 0) AND ((NAME = 'Smith') AND (FIRST_NAME = 'Tom'))
> rows: 1

drop table test;
> ok

CREATE TABLE O(X INT PRIMARY KEY, Y INT);
> ok

INSERT INTO O SELECT X, X+1 FROM SYSTEM_RANGE(1, 1000);
> update count: 1000

EXPLAIN SELECT A.X FROM O B, O A, O F, O D, O C, O E, O G, O H, O I, O J
WHERE 1=J.X and J.Y=I.X AND I.Y=H.X AND H.Y=G.X AND G.Y=F.X AND F.Y=E.X
AND E.Y=D.X AND D.Y=C.X AND C.Y=B.X AND B.Y=A.X;
> PLAN
> ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT A.X FROM PUBLIC.O J /* PUBLIC.PRIMARY_KEY_4: X = 1 */ /* WHERE J.X = 1 */ INNER JOIN PUBLIC.O I /* PUBLIC.PRIMARY_KEY_4: X = J.Y */ ON 1=1 /* WHERE J.Y = I.X */ INNER JOIN PUBLIC.O H /* PUBLIC.PRIMARY_KEY_4: X = I.Y */ ON 1=1 /* WHERE I.Y = H.X */ INNER JOIN PUBLIC.O G /* PUBLIC.PRIMARY_KEY_4: X = H.Y */ ON 1=1 /* WHERE H.Y = G.X */ INNER JOIN PUBLIC.O F /* PUBLIC.PRIMARY_KEY_4: X = G.Y */ ON 1=1 /* WHERE G.Y = F.X */ INNER JOIN PUBLIC.O E /* PUBLIC.PRIMARY_KEY_4: X = F.Y */ ON 1=1 /* WHERE F.Y = E.X */ INNER JOIN PUBLIC.O D /* PUBLIC.PRIMARY_KEY_4: X = E.Y */ ON 1=1 /* WHERE E.Y = D.X */ INNER JOIN PUBLIC.O C /* PUBLIC.PRIMARY_KEY_4: X = D.Y */ ON 1=1 /* WHERE D.Y = C.X */ INNER JOIN PUBLIC.O B /* PUBLIC.PRIMARY_KEY_4: X = C.Y */ ON 1=1 /* WHERE C.Y = B.X */ INNER JOIN PUBLIC.O A /* PUBLIC.PRIMARY_KEY_4: X = B.Y */ ON 1=1 WHERE (B.Y = A.X) AND ((C.Y = B.X) AND ((D.Y = C.X) AND ((E.Y = D.X) AND ((F.Y = E.X) AND ((G.Y = F.X) AND ((H.Y = G.X) AND ((I.Y = H.X) AND ((J.X = 1) AND (J.Y = I.X)))))))))
> rows: 1

DROP TABLE O;
> ok

CREATE TABLE PARENT(ID INT PRIMARY KEY, AID INT, BID INT, CID INT, DID INT, EID INT, FID INT, GID INT, HID INT);
> ok

CREATE TABLE CHILD(ID INT PRIMARY KEY);
> ok

INSERT INTO PARENT SELECT X, 1, 2, 1, 2, 1, 2, 1, 2 FROM SYSTEM_RANGE(0, 1000);
> update count: 1001

INSERT INTO CHILD SELECT X FROM SYSTEM_RANGE(0, 1000);
> update count: 1001

SELECT COUNT(*) FROM PARENT, CHILD A, CHILD B, CHILD C, CHILD D, CHILD E, CHILD F, CHILD G, CHILD H
WHERE AID=A.ID AND BID=B.ID AND CID=C.ID
AND DID=D.ID AND EID=E.ID AND FID=F.ID AND GID=G.ID AND HID=H.ID;
> COUNT(*)
> --------
> 1001
> rows: 1

EXPLAIN SELECT COUNT(*) FROM PARENT, CHILD A, CHILD B, CHILD C, CHILD D, CHILD E, CHILD F, CHILD G, CHILD H
WHERE AID=A.ID AND BID=B.ID AND CID=C.ID
AND DID=D.ID AND EID=E.ID AND FID=F.ID AND GID=G.ID AND HID=H.ID;
> PLAN
> -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT COUNT(*) FROM PUBLIC.PARENT /* PUBLIC.PARENT.tableScan */ INNER JOIN PUBLIC.CHILD A /* PUBLIC.PRIMARY_KEY_3: ID = AID */ ON 1=1 /* WHERE AID = A.ID */ INNER JOIN PUBLIC.CHILD B /* PUBLIC.PRIMARY_KEY_3: ID = BID */ ON 1=1 /* WHERE BID = B.ID */ INNER JOIN PUBLIC.CHILD C /* PUBLIC.PRIMARY_KEY_3: ID = CID */ ON 1=1 /* WHERE CID = C.ID */ INNER JOIN PUBLIC.CHILD D /* PUBLIC.PRIMARY_KEY_3: ID = DID */ ON 1=1 /* WHERE DID = D.ID */ INNER JOIN PUBLIC.CHILD E /* PUBLIC.PRIMARY_KEY_3: ID = EID */ ON 1=1 /* WHERE EID = E.ID */ INNER JOIN PUBLIC.CHILD F /* PUBLIC.PRIMARY_KEY_3: ID = FID */ ON 1=1 /* WHERE FID = F.ID */ INNER JOIN PUBLIC.CHILD G /* PUBLIC.PRIMARY_KEY_3: ID = GID */ ON 1=1 /* WHERE GID = G.ID */ INNER JOIN PUBLIC.CHILD H /* PUBLIC.PRIMARY_KEY_3: ID = HID */ ON 1=1 WHERE (HID = H.ID) AND ((GID = G.ID) AND ((FID = F.ID) AND ((EID = E.ID) AND ((DID = D.ID) AND ((CID = C.ID) AND ((AID = A.ID) AND (BID = B.ID)))))))
> rows: 1

CREATE TABLE FAMILY(ID INT PRIMARY KEY, PARENTID INT);
> ok

INSERT INTO FAMILY SELECT X, X-1 FROM SYSTEM_RANGE(0, 1000);
> update count: 1001

EXPLAIN SELECT COUNT(*) FROM CHILD A, CHILD B, FAMILY, CHILD C, CHILD D, PARENT, CHILD E, CHILD F, CHILD G
WHERE FAMILY.ID=1 AND FAMILY.PARENTID=PARENT.ID
AND AID=A.ID AND BID=B.ID AND CID=C.ID AND DID=D.ID AND EID=E.ID AND FID=F.ID AND GID=G.ID;
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT COUNT(*) FROM PUBLIC.FAMILY /* PUBLIC.PRIMARY_KEY_7: ID = 1 */ /* WHERE FAMILY.ID = 1 */ INNER JOIN PUBLIC.PARENT /* PUBLIC.PRIMARY_KEY_8: ID = FAMILY.PARENTID */ ON 1=1 /* WHERE FAMILY.PARENTID = PARENT.ID */ INNER JOIN PUBLIC.CHILD A /* PUBLIC.PRIMARY_KEY_3: ID = AID */ ON 1=1 /* WHERE AID = A.ID */ INNER JOIN PUBLIC.CHILD B /* PUBLIC.PRIMARY_KEY_3: ID = BID */ ON 1=1 /* WHERE BID = B.ID */ INNER JOIN PUBLIC.CHILD C /* PUBLIC.PRIMARY_KEY_3: ID = CID */ ON 1=1 /* WHERE CID = C.ID */ INNER JOIN PUBLIC.CHILD D /* PUBLIC.PRIMARY_KEY_3: ID = DID */ ON 1=1 /* WHERE DID = D.ID */ INNER JOIN PUBLIC.CHILD E /* PUBLIC.PRIMARY_KEY_3: ID = EID */ ON 1=1 /* WHERE EID = E.ID */ INNER JOIN PUBLIC.CHILD F /* PUBLIC.PRIMARY_KEY_3: ID = FID */ ON 1=1 /* WHERE FID = F.ID */ INNER JOIN PUBLIC.CHILD G /* PUBLIC.PRIMARY_KEY_3: ID = GID */ ON 1=1 WHERE (GID = G.ID) AND ((FID = F.ID) AND ((EID = E.ID) AND ((DID = D.ID) AND ((CID = C.ID) AND ((BID = B.ID) AND ((AID = A.ID) AND ((FAMILY.ID = 1) AND (FAMILY.PARENTID = PARENT.ID))))))))
> rows: 1

DROP TABLE FAMILY;
> ok

DROP TABLE PARENT;
> ok

DROP TABLE CHILD;
> ok

--- is null / not is null ---------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT UNIQUE, NAME VARCHAR CHECK LENGTH(NAME)>3);
> ok

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, NAME VARCHAR(255), B INT);
> ok

CREATE UNIQUE INDEX IDXNAME ON TEST(NAME);
> ok

CREATE UNIQUE INDEX IDX_NAME_B ON TEST(NAME, B);
> ok

INSERT INTO TEST(ID, NAME, B) VALUES (0, NULL, NULL);
> update count: 1

INSERT INTO TEST(ID, NAME, B) VALUES (1, 'Hello', NULL);
> update count: 1

INSERT INTO TEST(ID, NAME, B) VALUES (2, NULL, NULL);
> update count: 1

INSERT INTO TEST(ID, NAME, B) VALUES (3, 'World', NULL);
> update count: 1

select * from test;
> ID NAME  B
> -- ----- ----
> 0  null  null
> 1  Hello null
> 2  null  null
> 3  World null
> rows: 4

UPDATE test SET name='Hi';
> exception

select * from test;
> ID NAME  B
> -- ----- ----
> 0  null  null
> 1  Hello null
> 2  null  null
> 3  World null
> rows: 4

UPDATE test SET name=NULL;
> update count: 4

UPDATE test SET B=1;
> update count: 4

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES(NULL, NULL), (0, 'Hello'), (1, 'World');
> update count: 3

SELECT * FROM TEST WHERE NOT (1=1);
> ID NAME
> -- ----
> rows: 0

DROP TABLE TEST;
> ok

create table test_null(a int, b int);
> ok

insert into test_null values(0, 0);
> update count: 1

insert into test_null values(0, null);
> update count: 1

insert into test_null values(null, null);
> update count: 1

insert into test_null values(null, 0);
> update count: 1

select * from test_null where a=0;
> A B
> - ----
> 0 0
> 0 null
> rows: 2

select * from test_null where not a=0;
> A B
> - -
> rows: 0

select * from test_null where (a=0 or b=0);
> A    B
> ---- ----
> 0    0
> 0    null
> null 0
> rows: 3

select * from test_null where not (a=0 or b=0);
> A B
> - -
> rows: 0

select * from test_null where (a=1 or b=0);
> A    B
> ---- -
> 0    0
> null 0
> rows: 2

select * from test_null where not( a=1 or b=0);
> A B
> - -
> rows: 0

select * from test_null where not(not( a=1 or b=0));
> A    B
> ---- -
> 0    0
> null 0
> rows: 2

select * from test_null where a=0 or b=0;
> A    B
> ---- ----
> 0    0
> 0    null
> null 0
> rows: 3

SELECT count(*) FROM test_null WHERE not ('X'=null and 1=0);
> COUNT(*)
> --------
> 4
> rows: 1

drop table if exists test_null;
> ok

--- function alias ---------------------------------------------------------------------------------------------
CREATE ALIAS MY_SQRT FOR "java.lang.Math.sqrt";
> ok

SELECT MY_SQRT(2.0) MS, SQRT(2.0);
> MS                 1.4142135623730951
> ------------------ ------------------
> 1.4142135623730951 1.4142135623730951
> rows: 1

SELECT MY_SQRT(SUM(X)), SUM(X), MY_SQRT(55) FROM SYSTEM_RANGE(1, 10);
> PUBLIC.MY_SQRT(SUM(X)) SUM(X) PUBLIC.MY_SQRT(55)
> ---------------------- ------ ------------------
> 7.416198487095663      55     7.416198487095663
> rows: 1

SELECT MY_SQRT(-1.0) MS, SQRT(NULL) S;
> MS  S
> --- ----
> NaN null
> rows: 1

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ------------------------------------------------------------
> CREATE FORCE ALIAS PUBLIC.MY_SQRT FOR "java.lang.Math.sqrt";
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 2

SELECT ALIAS_NAME, JAVA_CLASS, JAVA_METHOD, DATA_TYPE, COLUMN_COUNT, RETURNS_RESULT, REMARKS FROM INFORMATION_SCHEMA.FUNCTION_ALIASES;
> ALIAS_NAME JAVA_CLASS     JAVA_METHOD DATA_TYPE COLUMN_COUNT RETURNS_RESULT REMARKS
> ---------- -------------- ----------- --------- ------------ -------------- -------
> MY_SQRT    java.lang.Math sqrt        8         1            2
> rows: 1

DROP ALIAS MY_SQRT;
> ok

--- schema ----------------------------------------------------------------------------------------------
SELECT DISTINCT TABLE_SCHEMA, TABLE_CATALOG FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA;
> TABLE_SCHEMA       TABLE_CATALOG
> ------------------ -------------
> INFORMATION_SCHEMA SCRIPT
> rows (ordered): 1

SELECT * FROM INFORMATION_SCHEMA.SCHEMATA;
> CATALOG_NAME SCHEMA_NAME        SCHEMA_OWNER DEFAULT_CHARACTER_SET_NAME DEFAULT_COLLATION_NAME IS_DEFAULT REMARKS ID
> ------------ ------------------ ------------ -------------------------- ---------------------- ---------- ------- --
> SCRIPT       INFORMATION_SCHEMA SA           Unicode                    OFF                    FALSE              -1
> SCRIPT       PUBLIC             SA           Unicode                    OFF                    TRUE               0
> rows: 2

SELECT * FROM INFORMATION_SCHEMA.CATALOGS;
> CATALOG_NAME
> ------------
> SCRIPT
> rows: 1

SELECT INFORMATION_SCHEMA.SCHEMATA.SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA;
> SCHEMA_NAME
> ------------------
> INFORMATION_SCHEMA
> PUBLIC
> rows: 2

SELECT INFORMATION_SCHEMA.SCHEMATA.* FROM INFORMATION_SCHEMA.SCHEMATA;
> CATALOG_NAME SCHEMA_NAME        SCHEMA_OWNER DEFAULT_CHARACTER_SET_NAME DEFAULT_COLLATION_NAME IS_DEFAULT REMARKS ID
> ------------ ------------------ ------------ -------------------------- ---------------------- ---------- ------- --
> SCRIPT       INFORMATION_SCHEMA SA           Unicode                    OFF                    FALSE              -1
> SCRIPT       PUBLIC             SA           Unicode                    OFF                    TRUE               0
> rows: 2

CREATE SCHEMA TEST_SCHEMA AUTHORIZATION SA;
> ok

DROP SCHEMA TEST_SCHEMA RESTRICT;
> ok

create schema Contact_Schema AUTHORIZATION SA;
> ok

CREATE TABLE Contact_Schema.Address (
address_id           BIGINT NOT NULL
CONSTRAINT address_id_check
CHECK (address_id > 0),
address_type         VARCHAR(20) NOT NULL
CONSTRAINT address_type
CHECK (address_type in ('postal','email','web')),
CONSTRAINT X_PKAddress
PRIMARY KEY (address_id)
);
> ok

create schema ClientServer_Schema AUTHORIZATION SA;
> ok

CREATE TABLE ClientServer_Schema.PrimaryKey_Seq (
sequence_name VARCHAR(100) NOT NULL,
seq_number BIGINT NOT NULL,
CONSTRAINT X_PKPrimaryKey_Seq
PRIMARY KEY (sequence_name)
);
> ok

alter table Contact_Schema.Address add constraint abc foreign key(address_id)
references ClientServer_Schema.PrimaryKey_Seq(seq_number);
> ok

drop table ClientServer_Schema.PrimaryKey_Seq;
> ok

drop table Contact_Schema.Address;
> ok

drop schema Contact_Schema restrict;
> ok

drop schema ClientServer_Schema restrict;
> ok

--- alter table add / drop / rename column ----------------------------------------------------------------------------------------------
CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY);
> ok

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ---------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 4

ALTER TABLE TEST ADD CREATEDATE VARCHAR(255) DEFAULT '2001-01-01' NOT NULL;
> ok

ALTER TABLE TEST ADD NAME VARCHAR(255) NULL BEFORE CREATEDATE;
> ok

CREATE INDEX IDXNAME ON TEST(NAME);
> ok

INSERT INTO TEST(ID, NAME) VALUES(1, 'Hi');
> update count: 1

ALTER TABLE TEST ALTER COLUMN NAME SET NOT NULL;
> ok

ALTER TABLE TEST ALTER COLUMN NAME SET NOT NULL;
> ok

ALTER TABLE TEST ALTER COLUMN NAME SET NULL;
> ok

ALTER TABLE TEST ALTER COLUMN NAME SET NULL;
> ok

ALTER TABLE TEST ALTER COLUMN NAME SET DEFAULT 1;
> ok

SELECT * FROM TEST;
> ID NAME CREATEDATE
> -- ---- ----------
> 1  Hi   2001-01-01
> rows: 1

ALTER TABLE TEST ADD MODIFY_DATE TIMESTAMP;
> ok

CREATE MEMORY TABLE TEST_SEQ(ID INT, NAME VARCHAR);
> ok

INSERT INTO TEST_SEQ VALUES(-1, '-1');
> update count: 1

ALTER TABLE TEST_SEQ ALTER COLUMN ID IDENTITY;
> ok

INSERT INTO TEST_SEQ VALUES(NULL, '1');
> update count: 1

ALTER TABLE TEST_SEQ ALTER COLUMN ID RESTART WITH 10;
> ok

INSERT INTO TEST_SEQ VALUES(NULL, '10');
> update count: 1

alter table test_seq drop primary key;
> ok

ALTER TABLE TEST_SEQ ALTER COLUMN ID INT DEFAULT 20;
> ok

INSERT INTO TEST_SEQ VALUES(DEFAULT, '20');
> update count: 1

ALTER TABLE TEST_SEQ ALTER COLUMN NAME RENAME TO DATA;
> ok

SELECT * FROM TEST_SEQ ORDER BY ID;
> ID DATA
> -- ----
> -1 -1
> 1  1
> 10 10
> 20 20
> rows (ordered): 4

SCRIPT SIMPLE NOPASSWORDS NOSETTINGS;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> -- 4 +/- SELECT COUNT(*) FROM PUBLIC.TEST_SEQ;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CREATE INDEX PUBLIC.IDXNAME ON PUBLIC.TEST(NAME);
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, NAME VARCHAR(255) DEFAULT 1, CREATEDATE VARCHAR(255) DEFAULT '2001-01-01' NOT NULL, MODIFY_DATE TIMESTAMP );
> CREATE MEMORY TABLE PUBLIC.TEST_SEQ( ID INT DEFAULT 20 NOT NULL, DATA VARCHAR );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.TEST(ID, NAME, CREATEDATE, MODIFY_DATE) VALUES(1, 'Hi', '2001-01-01', NULL);
> INSERT INTO PUBLIC.TEST_SEQ(ID, DATA) VALUES(-1, '-1');
> INSERT INTO PUBLIC.TEST_SEQ(ID, DATA) VALUES(1, '1');
> INSERT INTO PUBLIC.TEST_SEQ(ID, DATA) VALUES(10, '10');
> INSERT INTO PUBLIC.TEST_SEQ(ID, DATA) VALUES(20, '20');
> rows: 12

CREATE UNIQUE INDEX IDX_NAME_ID ON TEST(ID, NAME);
> ok

ALTER TABLE TEST DROP COLUMN NAME;
> exception

DROP INDEX IDX_NAME_ID;
> ok

DROP INDEX IDX_NAME_ID IF EXISTS;
> ok

ALTER TABLE TEST DROP NAME;
> ok

DROP TABLE TEST_SEQ;
> ok

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ---------------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, CREATEDATE VARCHAR(255) DEFAULT '2001-01-01' NOT NULL, MODIFY_DATE TIMESTAMP );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.TEST(ID, CREATEDATE, MODIFY_DATE) VALUES (1, '2001-01-01', NULL);
> rows: 5

ALTER TABLE TEST ADD NAME VARCHAR(255) NULL BEFORE CREATEDATE;
> ok

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ----------------------------------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, NAME VARCHAR(255), CREATEDATE VARCHAR(255) DEFAULT '2001-01-01' NOT NULL, MODIFY_DATE TIMESTAMP );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.TEST(ID, NAME, CREATEDATE, MODIFY_DATE) VALUES (1, NULL, '2001-01-01', NULL);
> rows: 5

UPDATE TEST SET NAME = 'Hi';
> update count: 1

INSERT INTO TEST VALUES(2, 'Hello', DEFAULT, DEFAULT);
> update count: 1

SELECT * FROM TEST;
> ID NAME  CREATEDATE MODIFY_DATE
> -- ----- ---------- -----------
> 1  Hi    2001-01-01 null
> 2  Hello 2001-01-01 null
> rows: 2

DROP TABLE TEST;
> ok

create table test(id int, name varchar invisible);
> ok

select * from test;
> ID
> --
> rows: 0

alter table test alter column name set visible;
> ok

select * from test;
> ID NAME
> -- ----
> rows: 0

alter table test add modify_date timestamp invisible before name;
> ok

select * from test;
> ID NAME
> -- ----
> rows: 0

alter table test alter column modify_date timestamp visible;
> ok

select * from test;
> ID MODIFY_DATE NAME
> -- ----------- ----
> rows: 0

alter table test alter column modify_date set invisible;
> ok

select * from test;
> ID NAME
> -- ----
> rows: 0

drop table test;
> ok

--- autoIncrement ----------------------------------------------------------------------------------------------
CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ---------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, NAME VARCHAR );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 4

INSERT INTO TEST(ID, NAME) VALUES(1, 'Hi'), (2, 'World');
> update count: 2

SELECT * FROM TEST;
> ID NAME
> -- -----
> 1  Hi
> 2  World
> rows: 2

SELECT * FROM TEST WHERE ? IS NULL;
{
Hello
> ID NAME
> -- ----
> rows: 0
};
> update count: 0

DROP TABLE TEST;
> ok

--- limit/offset ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'with'), (4, 'limited'), (5, 'resources');
> update count: 5

SELECT TOP 2 * FROM TEST ORDER BY ID;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

SELECT LIMIT (0+0) (2+0) * FROM TEST ORDER BY ID;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

SELECT LIMIT (1+0) (2+0) NAME, -ID, ID _ID_ FROM TEST ORDER BY _ID_;
> NAME  - ID _ID_
> ----- ---- ----
> World -2   2
> with  -3   3
> rows (ordered): 2

EXPLAIN SELECT LIMIT (1+0) (2+0) * FROM TEST ORDER BY ID;
> PLAN
> --------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2 */ ORDER BY 1 LIMIT 2 OFFSET 1 /* index sorted */
> rows (ordered): 1

SELECT * FROM TEST ORDER BY ID LIMIT 2+0 OFFSET 1+0;
> ID NAME
> -- -----
> 2  World
> 3  with
> rows (ordered): 2

SELECT * FROM TEST UNION ALL SELECT * FROM TEST ORDER BY ID LIMIT 2+0 OFFSET 1+0;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

SELECT * FROM TEST ORDER BY ID OFFSET 4;
> ID NAME
> -- ---------
> 5  resources
> rows (ordered): 1

SELECT ID FROM TEST GROUP BY ID UNION ALL SELECT ID FROM TEST GROUP BY ID;
> ID
> --
> 1
> 1
> 2
> 2
> 3
> 3
> 4
> 4
> 5
> 5
> rows: 10

SELECT * FROM (SELECT ID FROM TEST GROUP BY ID);
> ID
> --
> 1
> 2
> 3
> 4
> 5
> rows: 5

EXPLAIN SELECT * FROM TEST UNION ALL SELECT * FROM TEST ORDER BY ID LIMIT 2+0 OFFSET 1+0;
> PLAN
> ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> (SELECT TEST.ID, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */) UNION ALL (SELECT TEST.ID, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */) ORDER BY 1 LIMIT 2 OFFSET 1
> rows (ordered): 1

EXPLAIN DELETE FROM TEST WHERE ID=1;
> PLAN
> -----------------------------------------------------------------------
> DELETE FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE ID = 1
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST2COL(A INT, B INT, C VARCHAR(255), PRIMARY KEY(A, B));
> ok

INSERT INTO TEST2COL VALUES(0, 0, 'Hallo'), (0, 1, 'Welt'), (1, 0, 'Hello'), (1, 1, 'World');
> update count: 4

SELECT * FROM TEST2COL WHERE A=0 AND B=0;
> A B C
> - - -----
> 0 0 Hallo
> rows: 1

EXPLAIN SELECT * FROM TEST2COL WHERE A=0 AND B=0;
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST2COL.A, TEST2COL.B, TEST2COL.C FROM PUBLIC.TEST2COL /* PUBLIC.PRIMARY_KEY_E: A = 0 AND B = 0 */ WHERE ((A = 0) AND (B = 0)) AND (A = B)
> rows: 1

SELECT * FROM TEST2COL WHERE A=0;
> A B C
> - - -----
> 0 0 Hallo
> 0 1 Welt
> rows: 2

EXPLAIN SELECT * FROM TEST2COL WHERE A=0;
> PLAN
> ------------------------------------------------------------------------------------------------------------
> SELECT TEST2COL.A, TEST2COL.B, TEST2COL.C FROM PUBLIC.TEST2COL /* PUBLIC.PRIMARY_KEY_E: A = 0 */ WHERE A = 0
> rows: 1

SELECT * FROM TEST2COL WHERE B=0;
> A B C
> - - -----
> 0 0 Hallo
> 1 0 Hello
> rows: 2

EXPLAIN SELECT * FROM TEST2COL WHERE B=0;
> PLAN
> ----------------------------------------------------------------------------------------------------------
> SELECT TEST2COL.A, TEST2COL.B, TEST2COL.C FROM PUBLIC.TEST2COL /* PUBLIC.TEST2COL.tableScan */ WHERE B = 0
> rows: 1

DROP TABLE TEST2COL;
> ok

--- testCases ----------------------------------------------------------------------------------------------
CREATE TABLE t_1 (ch CHARACTER(10), dec DECIMAL(10,2), do DOUBLE, lo BIGINT, "IN" INTEGER, sm SMALLINT, ty TINYINT,
da DATE DEFAULT CURRENT_DATE, ti TIME DEFAULT CURRENT_TIME, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP );
> ok

INSERT INTO T_1 (ch, dec, do) VALUES ('name', 10.23, 0);
> update count: 1

SELECT COUNT(*) FROM T_1;
> COUNT(*)
> --------
> 1
> rows: 1

DROP TABLE T_1;
> ok

--- rights ----------------------------------------------------------------------------------------------
CREATE USER TEST_USER PASSWORD '123';
> ok

CREATE TABLE TEST(ID INT);
> ok

CREATE ROLE TEST_ROLE;
> ok

CREATE ROLE IF NOT EXISTS TEST_ROLE;
> ok

GRANT SELECT, INSERT ON TEST TO TEST_USER;
> ok

GRANT UPDATE ON TEST TO TEST_ROLE;
> ok

GRANT TEST_ROLE TO TEST_USER;
> ok

SELECT NAME FROM INFORMATION_SCHEMA.ROLES;
> NAME
> ---------
> PUBLIC
> TEST_ROLE
> rows: 2

SELECT GRANTEE, GRANTEETYPE, GRANTEDROLE, RIGHTS, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.RIGHTS;
> GRANTEE   GRANTEETYPE GRANTEDROLE RIGHTS         TABLE_SCHEMA TABLE_NAME
> --------- ----------- ----------- -------------- ------------ ----------
> TEST_ROLE ROLE                    UPDATE         PUBLIC       TEST
> TEST_USER USER                    SELECT, INSERT PUBLIC       TEST
> TEST_USER USER        TEST_ROLE
> rows: 3

SELECT * FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES;
> GRANTOR GRANTEE   TABLE_CATALOG TABLE_SCHEMA TABLE_NAME PRIVILEGE_TYPE IS_GRANTABLE
> ------- --------- ------------- ------------ ---------- -------------- ------------
> null    TEST_ROLE SCRIPT        PUBLIC       TEST       UPDATE         NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       INSERT         NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       SELECT         NO
> rows: 3

SELECT * FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES;
> GRANTOR GRANTEE   TABLE_CATALOG TABLE_SCHEMA TABLE_NAME COLUMN_NAME PRIVILEGE_TYPE IS_GRANTABLE
> ------- --------- ------------- ------------ ---------- ----------- -------------- ------------
> null    TEST_ROLE SCRIPT        PUBLIC       TEST       ID          UPDATE         NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       ID          INSERT         NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       ID          SELECT         NO
> rows: 3

REVOKE INSERT ON TEST FROM TEST_USER;
> ok

REVOKE TEST_ROLE FROM TEST_USER;
> ok

SELECT GRANTEE, GRANTEETYPE, GRANTEDROLE, RIGHTS, TABLE_NAME FROM INFORMATION_SCHEMA.RIGHTS;
> GRANTEE   GRANTEETYPE GRANTEDROLE RIGHTS TABLE_NAME
> --------- ----------- ----------- ------ ----------
> TEST_ROLE ROLE                    UPDATE TEST
> TEST_USER USER                    SELECT TEST
> rows: 2

SELECT * FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES;
> GRANTOR GRANTEE   TABLE_CATALOG TABLE_SCHEMA TABLE_NAME PRIVILEGE_TYPE IS_GRANTABLE
> ------- --------- ------------- ------------ ---------- -------------- ------------
> null    TEST_ROLE SCRIPT        PUBLIC       TEST       UPDATE         NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       SELECT         NO
> rows: 2

DROP USER TEST_USER;
> ok

DROP TABLE TEST;
> ok

DROP ROLE TEST_ROLE;
> ok

SELECT * FROM INFORMATION_SCHEMA.ROLES;
> NAME   REMARKS ID
> ------ ------- --
> PUBLIC         0
> rows: 1

SELECT * FROM INFORMATION_SCHEMA.RIGHTS;
> GRANTEE GRANTEETYPE GRANTEDROLE RIGHTS TABLE_SCHEMA TABLE_NAME ID
> ------- ----------- ----------- ------ ------------ ---------- --
> rows: 0

--- plan ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(?, ?);
{
1, Hello
2, World
3, Peace
};
> update count: 3

EXPLAIN INSERT INTO TEST VALUES(1, 'Test');
> PLAN
> ----------------------------------------------------
> INSERT INTO PUBLIC.TEST(ID, NAME) VALUES (1, 'Test')
> rows: 1

EXPLAIN INSERT INTO TEST VALUES(1, 'Test'), (2, 'World');
> PLAN
> ------------------------------------------------------------------
> INSERT INTO PUBLIC.TEST(ID, NAME) VALUES (1, 'Test'), (2, 'World')
> rows: 1

EXPLAIN INSERT INTO TEST SELECT DISTINCT ID+1, NAME FROM TEST;
> PLAN
> -------------------------------------------------------------------------------------------------------------
> INSERT INTO PUBLIC.TEST(ID, NAME) SELECT DISTINCT (ID + 1), NAME FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */
> rows: 1

EXPLAIN SELECT DISTINCT ID + 1, NAME FROM TEST;
> PLAN
> ---------------------------------------------------------------------------
> SELECT DISTINCT (ID + 1), NAME FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */
> rows: 1

EXPLAIN SELECT * FROM TEST WHERE 1=0;
> PLAN
> -----------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan: FALSE */ WHERE FALSE
> rows: 1

EXPLAIN SELECT TOP 1 * FROM TEST FOR UPDATE;
> PLAN
> -----------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ LIMIT 1 FOR UPDATE
> rows: 1

EXPLAIN SELECT COUNT(NAME) FROM TEST WHERE ID=1;
> PLAN
> -----------------------------------------------------------------------------------
> SELECT COUNT(NAME) FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE ID = 1
> rows: 1

EXPLAIN SELECT * FROM TEST WHERE (ID>=1 AND ID<=2)  OR (ID>0 AND ID<3) AND (ID<>6) ORDER BY NAME NULLS FIRST, 1 NULLS LAST, (1+1) DESC;
> PLAN
> -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ WHERE ((ID >= 1) AND (ID <= 2)) OR ((ID <> 6) AND ((ID > 0) AND (ID < 3))) ORDER BY 2 NULLS FIRST, 1 NULLS LAST, =2 DESC
> rows (ordered): 1

EXPLAIN SELECT * FROM TEST WHERE ID=1 GROUP BY NAME, ID;
> PLAN
> ------------------------------------------------------------------------------------------------------------
> SELECT TEST.ID, TEST.NAME FROM PUBLIC.TEST /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE ID = 1 GROUP BY NAME, ID
> rows: 1

EXPLAIN PLAN FOR UPDATE TEST SET NAME='Hello', ID=1 WHERE NAME LIKE 'T%' ESCAPE 'x';
> PLAN
> ---------------------------------------------------------------------------------------------------------
> UPDATE PUBLIC.TEST /* PUBLIC.TEST.tableScan */ SET NAME = 'Hello', ID = 1 WHERE NAME LIKE 'T%' ESCAPE 'x'
> rows: 1

EXPLAIN PLAN FOR DELETE FROM TEST;
> PLAN
> ---------------------------------------------------
> DELETE FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */
> rows: 1

EXPLAIN PLAN FOR SELECT NAME, COUNT(*) FROM TEST GROUP BY NAME HAVING COUNT(*) > 1;
> PLAN
> ----------------------------------------------------------------------------------------------------
> SELECT NAME, COUNT(*) FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ GROUP BY NAME HAVING COUNT(*) > 1
> rows: 1

EXPLAIN PLAN FOR SELECT * FROM test t1 inner join test t2 on t1.id=t2.id and t2.name is not null where t1.id=1;
> PLAN
> ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME, T2.ID, T2.NAME FROM PUBLIC.TEST T1 /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ /* WHERE T1.ID = 1 */ INNER JOIN PUBLIC.TEST T2 /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID AND ID = T1.ID */ ON 1=1 WHERE (T1.ID = 1) AND ((T2.NAME IS NOT NULL) AND (T1.ID = T2.ID))
> rows: 1

EXPLAIN PLAN FOR SELECT * FROM test t1 left outer join test t2 on t1.id=t2.id and t2.name is not null where t1.id=1;
> PLAN
> -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME, T2.ID, T2.NAME FROM PUBLIC.TEST T1 /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ /* WHERE T1.ID = 1 */ LEFT OUTER JOIN PUBLIC.TEST T2 /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID */ ON (T2.NAME IS NOT NULL) AND (T1.ID = T2.ID) WHERE T1.ID = 1
> rows: 1

EXPLAIN PLAN FOR SELECT * FROM test t1 left outer join test t2 on t1.id=t2.id and t2.name is null where t1.id=1;
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME, T2.ID, T2.NAME FROM PUBLIC.TEST T1 /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ /* WHERE T1.ID = 1 */ LEFT OUTER JOIN PUBLIC.TEST T2 /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID */ ON (T2.NAME IS NULL) AND (T1.ID = T2.ID) WHERE T1.ID = 1
> rows: 1

EXPLAIN PLAN FOR SELECT * FROM TEST T1 WHERE EXISTS(SELECT * FROM TEST T2 WHERE T1.ID-1 = T2.ID);
> PLAN
> -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.TEST.tableScan */ WHERE EXISTS( SELECT T2.ID, T2.NAME FROM PUBLIC.TEST T2 /* PUBLIC.PRIMARY_KEY_2: ID = (T1.ID - 1) */ WHERE (T1.ID - 1) = T2.ID)
> rows: 1

EXPLAIN PLAN FOR SELECT * FROM TEST T1 WHERE ID IN(1, 2);
> PLAN
> ---------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.PRIMARY_KEY_2: ID IN(1, 2) */ WHERE ID IN(1, 2)
> rows: 1

EXPLAIN PLAN FOR SELECT * FROM TEST T1 WHERE ID IN(SELECT ID FROM TEST);
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.PRIMARY_KEY_2: ID IN(SELECT ID FROM PUBLIC.TEST /++ PUBLIC.TEST.tableScan ++/) */ WHERE ID IN( SELECT ID FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */)
> rows: 1

EXPLAIN PLAN FOR SELECT * FROM TEST T1 WHERE ID NOT IN(SELECT ID FROM TEST);
> PLAN
> ------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT T1.ID, T1.NAME FROM PUBLIC.TEST T1 /* PUBLIC.TEST.tableScan */ WHERE NOT (ID IN( SELECT ID FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */))
> rows: 1

EXPLAIN PLAN FOR SELECT CAST(ID AS VARCHAR(255)) FROM TEST;
> PLAN
> ----------------------------------------------------------------------------
> SELECT CAST(ID AS VARCHAR(255)) FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */
> rows: 1

EXPLAIN PLAN FOR SELECT LEFT(NAME, 2) FROM TEST;
> PLAN
> -----------------------------------------------------------------
> SELECT LEFT(NAME, 2) FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */
> rows: 1

EXPLAIN PLAN FOR SELECT * FROM SYSTEM_RANGE(1, 20);
> PLAN
> -----------------------------------------------------------------------
> SELECT SYSTEM_RANGE.X FROM SYSTEM_RANGE(1, 20) /* PUBLIC.RANGE_INDEX */
> rows: 1

SELECT * FROM test t1 inner join test t2 on t1.id=t2.id and t2.name is not null where t1.id=1;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  Hello
> rows: 1

SELECT * FROM test t1 left outer join test t2 on t1.id=t2.id and t2.name is not null where t1.id=1;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  Hello
> rows: 1

SELECT * FROM test t1 left outer join test t2 on t1.id=t2.id and t2.name is null where t1.id=1;
> ID NAME  ID   NAME
> -- ----- ---- ----
> 1  Hello null null
> rows: 1

DROP TABLE TEST;
> ok

--- union ----------------------------------------------------------------------------------------------
SELECT * FROM SYSTEM_RANGE(1,2) UNION ALL SELECT * FROM SYSTEM_RANGE(1,2) ORDER BY 1;
> X
> -
> 1
> 1
> 2
> 2
> rows (ordered): 4

EXPLAIN (SELECT * FROM SYSTEM_RANGE(1,2) UNION ALL SELECT * FROM SYSTEM_RANGE(1,2) ORDER BY 1);
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
> (SELECT SYSTEM_RANGE.X FROM SYSTEM_RANGE(1, 2) /* PUBLIC.RANGE_INDEX */) UNION ALL (SELECT SYSTEM_RANGE.X FROM SYSTEM_RANGE(1, 2) /* PUBLIC.RANGE_INDEX */) ORDER BY 1
> rows (ordered): 1

CREATE TABLE CHILDREN(ID INT PRIMARY KEY, NAME VARCHAR(255), CLASS INT);
> ok

CREATE TABLE CLASSES(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO CHILDREN VALUES(?, ?, ?);
{
0, Joe, 0
1, Anne, 1
2, Joerg, 1
3, Petra, 2
};
> update count: 4

INSERT INTO CLASSES VALUES(?, ?);
{
0, Kindergarden
1, Class 1
2, Class 2
3, Class 3
4, Class 4
};
> update count: 5

SELECT * FROM CHILDREN UNION ALL SELECT * FROM CHILDREN ORDER BY ID, NAME FOR UPDATE;
> ID NAME  CLASS
> -- ----- -----
> 0  Joe   0
> 0  Joe   0
> 1  Anne  1
> 1  Anne  1
> 2  Joerg 1
> 2  Joerg 1
> 3  Petra 2
> 3  Petra 2
> rows (ordered): 8

EXPLAIN SELECT * FROM CHILDREN UNION ALL SELECT * FROM CHILDREN ORDER BY ID, NAME FOR UPDATE;
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> (SELECT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.CHILDREN.tableScan */ FOR UPDATE) UNION ALL (SELECT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.CHILDREN.tableScan */ FOR UPDATE) ORDER BY 1, 2 FOR UPDATE
> rows (ordered): 1

SELECT 'Child', ID, NAME FROM CHILDREN UNION SELECT 'Class', ID, NAME FROM CLASSES;
> 'Child' ID NAME
> ------- -- ------------
> Child   0  Joe
> Child   1  Anne
> Child   2  Joerg
> Child   3  Petra
> Class   0  Kindergarden
> Class   1  Class1
> Class   2  Class2
> Class   3  Class3
> Class   4  Class4
> rows: 9

EXPLAIN SELECT 'Child', ID, NAME FROM CHILDREN UNION SELECT 'Class', ID, NAME FROM CLASSES;
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------
> (SELECT 'Child', ID, NAME FROM PUBLIC.CHILDREN /* PUBLIC.CHILDREN.tableScan */) UNION (SELECT 'Class', ID, NAME FROM PUBLIC.CLASSES /* PUBLIC.CLASSES.tableScan */)
> rows: 1

SELECT * FROM CHILDREN EXCEPT SELECT * FROM CHILDREN WHERE CLASS=0;
> ID NAME  CLASS
> -- ----- -----
> 1  Anne  1
> 2  Joerg 1
> 3  Petra 2
> rows: 3

EXPLAIN SELECT * FROM CHILDREN EXCEPT SELECT * FROM CHILDREN WHERE CLASS=0;
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> (SELECT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.CHILDREN.tableScan */) EXCEPT (SELECT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.CHILDREN.tableScan */ WHERE CLASS = 0)
> rows: 1

EXPLAIN SELECT CLASS FROM CHILDREN INTERSECT SELECT ID FROM CLASSES;
> PLAN
> --------------------------------------------------------------------------------------------------------------------------------------------
> (SELECT CLASS FROM PUBLIC.CHILDREN /* PUBLIC.CHILDREN.tableScan */) INTERSECT (SELECT ID FROM PUBLIC.CLASSES /* PUBLIC.CLASSES.tableScan */)
> rows: 1

SELECT CLASS FROM CHILDREN INTERSECT SELECT ID FROM CLASSES;
> CLASS
> -----
> 0
> 1
> 2
> rows: 3

EXPLAIN SELECT * FROM CHILDREN EXCEPT SELECT * FROM CHILDREN WHERE CLASS=0;
> PLAN
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> (SELECT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.CHILDREN.tableScan */) EXCEPT (SELECT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.CHILDREN.tableScan */ WHERE CLASS = 0)
> rows: 1

SELECT * FROM CHILDREN CH, CLASSES CL WHERE CH.CLASS = CL.ID;
> ID NAME  CLASS ID NAME
> -- ----- ----- -- ------------
> 0  Joe   0     0  Kindergarden
> 1  Anne  1     1  Class1
> 2  Joerg 1     1  Class1
> 3  Petra 2     2  Class2
> rows: 4

SELECT CH.ID CH_ID, CH.NAME CH_NAME, CL.ID CL_ID, CL.NAME CL_NAME FROM CHILDREN CH, CLASSES CL WHERE CH.CLASS = CL.ID;
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- ------------
> 0     Joe     0     Kindergarden
> 1     Anne    1     Class1
> 2     Joerg   1     Class1
> 3     Petra   2     Class2
> rows: 4

CREATE VIEW CHILDREN_CLASSES(CH_ID, CH_NAME, CL_ID, CL_NAME) AS
SELECT CH.ID CH_ID1, CH.NAME CH_NAME2, CL.ID CL_ID3, CL.NAME CL_NAME4
FROM CHILDREN CH, CLASSES CL WHERE CH.CLASS = CL.ID;
> ok

SELECT * FROM CHILDREN_CLASSES WHERE CH_NAME <> 'X';
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- ------------
> 0     Joe     0     Kindergarden
> 1     Anne    1     Class1
> 2     Joerg   1     Class1
> 3     Petra   2     Class2
> rows: 4

CREATE VIEW CHILDREN_CLASS1 AS SELECT * FROM CHILDREN_CLASSES WHERE CL_ID=1;
> ok

SELECT * FROM CHILDREN_CLASS1;
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- -------
> 1     Anne    1     Class1
> 2     Joerg   1     Class1
> rows: 2

CREATE VIEW CHILDREN_CLASS2 AS SELECT * FROM CHILDREN_CLASSES WHERE CL_ID=2;
> ok

SELECT * FROM CHILDREN_CLASS2;
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- -------
> 3     Petra   2     Class2
> rows: 1

CREATE VIEW CHILDREN_CLASS12 AS SELECT * FROM CHILDREN_CLASS1 UNION ALL SELECT * FROM CHILDREN_CLASS1;
> ok

SELECT * FROM CHILDREN_CLASS12;
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- -------
> 1     Anne    1     Class1
> 1     Anne    1     Class1
> 2     Joerg   1     Class1
> 2     Joerg   1     Class1
> rows: 4

DROP VIEW CHILDREN_CLASS2;
> ok

DROP VIEW CHILDREN_CLASS1 cascade;
> ok

DROP VIEW CHILDREN_CLASSES;
> ok

DROP VIEW CHILDREN_CLASS12;
> exception

CREATE VIEW V_UNION AS SELECT * FROM CHILDREN UNION ALL SELECT * FROM CHILDREN;
> ok

SELECT * FROM V_UNION WHERE ID=1;
> ID NAME CLASS
> -- ---- -----
> 1  Anne 1
> 1  Anne 1
> rows: 2

EXPLAIN SELECT * FROM V_UNION WHERE ID=1;
> PLAN
> ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT V_UNION.ID, V_UNION.NAME, V_UNION.CLASS FROM PUBLIC.V_UNION /* (SELECT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /++ PUBLIC.PRIMARY_KEY_9: ID IS ?1 ++/ /++ scanCount: 2 ++/ WHERE CHILDREN.ID IS ?1) UNION ALL (SELECT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /++ PUBLIC.PRIMARY_KEY_9: ID IS ?1 ++/ /++ scanCount: 2 ++/ WHERE CHILDREN.ID IS ?1): ID = 1 */ WHERE ID = 1
> rows: 1

CREATE VIEW V_EXCEPT AS SELECT * FROM CHILDREN EXCEPT SELECT * FROM CHILDREN WHERE ID=2;
> ok

SELECT * FROM V_EXCEPT WHERE ID=1;
> ID NAME CLASS
> -- ---- -----
> 1  Anne 1
> rows: 1

EXPLAIN SELECT * FROM V_EXCEPT WHERE ID=1;
> PLAN
> ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT V_EXCEPT.ID, V_EXCEPT.NAME, V_EXCEPT.CLASS FROM PUBLIC.V_EXCEPT /* (SELECT DISTINCT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /++ PUBLIC.PRIMARY_KEY_9: ID IS ?1 ++/ /++ scanCount: 2 ++/ WHERE CHILDREN.ID IS ?1) EXCEPT (SELECT DISTINCT CHILDREN.ID, CHILDREN.NAME, CHILDREN.CLASS FROM PUBLIC.CHILDREN /++ PUBLIC.PRIMARY_KEY_9: ID = 2 ++/ /++ scanCount: 2 ++/ WHERE ID = 2): ID = 1 */ WHERE ID = 1
> rows: 1

CREATE VIEW V_INTERSECT AS SELECT ID, NAME FROM CHILDREN INTERSECT SELECT * FROM CLASSES;
> ok

SELECT * FROM V_INTERSECT WHERE ID=1;
> ID NAME
> -- ----
> rows: 0

EXPLAIN SELECT * FROM V_INTERSECT WHERE ID=1;
> PLAN
> ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT V_INTERSECT.ID, V_INTERSECT.NAME FROM PUBLIC.V_INTERSECT /* (SELECT DISTINCT ID, NAME FROM PUBLIC.CHILDREN /++ PUBLIC.PRIMARY_KEY_9: ID IS ?1 ++/ /++ scanCount: 2 ++/ WHERE ID IS ?1) INTERSECT (SELECT DISTINCT CLASSES.ID, CLASSES.NAME FROM PUBLIC.CLASSES /++ PUBLIC.PRIMARY_KEY_5: ID IS ?1 ++/ /++ scanCount: 2 ++/ WHERE CLASSES.ID IS ?1): ID = 1 */ WHERE ID = 1
> rows: 1

DROP VIEW V_UNION;
> ok

DROP VIEW V_EXCEPT;
> ok

DROP VIEW V_INTERSECT;
> ok

DROP TABLE CHILDREN;
> ok

DROP TABLE CLASSES;
> ok

--- view ----------------------------------------------------------------------------------------------
CREATE CACHED TABLE TEST_A(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE CACHED TABLE TEST_B(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

SELECT A.ID AID, A.NAME A_NAME, B.ID BID, B.NAME B_NAME FROM TEST_A A INNER JOIN TEST_B B WHERE A.ID = B.ID;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> rows: 0

INSERT INTO TEST_B VALUES(1, 'Hallo'), (2, 'Welt'), (3, 'Rekord');
> update count: 3

CREATE VIEW IF NOT EXISTS TEST_ALL AS SELECT A.ID AID, A.NAME A_NAME, B.ID BID, B.NAME B_NAME FROM TEST_A A, TEST_B B WHERE A.ID = B.ID;
> ok

SELECT COUNT(*) FROM TEST_ALL;
> COUNT(*)
> --------
> 0
> rows: 1

CREATE VIEW IF NOT EXISTS TEST_ALL AS
SELECT * FROM TEST_A;
> ok

INSERT INTO TEST_A VALUES(1, 'Hello'), (2, 'World'), (3, 'Record');
> update count: 3

SELECT * FROM TEST_ALL;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> 2   World  2   Welt
> 3   Record 3   Rekord
> rows: 3

SELECT * FROM TEST_ALL WHERE  AID=1;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> rows: 1

SELECT * FROM TEST_ALL WHERE AID>0;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> 2   World  2   Welt
> 3   Record 3   Rekord
> rows: 3

SELECT * FROM TEST_ALL WHERE AID<2;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> rows: 1

SELECT * FROM TEST_ALL WHERE AID<=2;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> 2   World  2   Welt
> rows: 2

SELECT * FROM TEST_ALL WHERE AID>=2;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 2   World  2   Welt
> 3   Record 3   Rekord
> rows: 2

CREATE VIEW TEST_A_SUB AS SELECT * FROM TEST_A WHERE ID < 2;
> ok

SELECT TABLE_NAME, SQL FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='VIEW';
> TABLE_NAME SQL
> ---------- -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> TEST_ALL   CREATE FORCE VIEW PUBLIC.TEST_ALL(AID, A_NAME, BID, B_NAME) AS SELECT A.ID AS AID, A.NAME AS A_NAME, B.ID AS BID, B.NAME AS B_NAME FROM PUBLIC.TEST_A A INNER JOIN PUBLIC.TEST_B B ON 1=1 WHERE A.ID = B.ID
> TEST_A_SUB CREATE FORCE VIEW PUBLIC.TEST_A_SUB(ID, NAME) AS SELECT TEST_A.ID, TEST_A.NAME FROM PUBLIC.TEST_A WHERE ID < 2
> rows: 2

SELECT * FROM TEST_A_SUB WHERE NAME IS NOT NULL;
> ID NAME
> -- -----
> 1  Hello
> rows: 1

DROP VIEW TEST_A_SUB;
> ok

DROP TABLE TEST_A cascade;
> ok

DROP TABLE TEST_B cascade;
> ok

DROP VIEW TEST_ALL;
> exception

DROP VIEW IF EXISTS TEST_ALL;
> ok

--- commit/rollback ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

SET AUTOCOMMIT FALSE;
> ok

INSERT INTO TEST VALUES(1, 'Test');
> update count: 1

ROLLBACK;
> ok

SELECT * FROM TEST;
> ID NAME
> -- ----
> rows: 0

INSERT INTO TEST VALUES(1, 'Test2');
> update count: 1

SAVEPOINT TEST;
> ok

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

ROLLBACK TO SAVEPOINT NOT_EXISTING;
> exception

ROLLBACK TO SAVEPOINT TEST;
> ok

SELECT * FROM TEST;
> ID NAME
> -- -----
> 1  Test2
> rows: 1

ROLLBACK WORK;
> ok

SELECT * FROM TEST;
> ID NAME
> -- ----
> rows: 0

INSERT INTO TEST VALUES(1, 'Test3');
> update count: 1

SAVEPOINT TEST3;
> ok

INSERT INTO TEST VALUES(2, 'World2');
> update count: 1

ROLLBACK TO SAVEPOINT TEST3;
> ok

COMMIT WORK;
> ok

SELECT * FROM TEST;
> ID NAME
> -- -----
> 1  Test3
> rows: 1

SET AUTOCOMMIT TRUE;
> ok

DROP TABLE TEST;
> ok

--- insert..select ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(0, 'Hello');
> update count: 1

INSERT INTO TEST SELECT ID+1, NAME||'+' FROM TEST;
> update count: 1

INSERT INTO TEST SELECT ID+2, NAME||'+' FROM TEST;
> update count: 2

INSERT INTO TEST SELECT ID+4, NAME||'+' FROM TEST;
> update count: 4

SELECT * FROM TEST;
> ID NAME
> -- --------
> 0  Hello
> 1  Hello+
> 2  Hello+
> 3  Hello++
> 4  Hello+
> 5  Hello++
> 6  Hello++
> 7  Hello+++
> rows: 8

DROP TABLE TEST;
> ok

--- range ----------------------------------------------------------------------------------------------
--import java.math.*;
--int s=0;for(int i=2;i<=1000;i++)
--s+=BigInteger.valueOf(i).isProbablePrime(10000)?i:0;s;
select sum(x) from system_range(2, 1000) r where
not exists(select * from system_range(2, 32) r2 where r.x>r2.x and mod(r.x, r2.x)=0);
> SUM(X)
> ------
> 76127
> rows: 1

SELECT COUNT(*) FROM SYSTEM_RANGE(0, 2111222333);
> COUNT(*)
> ----------
> 2111222334
> rows: 1

select * from system_range(2, 100) r where
not exists(select * from system_range(2, 11) r2 where r.x>r2.x and mod(r.x, r2.x)=0);
> X
> --
> 11
> 13
> 17
> 19
> 2
> 23
> 29
> 3
> 31
> 37
> 41
> 43
> 47
> 5
> 53
> 59
> 61
> 67
> 7
> 71
> 73
> 79
> 83
> 89
> 97
> rows: 25

--- syntax errors ----------------------------------------------------------------------------------------------
CREATE SOMETHING STRANGE;
> exception

SELECT T1.* T2;
> exception

select replace('abchihihi', 'i', 'o') abcehohoho, replace('this is tom', 'i') 1e_th_st_om from test;
> exception

select monthname(date )'005-0E9-12') d_set fm test;
> exception

call substring('bob', 2, -1);
> ''
> --
>
> rows: 1

--- like ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(0, NULL);
> update count: 1

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

INSERT INTO TEST VALUES(3, 'Word');
> update count: 1

INSERT INTO TEST VALUES(4, 'Wo%');
> update count: 1

SELECT * FROM TEST WHERE NAME IS NULL;
> ID NAME
> -- ----
> 0  null
> rows: 1

SELECT * FROM TEST WHERE NAME IS NOT NULL;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> 3  Word
> 4  Wo%
> rows: 4

SELECT * FROM TEST WHERE NAME BETWEEN 'H' AND 'Word';
> ID NAME
> -- -----
> 1  Hello
> 3  Word
> 4  Wo%
> rows: 3

SELECT * FROM TEST WHERE ID >= 2 AND ID <= 3 AND ID <> 2;
> ID NAME
> -- ----
> 3  Word
> rows: 1

SELECT * FROM TEST WHERE ID>0 AND ID<4 AND ID!=2;
> ID NAME
> -- -----
> 1  Hello
> 3  Word
> rows: 2

SELECT * FROM TEST WHERE 'Hello' LIKE '_el%';
> ID NAME
> -- -----
> 0  null
> 1  Hello
> 2  World
> 3  Word
> 4  Wo%
> rows: 5

SELECT * FROM TEST WHERE NAME LIKE 'Hello%';
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT * FROM TEST WHERE NAME ILIKE 'hello%';
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT * FROM TEST WHERE NAME ILIKE 'xxx%';
> ID NAME
> -- ----
> rows: 0

SELECT * FROM TEST WHERE NAME LIKE 'Wo%';
> ID NAME
> -- -----
> 2  World
> 3  Word
> 4  Wo%
> rows: 3

SELECT * FROM TEST WHERE NAME LIKE 'Wo\%';
> ID NAME
> -- ----
> 4  Wo%
> rows: 1

SELECT * FROM TEST WHERE NAME LIKE 'WoX%' ESCAPE 'X';
> ID NAME
> -- ----
> 4  Wo%
> rows: 1

SELECT * FROM TEST WHERE NAME LIKE 'Word_';
> ID NAME
> -- ----
> rows: 0

SELECT * FROM TEST WHERE NAME LIKE '%Hello%';
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT * FROM TEST WHERE 'Hello' LIKE NAME;
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT T1.*, T2.* FROM TEST AS T1, TEST AS T2 WHERE T1.ID = T2.ID AND T1.NAME LIKE T2.NAME || '%';
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  Hello
> 2  World 2  World
> 3  Word  3  Word
> 4  Wo%   4  Wo%
> rows: 4

SELECT ID, MAX(NAME) FROM TEST GROUP BY ID HAVING MAX(NAME) = 'World';
> ID MAX(NAME)
> -- ---------
> 2  World
> rows: 1

SELECT ID, MAX(NAME) FROM TEST GROUP BY ID HAVING MAX(NAME) LIKE 'World%';
> ID MAX(NAME)
> -- ---------
> 2  World
> rows: 1

DROP TABLE TEST;
> ok

--- remarks/comments/syntax ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(
ID INT PRIMARY KEY, -- this is the primary key, type {integer}
NAME VARCHAR(255) -- this is a string
);
> ok

INSERT INTO TEST VALUES(
1 /* ID */,
'Hello' // NAME
);
> update count: 1

SELECT * FROM TEST;
> ID NAME
> -- -----
> 1  Hello
> rows: 1

DROP_ TABLE_ TEST_T;
> exception

DROP TABLE TEST /*;
> exception

DROP TABLE TEST;
> ok

--- exists ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(0, NULL);
> update count: 1

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT * FROM TEST T WHERE NOT EXISTS(
SELECT * FROM TEST T2 WHERE T.ID > T2.ID);
> ID NAME
> -- ----
> 0  null
> rows: 1

DROP TABLE TEST;
> ok

--- subquery ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(0, NULL);
> update count: 1

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

select * from test where (select max(t1.id) from test t1) between 0 and 100;
> ID NAME
> -- -----
> 0  null
> 1  Hello
> rows: 2

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT * FROM TEST T WHERE T.ID = (SELECT T2.ID FROM TEST T2 WHERE T2.ID=T.ID);
> ID NAME
> -- -----
> 0  null
> 1  Hello
> 2  World
> rows: 3

SELECT (SELECT T2.NAME FROM TEST T2 WHERE T2.ID=T.ID), T.NAME FROM TEST T;
> SELECT T2.NAME FROM PUBLIC.TEST T2 /* PUBLIC.PRIMARY_KEY_2: ID = T.ID */ /* scanCount: 2 */ WHERE T2.ID = T.ID NAME
> -------------------------------------------------------------------------------------------------------------- -----
> Hello                                                                                                          Hello
> World                                                                                                          World
> null                                                                                                           null
> rows: 3

SELECT (SELECT SUM(T2.ID) FROM TEST T2 WHERE T2.ID>T.ID), T.ID FROM TEST T;
> SELECT SUM(T2.ID) FROM PUBLIC.TEST T2 /* PUBLIC.PRIMARY_KEY_2: ID > T.ID */ /* scanCount: 2 */ WHERE T2.ID > T.ID ID
> ----------------------------------------------------------------------------------------------------------------- --
> 2                                                                                                                 1
> 3                                                                                                                 0
> null                                                                                                              2
> rows: 3

select * from test t where t.id+1 in (select id from test);
> ID NAME
> -- -----
> 0  null
> 1  Hello
> rows: 2

select * from test t where t.id in (select id from test where id=t.id);
> ID NAME
> -- -----
> 0  null
> 1  Hello
> 2  World
> rows: 3

select 1 from test, test where 1 in (select 1 from test where id=1);
> 1
> -
> 1
> 1
> 1
> 1
> 1
> 1
> 1
> 1
> 1
> rows: 9

select * from test, test where id=id;
> exception

select 1 from test, test where id=id;
> exception

select 1 from test where id in (select id from test, test);
> exception

DROP TABLE TEST;
> ok

--- group by ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(A INT, B INT, VALUE INT, UNIQUE(A, B));
> ok

INSERT INTO TEST VALUES(?, ?, ?);
{
NULL, NULL, NULL
NULL, 0, 0
NULL, 1, 10
0, 0, -1
0, 1, 100
1, 0, 200
1, 1, 300
};
> update count: 7

SELECT A, B, COUNT(*) CAL, COUNT(A) CA, COUNT(B) CB, MIN(VALUE) MI, MAX(VALUE) MA, SUM(VALUE) S FROM TEST GROUP BY A, B;
> A    B    CAL CA CB MI   MA   S
> ---- ---- --- -- -- ---- ---- ----
> 0    0    1   1  1  -1   -1   -1
> 0    1    1   1  1  100  100  100
> 1    0    1   1  1  200  200  200
> 1    1    1   1  1  300  300  300
> null 0    1   0  1  0    0    0
> null 1    1   0  1  10   10   10
> null null 1   0  0  null null null
> rows: 7

DROP TABLE TEST;
> ok

--- data types (blob, clob, varchar_ignorecase) ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT, XB BINARY, XBL BLOB, XO OTHER, XCL CLOB, XVI VARCHAR_IGNORECASE);
> ok

INSERT INTO TEST VALUES(0, X '', '', '', '', '');
> update count: 1

INSERT INTO TEST VALUES(1, X '0101', '0101', '0101', 'abc', 'aa');
> update count: 1

INSERT INTO TEST VALUES(2, X '0AFF', '08FE', 'F0F1', 'AbCdEfG', 'ZzAaBb');
> update count: 1

INSERT INTO TEST VALUES(3, X '112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff', '112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff', '112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff', 'AbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYz', 'AbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYz');
> update count: 1

INSERT INTO TEST VALUES(4, NULL, NULL, NULL, NULL, NULL);
> update count: 1

SELECT * FROM TEST;
> ID XB                                                                                                                                                                                                                                                                                                           XBL                                                                                                                                                                                                                                                                                                          XO                                                                                                                                                                                                                                                                                                           XCL                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      XVI
> -- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> 0
> 1  0101                                                                                                                                                                                                                                                                                                         0101                                                                                                                                                                                                                                                                                                         0101                                                                                                                                                                                                                                                                                                         abc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      aa
> 2  0aff                                                                                                                                                                                                                                                                                                         08fe                                                                                                                                                                                                                                                                                                         f0f1                                                                                                                                                                                                                                                                                                         AbCdEfG                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  ZzAaBb
> 3  112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff 112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff 112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff AbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYz AbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYz
> 4  null                                                                                                                                                                                                                                                                                                         null                                                                                                                                                                                                                                                                                                         null                                                                                                                                                                                                                                                                                                         null                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     null
> rows: 5

SELECT ID FROM TEST WHERE XCL = XCL;
> ID
> --
> 0
> 1
> 2
> 3
> rows: 4

SELECT ID FROM TEST WHERE XCL LIKE 'abc%';
> ID
> --
> 1
> rows: 1

SELECT ID FROM TEST WHERE XVI LIKE 'abc%';
> ID
> --
> 3
> rows: 1

SELECT 'abc', 'Papa Joe''s', CAST(-1 AS SMALLINT), CAST(2 AS BIGINT), CAST(0 AS DOUBLE), CAST('0a0f' AS BINARY), CAST(125 AS TINYINT), TRUE, FALSE FROM TEST WHERE ID=1;
> 'abc' 'Papa Joe''s' -1 2 0.0 X'0a0f' 125 TRUE FALSE
> ----- ------------- -- - --- ------- --- ---- -----
> abc   Papa Joe's    -1 2 0.0 0a0f    125 TRUE FALSE
> rows: 1

SELECT CAST('abcd' AS VARCHAR(255)), CAST('ef_gh' AS VARCHAR(3));
> 'abcd' 'ef_'
> ------ -----
> abcd   ef_
> rows: 1

DROP TABLE TEST;
> ok

--- data types (date and time) ----------------------------------------------------------------------------------------------
CREATE MEMORY TABLE TEST(ID INT, XT TIME, XD DATE, XTS TIMESTAMP(9));
> ok

INSERT INTO TEST VALUES(0, '0:0:0','1-2-3','2-3-4 0:0:0');
> update count: 1

INSERT INTO TEST VALUES(1, '01:02:03','2001-02-03','2001-02-29 0:0:0');
> exception

INSERT INTO TEST VALUES(1, '24:62:03','2001-02-03','2001-02-01 0:0:0');
> exception

INSERT INTO TEST VALUES(1, '23:02:03','2001-04-31','2001-02-01 0:0:0');
> exception

INSERT INTO TEST VALUES(1,'1:2:3','4-5-6','7-8-9 0:1:2');
> update count: 1

INSERT INTO TEST VALUES(2,'23:59:59','1999-12-31','1999-12-31 23:59:59.123456789');
> update count: 1

INSERT INTO TEST VALUES(NULL,NULL,NULL,NULL);
> update count: 1

SELECT * FROM TEST;
> ID   XT       XD         XTS
> ---- -------- ---------- -----------------------------
> 0    00:00:00 0001-02-03 0002-03-04 00:00:00
> 1    01:02:03 0004-05-06 0007-08-09 00:01:02
> 2    23:59:59 1999-12-31 1999-12-31 23:59:59.123456789
> null null     null       null
> rows: 4

SELECT XD+1, XD-1, XD-XD FROM TEST;
> DATEADD('DAY', 1, XD) DATEADD('DAY', -1, XD) DATEDIFF('DAY', XD, XD)
> --------------------- ---------------------- -----------------------
> 0001-02-04            0001-02-02             0
> 0004-05-07            0004-05-05             0
> 2000-01-01            1999-12-30             0
> null                  null                   null
> rows: 4

SELECT ID, CAST(XT AS DATE) T2D, CAST(XTS AS DATE) TS2D,
CAST(XD AS TIME) D2T, CAST(XTS AS TIME(9)) TS2T,
CAST(XT AS TIMESTAMP) D2TS, CAST(XD AS TIMESTAMP) D2TS FROM TEST;
> ID   T2D        TS2D       D2T      TS2T               D2TS                D2TS
> ---- ---------- ---------- -------- ------------------ ------------------- -------------------
> 0    1970-01-01 0002-03-04 00:00:00 00:00:00           1970-01-01 00:00:00 0001-02-03 00:00:00
> 1    1970-01-01 0007-08-09 00:00:00 00:01:02           1970-01-01 01:02:03 0004-05-06 00:00:00
> 2    1970-01-01 1999-12-31 00:00:00 23:59:59.123456789 1970-01-01 23:59:59 1999-12-31 00:00:00
> null null       null       null     null               null                null
> rows: 4

SCRIPT SIMPLE NOPASSWORDS NOSETTINGS;
> SCRIPT
> ----------------------------------------------------------------------------------------------------------------------------------
> -- 4 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT, XT TIME, XD DATE, XTS TIMESTAMP(9) );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.TEST(ID, XT, XD, XTS) VALUES(0, TIME '00:00:00', DATE '0001-02-03', TIMESTAMP '0002-03-04 00:00:00');
> INSERT INTO PUBLIC.TEST(ID, XT, XD, XTS) VALUES(1, TIME '01:02:03', DATE '0004-05-06', TIMESTAMP '0007-08-09 00:01:02');
> INSERT INTO PUBLIC.TEST(ID, XT, XD, XTS) VALUES(2, TIME '23:59:59', DATE '1999-12-31', TIMESTAMP '1999-12-31 23:59:59.123456789');
> INSERT INTO PUBLIC.TEST(ID, XT, XD, XTS) VALUES(NULL, NULL, NULL, NULL);
> rows: 7

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, t0 timestamp(23, 0), t1 timestamp(23, 1), t2 timestamp(23, 2), t5 timestamp(23, 5));
> ok

INSERT INTO TEST VALUES(1, '2001-01-01 12:34:56.789123', '2001-01-01 12:34:56.789123', '2001-01-01 12:34:56.789123', '2001-01-01 12:34:56.789123');
> update count: 1

select * from test;
> ID T0                  T1                    T2                     T5
> -- ------------------- --------------------- ---------------------- -------------------------
> 1  2001-01-01 12:34:57 2001-01-01 12:34:56.8 2001-01-01 12:34:56.79 2001-01-01 12:34:56.78912
> rows: 1

DROP TABLE IF EXISTS TEST;
> ok

--- data types (decimal) ----------------------------------------------------------------------------------------------
CALL 1.2E10+1;
> 12000000001
> -----------
> 12000000001
> rows: 1

CALL -1.2E-10-1;
> -1.00000000012
> --------------
> -1.00000000012
> rows: 1

CALL 1E-1;
> 0.1
> ---
> 0.1
> rows: 1

CREATE TABLE TEST(ID INT, X1 BIT, XT TINYINT, X_SM SMALLINT, XB BIGINT, XD DECIMAL(10,2), XD2 DOUBLE PRECISION, XR REAL);
> ok

INSERT INTO TEST VALUES(?, ?, ?, ?, ?, ?, ?, ?);
{
0,FALSE,0,0,0,0.0,0.0,0.0
1,TRUE,1,1,1,1.0,1.0,1.0
4,TRUE,4,4,4,4.0,4.0,4.0
-1,FALSE,-1,-1,-1,-1.0,-1.0,-1.0
NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
};
> update count: 5

SELECT *, 0xFF, -0x1234567890abcd FROM TEST;
> ID   X1    XT   X_SM XB   XD    XD2  XR   255 -5124095575370701
> ---- ----- ---- ---- ---- ----- ---- ---- --- -----------------
> -1   FALSE -1   -1   -1   -1.00 -1.0 -1.0 255 -5124095575370701
> 0    FALSE 0    0    0    0.00  0.0  0.0  255 -5124095575370701
> 1    TRUE  1    1    1    1.00  1.0  1.0  255 -5124095575370701
> 4    TRUE  4    4    4    4.00  4.0  4.0  255 -5124095575370701
> null null  null null null null  null null 255 -5124095575370701
> rows: 5

SELECT XD, CAST(XD AS DECIMAL(10,1)) D2DE, CAST(XD2 AS DECIMAL(4, 3)) DO2DE, CAST(XR AS DECIMAL(20,3)) R2DE FROM TEST;
> XD    D2DE DO2DE  R2DE
> ----- ---- ------ ------
> -1.00 -1.0 -1.000 -1.000
> 0.00  0.0  0.000  0.000
> 1.00  1.0  1.000  1.000
> 4.00  4.0  4.000  4.000
> null  null null   null
> rows: 5

SELECT ID, CAST(XB AS DOUBLE) L2D, CAST(X_SM AS DOUBLE) S2D, CAST(XT AS DOUBLE) X2D FROM TEST;
> ID   L2D  S2D  X2D
> ---- ---- ---- ----
> -1   -1.0 -1.0 -1.0
> 0    0.0  0.0  0.0
> 1    1.0  1.0  1.0
> 4    4.0  4.0  4.0
> null null null null
> rows: 5

SELECT ID, CAST(XB AS REAL) L2D, CAST(X_SM AS REAL) S2D, CAST(XT AS REAL) T2R FROM TEST;
> ID   L2D  S2D  T2R
> ---- ---- ---- ----
> -1   -1.0 -1.0 -1.0
> 0    0.0  0.0  0.0
> 1    1.0  1.0  1.0
> 4    4.0  4.0  4.0
> null null null null
> rows: 5

SELECT ID, CAST(X_SM AS BIGINT) S2L, CAST(XT AS BIGINT) B2L, CAST(XD2 AS BIGINT) D2L, CAST(XR AS BIGINT) R2L FROM TEST;
> ID   S2L  B2L  D2L  R2L
> ---- ---- ---- ---- ----
> -1   -1   -1   -1   -1
> 0    0    0    0    0
> 1    1    1    1    1
> 4    4    4    4    4
> null null null null null
> rows: 5

SELECT ID, CAST(XB AS INT) L2I, CAST(XD2 AS INT) D2I, CAST(XD2 AS SMALLINT) DO2I, CAST(XR AS SMALLINT) R2I FROM TEST;
> ID   L2I  D2I  DO2I R2I
> ---- ---- ---- ---- ----
> -1   -1   -1   -1   -1
> 0    0    0    0    0
> 1    1    1    1    1
> 4    4    4    4    4
> null null null null null
> rows: 5

SELECT ID, CAST(XD AS SMALLINT) D2S, CAST(XB AS SMALLINT) L2S, CAST(XT AS SMALLINT) B2S FROM TEST;
> ID   D2S  L2S  B2S
> ---- ---- ---- ----
> -1   -1   -1   -1
> 0    0    0    0
> 1    1    1    1
> 4    4    4    4
> null null null null
> rows: 5

SELECT ID, CAST(XD2 AS TINYINT) D2B, CAST(XD AS TINYINT) DE2B, CAST(XB AS TINYINT) L2B, CAST(X_SM AS TINYINT) S2B FROM TEST;
> ID   D2B  DE2B L2B  S2B
> ---- ---- ---- ---- ----
> -1   -1   -1   -1   -1
> 0    0    0    0    0
> 1    1    1    1    1
> 4    4    4    4    4
> null null null null null
> rows: 5

SELECT ID, CAST(XD2 AS BIT) D2B, CAST(XD AS BIT) DE2B, CAST(XB AS BIT) L2B, CAST(X_SM AS BIT) S2B FROM TEST;
> ID   D2B   DE2B  L2B   S2B
> ---- ----- ----- ----- -----
> -1   TRUE  TRUE  TRUE  TRUE
> 0    FALSE FALSE FALSE FALSE
> 1    TRUE  TRUE  TRUE  TRUE
> 4    TRUE  TRUE  TRUE  TRUE
> null null  null  null  null
> rows: 5

SELECT CAST('TRUE' AS BIT) NT, CAST('1.0' AS BIT) N1, CAST('0.0' AS BIT) N0;
> NT   N1   N0
> ---- ---- -----
> TRUE TRUE FALSE
> rows: 1

SELECT ID, ID+X1, ID+XT, ID+X_SM, ID+XB, ID+XD, ID+XD2, ID+XR FROM TEST;
> ID   ID + X1 ID + XT ID + X_SM ID + XB ID + XD ID + XD2 ID + XR
> ---- ------- ------- --------- ------- ------- -------- -------
> -1   -1      -2      -2        -2      -2.00   -2.0     -2.0
> 0    0       0       0         0       0.00    0.0      0.0
> 1    2       2       2         2       2.00    2.0      2.0
> 4    5       8       8         8       8.00    8.0      8.0
> null null    null    null      null    null    null     null
> rows: 5

SELECT ID, 10-X1, 10-XT, 10-X_SM, 10-XB, 10-XD, 10-XD2, 10-XR FROM TEST;
> ID   10 - X1 10 - XT 10 - X_SM 10 - XB 10 - XD 10 - XD2 10 - XR
> ---- ------- ------- --------- ------- ------- -------- -------
> -1   10      11      11        11      11.00   11.0     11.0
> 0    10      10      10        10      10.00   10.0     10.0
> 1    9       9       9         9       9.00    9.0      9.0
> 4    9       6       6         6       6.00    6.0      6.0
> null null    null    null      null    null    null     null
> rows: 5

SELECT ID, 10*X1, 10*XT, 10*X_SM, 10*XB, 10*XD, 10*XD2, 10*XR FROM TEST;
> ID   10 * X1 10 * XT 10 * X_SM 10 * XB 10 * XD 10 * XD2 10 * XR
> ---- ------- ------- --------- ------- ------- -------- -------
> -1   0       -10     -10       -10     -10.00  -10.0    -10.0
> 0    0       0       0         0       0.00    0.0      0.0
> 1    10      10      10        10      10.00   10.0     10.0
> 4    10      40      40        40      40.00   40.0     40.0
> null null    null    null      null    null    null     null
> rows: 5

SELECT ID, SIGN(XT), SIGN(X_SM), SIGN(XB), SIGN(XD), SIGN(XD2), SIGN(XR) FROM TEST;
> ID   SIGN(XT) SIGN(X_SM) SIGN(XB) SIGN(XD) SIGN(XD2) SIGN(XR)
> ---- -------- ---------- -------- -------- --------- --------
> -1   -1       -1         -1       -1       -1        -1
> 0    0        0          0        0        0         0
> 1    1        1          1        1        1         1
> 4    1        1          1        1        1         1
> null null     null       null     null     null      null
> rows: 5

SELECT ID, XT-XT-XT, X_SM-X_SM-X_SM, XB-XB-XB, XD-XD-XD, XD2-XD2-XD2, XR-XR-XR FROM TEST;
> ID   (XT - XT) - XT (X_SM - X_SM) - X_SM (XB - XB) - XB (XD - XD) - XD (XD2 - XD2) - XD2 (XR - XR) - XR
> ---- -------------- -------------------- -------------- -------------- ----------------- --------------
> -1   1              1                    1              1.00           1.0               1.0
> 0    0              0                    0              0.00           0.0               0.0
> 1    -1             -1                   -1             -1.00          -1.0              -1.0
> 4    -4             -4                   -4             -4.00          -4.0              -4.0
> null null           null                 null           null           null              null
> rows: 5

SELECT ID, XT+XT, X_SM+X_SM, XB+XB, XD+XD, XD2+XD2, XR+XR FROM TEST;
> ID   XT + XT X_SM + X_SM XB + XB XD + XD XD2 + XD2 XR + XR
> ---- ------- ----------- ------- ------- --------- -------
> -1   -2      -2          -2      -2.00   -2.0      -2.0
> 0    0       0           0       0.00    0.0       0.0
> 1    2       2           2       2.00    2.0       2.0
> 4    8       8           8       8.00    8.0       8.0
> null null    null        null    null    null      null
> rows: 5

SELECT ID, XT*XT, X_SM*X_SM, XB*XB, XD*XD, XD2*XD2, XR*XR FROM TEST;
> ID   XT * XT X_SM * X_SM XB * XB XD * XD XD2 * XD2 XR * XR
> ---- ------- ----------- ------- ------- --------- -------
> -1   1       1           1       1.0000  1.0       1.0
> 0    0       0           0       0.0000  0.0       0.0
> 1    1       1           1       1.0000  1.0       1.0
> 4    16      16          16      16.0000 16.0      16.0
> null null    null        null    null    null      null
> rows: 5

SELECT 2/3 FROM TEST WHERE ID=1;
> 0
> -
> 0
> rows: 1

SELECT ID/ID FROM TEST;
> exception

SELECT XT/XT FROM TEST;
> exception

SELECT X_SM/X_SM FROM TEST;
> exception

SELECT XB/XB FROM TEST;
> exception

SELECT XD/XD FROM TEST;
> exception

SELECT XD2/XD2 FROM TEST;
> exception

SELECT XR/XR FROM TEST;
> exception

SELECT ID++0, -X1, -XT, -X_SM, -XB, -XD, -XD2, -XR FROM TEST;
> ID + 0 - X1  - XT - X_SM - XB - XD  - XD2 - XR
> ------ ----- ---- ------ ---- ----- ----- ----
> -1     TRUE  1    1      1    1.00  1.0   1.0
> 0      TRUE  0    0      0    0.00  0.0   0.0
> 1      FALSE -1   -1     -1   -1.00 -1.0  -1.0
> 4      FALSE -4   -4     -4   -4.00 -4.0  -4.0
> null   null  null null   null null  null  null
> rows: 5

SELECT ID, X1||'!', XT||'!', X_SM||'!', XB||'!', XD||'!', XD2||'!', XR||'!' FROM TEST;
> ID   X1 || '!' XT || '!' X_SM || '!' XB || '!' XD || '!' XD2 || '!' XR || '!'
> ---- --------- --------- ----------- --------- --------- ---------- ---------
> -1   FALSE!    -1!       -1!         -1!       -1.00!    -1.0!      -1.0!
> 0    FALSE!    0!        0!          0!        0.00!     0.0!       0.0!
> 1    TRUE!     1!        1!          1!        1.00!     1.0!       1.0!
> 4    TRUE!     4!        4!          4!        4.00!     4.0!       4.0!
> null null      null      null        null      null      null       null
> rows: 5

DROP TABLE TEST;
> ok

--- in ----------------------------------------------------------------------------------------------
CREATE TABLE CUSTOMER(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE TABLE INVOICE(ID INT, CUSTOMER_ID INT, PRIMARY KEY(CUSTOMER_ID, ID), VALUE DECIMAL(10,2));
> ok

INSERT INTO CUSTOMER VALUES(?, ?);
{
1,Lehmann
2,Meier
3,Scott
4,NULL
};
> update count: 4

INSERT INTO INVOICE VALUES(?, ?, ?);
{
10,1,100.10
11,1,10.01
12,1,1.001
20,2,22.2
21,2,200.02
};
> update count: 5

SELECT * FROM CUSTOMER WHERE ID IN(1,2,4,-1);
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> 4  null
> rows: 3

SELECT * FROM CUSTOMER WHERE ID NOT IN(3,4,5,'1');
> ID NAME
> -- -----
> 2  Meier
> rows: 1

SELECT * FROM CUSTOMER WHERE ID NOT IN(SELECT CUSTOMER_ID FROM INVOICE);
> ID NAME
> -- -----
> 3  Scott
> 4  null
> rows: 2

SELECT * FROM INVOICE WHERE CUSTOMER_ID IN(SELECT C.ID FROM CUSTOMER C);
> ID CUSTOMER_ID VALUE
> -- ----------- ------
> 10 1           100.10
> 11 1           10.01
> 12 1           1.00
> 20 2           22.20
> 21 2           200.02
> rows: 5

SELECT * FROM CUSTOMER WHERE NAME IN('Lehmann', 20);
> ID NAME
> -- -------
> 1  Lehmann
> rows: 1

SELECT * FROM CUSTOMER WHERE NAME NOT IN('Scott');
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> rows: 2

SELECT * FROM CUSTOMER WHERE NAME IN(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> 3  Scott
> rows: 3

SELECT * FROM CUSTOMER WHERE NAME NOT IN(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- ----
> rows: 0

SELECT * FROM CUSTOMER WHERE NAME = ANY(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> 3  Scott
> rows: 3

SELECT * FROM CUSTOMER WHERE NAME = ALL(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- ----
> rows: 0

SELECT * FROM CUSTOMER WHERE NAME > ALL(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- ----
> rows: 0

SELECT * FROM CUSTOMER WHERE NAME > ANY(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- -----
> 2  Meier
> 3  Scott
> rows: 2

SELECT * FROM CUSTOMER WHERE NAME < ANY(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> rows: 2

DROP TABLE INVOICE;
> ok

DROP TABLE CUSTOMER;
> ok

--- aggregates ----------------------------------------------------------------------------------------------
drop table if exists t;
> ok

create table t(x double precision, y double precision);
> ok

create view s as
select stddev_pop(x) s_px, stddev_samp(x) s_sx, var_pop(x) v_px, var_samp(x) v_sx,
stddev_pop(y) s_py, stddev_samp(y) s_sy, var_pop(y) v_py, var_samp(y) v_sy from t;
> ok

select var(100000000.1) z from system_range(1, 1000000);
> Z
> ---
> 0.0
> rows: 1

select * from s;
> S_PX S_SX V_PX V_SX S_PY S_SY V_PY V_SY
> ---- ---- ---- ---- ---- ---- ---- ----
> null null null null null null null null
> rows: 1

select some(y>10), every(y>10), min(y), max(y) from t;
> BOOL_OR(Y > 10.0) BOOL_AND(Y > 10.0) MIN(Y) MAX(Y)
> ----------------- ------------------ ------ ------
> null              null               null   null
> rows: 1

insert into t values(1000000004, 4);
> update count: 1

select * from s;
> S_PX S_SX V_PX V_SX S_PY S_SY V_PY V_SY
> ---- ---- ---- ---- ---- ---- ---- ----
> 0.0  null 0.0  null 0.0  null 0.0  null
> rows: 1

insert into t values(1000000007, 7);
> update count: 1

select * from s;
> S_PX S_SX               V_PX V_SX S_PY S_SY               V_PY V_SY
> ---- ------------------ ---- ---- ---- ------------------ ---- ----
> 1.5  2.1213203435596424 2.25 4.5  1.5  2.1213203435596424 2.25 4.5
> rows: 1

insert into t values(1000000013, 13);
> update count: 1

select * from s;
> S_PX               S_SX             V_PX V_SX S_PY               S_SY             V_PY V_SY
> ------------------ ---------------- ---- ---- ------------------ ---------------- ---- ----
> 3.7416573867739413 4.58257569495584 14.0 21.0 3.7416573867739413 4.58257569495584 14.0 21.0
> rows: 1

insert into t values(1000000016, 16);
> update count: 1

select * from s;
> S_PX              S_SX              V_PX V_SX S_PY              S_SY              V_PY V_SY
> ----------------- ----------------- ---- ---- ----------------- ----------------- ---- ----
> 4.743416490252569 5.477225575051661 22.5 30.0 4.743416490252569 5.477225575051661 22.5 30.0
> rows: 1

insert into t values(1000000016, 16);
> update count: 1

select * from s;
> S_PX              S_SX              V_PX              V_SX               S_PY              S_SY              V_PY  V_SY
> ----------------- ----------------- ----------------- ------------------ ----------------- ----------------- ----- ------------------
> 4.874423036912116 5.449770630813229 23.75999994277954 29.699999928474426 4.874423042781577 5.449770637375485 23.76 29.700000000000003
> rows: 1

select stddev_pop(distinct x) s_px, stddev_samp(distinct x) s_sx, var_pop(distinct x) v_px, var_samp(distinct x) v_sx,
stddev_pop(distinct y) s_py, stddev_samp(distinct y) s_sy, var_pop(distinct y) v_py, var_samp(distinct y) V_SY from t;
> S_PX              S_SX              V_PX V_SX S_PY              S_SY              V_PY V_SY
> ----------------- ----------------- ---- ---- ----------------- ----------------- ---- ----
> 4.743416490252569 5.477225575051661 22.5 30.0 4.743416490252569 5.477225575051661 22.5 30.0
> rows: 1

select some(y>10), every(y>10), min(y), max(y) from t;
> BOOL_OR(Y > 10.0) BOOL_AND(Y > 10.0) MIN(Y) MAX(Y)
> ----------------- ------------------ ------ ------
> TRUE              FALSE              4.0    16.0
> rows: 1

drop view s;
> ok

drop table t;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255), VALUE DECIMAL(10,2));
> ok

INSERT INTO TEST VALUES(?, ?, ?);
{
1,Apples,1.20
2,Oranges,2.05
3,Cherries,5.10
4,Apples,1.50
5,Apples,1.10
6,Oranges,1.80
7,Bananas,2.50
8,NULL,3.10
9,NULL,-10.0
};
> update count: 9

SELECT IFNULL(NAME, '') || ': ' || GROUP_CONCAT(VALUE ORDER BY NAME, VALUE DESC SEPARATOR ', ') FROM TEST GROUP BY NAME;
> (IFNULL(NAME, '') || ': ') || GROUP_CONCAT(VALUE ORDER BY NAME, VALUE DESC SEPARATOR ', ')
> ------------------------------------------------------------------------------------------
> Apples: 1.50, 1.20, 1.10
> Oranges: 2.05, 1.80
> Bananas: 2.50
> Cherries: 5.10
> : 3.10, -10.00
> rows (ordered): 5

SELECT GROUP_CONCAT(ID ORDER BY ID) FROM TEST;
> GROUP_CONCAT(ID ORDER BY ID)
> ----------------------------
> 1,2,3,4,5,6,7,8,9
> rows (ordered): 1

SELECT STRING_AGG(ID,';') FROM TEST;
> GROUP_CONCAT(ID SEPARATOR ';')
> ------------------------------
> 1;2;3;4;5;6;7;8;9
> rows: 1

SELECT DISTINCT NAME FROM TEST;
> NAME
> --------
> Apples
> Bananas
> Cherries
> Oranges
> null
> rows: 5

SELECT DISTINCT NAME FROM TEST ORDER BY NAME DESC NULLS LAST;
> NAME
> --------
> Oranges
> Cherries
> Bananas
> Apples
> null
> rows (ordered): 5

SELECT DISTINCT NAME FROM TEST ORDER BY NAME DESC NULLS LAST LIMIT 2 OFFSET 1;
> NAME
> --------
> Cherries
> Bananas
> rows (ordered): 2

SELECT NAME, COUNT(*), SUM(VALUE), MAX(VALUE), MIN(VALUE), AVG(VALUE), COUNT(DISTINCT VALUE) FROM TEST GROUP BY NAME;
> NAME     COUNT(*) SUM(VALUE) MAX(VALUE) MIN(VALUE) AVG(VALUE)                    COUNT(DISTINCT VALUE)
> -------- -------- ---------- ---------- ---------- ----------------------------- ---------------------
> Apples   3        3.80       1.50       1.10       1.266666666666666666666666667 3
> Bananas  1        2.50       2.50       2.50       2.5                           1
> Cherries 1        5.10       5.10       5.10       5.1                           1
> Oranges  2        3.85       2.05       1.80       1.925                         2
> null     2        -6.90      3.10       -10.00     -3.45                         2
> rows: 5

SELECT NAME, MAX(VALUE), MIN(VALUE), MAX(VALUE+1)*MIN(VALUE+1) FROM TEST GROUP BY NAME;
> NAME     MAX(VALUE) MIN(VALUE) MAX(VALUE + 1) * MIN(VALUE + 1)
> -------- ---------- ---------- -------------------------------
> Apples   1.50       1.10       5.2500
> Bananas  2.50       2.50       12.2500
> Cherries 5.10       5.10       37.2100
> Oranges  2.05       1.80       8.5400
> null     3.10       -10.00     -36.9000
> rows: 5

DROP TABLE TEST;
> ok

--- order by ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE UNIQUE INDEX IDXNAME ON TEST(NAME);
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

INSERT INTO TEST VALUES(3, NULL);
> update count: 1

SELECT * FROM TEST ORDER BY NAME;
> ID NAME
> -- -----
> 3  null
> 1  Hello
> 2  World
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME DESC;
> ID NAME
> -- -----
> 2  World
> 1  Hello
> 3  null
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME NULLS FIRST;
> ID NAME
> -- -----
> 3  null
> 1  Hello
> 2  World
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME DESC NULLS FIRST;
> ID NAME
> -- -----
> 3  null
> 2  World
> 1  Hello
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME NULLS LAST;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> 3  null
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME DESC NULLS LAST;
> ID NAME
> -- -----
> 2  World
> 1  Hello
> 3  null
> rows (ordered): 3

SELECT ID, '=', NAME FROM TEST ORDER BY 2 FOR UPDATE;
> ID '=' NAME
> -- --- -----
> 1  =   Hello
> 2  =   World
> 3  =   null
> rows (ordered): 3

DROP TABLE TEST;
> ok

--- having ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE INDEX IDXNAME ON TEST(NAME);
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(3, 'World');
> update count: 1

INSERT INTO TEST VALUES(4, 'World');
> update count: 1

INSERT INTO TEST VALUES(5, 'Orange');
> update count: 1

SELECT NAME, SUM(ID) FROM TEST GROUP BY NAME HAVING COUNT(*)>1 ORDER BY NAME;
> NAME  SUM(ID)
> ----- -------
> Hello 3
> World 7
> rows (ordered): 2

DROP INDEX IF EXISTS IDXNAME;
> ok

DROP TABLE TEST;
> ok

--- help ----------------------------------------------------------------------------------------------
HELP ABCDE EF_GH;
> ID SECTION TOPIC SYNTAX TEXT
> -- ------- ----- ------ ----
> rows: 0

--- sequence ----------------------------------------------------------------------------------------------
CREATE CACHED TABLE TEST(ID INT PRIMARY KEY);
> ok

CREATE CACHED TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY);
> ok

CREATE SEQUENCE IF NOT EXISTS TEST_SEQ START WITH 10;
> ok

CREATE SEQUENCE IF NOT EXISTS TEST_SEQ START WITH 20;
> ok

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

CALL CURRVAL('test_seq');
> CURRVAL('test_seq')
> -------------------
> 10
> rows: 1

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

CALL NEXT VALUE FOR TEST_SEQ;
> NEXT VALUE FOR PUBLIC.TEST_SEQ
> ------------------------------
> 12
> rows: 1

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

SELECT * FROM TEST;
> ID
> --
> 10
> 11
> 13
> rows: 3

SELECT TOP 2 * FROM TEST;
> ID
> --
> 10
> 11
> rows: 2

SELECT TOP 2 * FROM TEST ORDER BY ID DESC;
> ID
> --
> 13
> 11
> rows (ordered): 2

ALTER SEQUENCE TEST_SEQ RESTART WITH 20 INCREMENT BY -1;
> ok

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

SELECT * FROM TEST ORDER BY ID ASC;
> ID
> --
> 10
> 11
> 13
> 19
> 20
> rows (ordered): 5

CALL NEXTVAL('test_seq');
> NEXTVAL('test_seq')
> -------------------
> 18
> rows: 1

DROP SEQUENCE IF EXISTS TEST_SEQ;
> ok

DROP SEQUENCE IF EXISTS TEST_SEQ;
> ok

CREATE SEQUENCE TEST_LONG START WITH 90123456789012345 MAXVALUE 90123456789012345 INCREMENT BY -1;
> ok

SET AUTOCOMMIT FALSE;
> ok

CALL NEXT VALUE FOR TEST_LONG;
> NEXT VALUE FOR PUBLIC.TEST_LONG
> -------------------------------
> 90123456789012345
> rows: 1

CALL IDENTITY();
> IDENTITY()
> -----------------
> 90123456789012345
> rows: 1

SELECT SEQUENCE_NAME, CURRENT_VALUE, INCREMENT FROM INFORMATION_SCHEMA.SEQUENCES;
> SEQUENCE_NAME CURRENT_VALUE     INCREMENT
> ------------- ----------------- ---------
> TEST_LONG     90123456789012345 -1
> rows: 1

SET AUTOCOMMIT TRUE;
> ok

DROP SEQUENCE TEST_LONG;
> ok

DROP TABLE TEST;
> ok

--- call ----------------------------------------------------------------------------------------------
CALL PI();
> 3.141592653589793
> -----------------
> 3.141592653589793
> rows: 1

CALL 1+1;
> 2
> -
> 2
> rows: 1

--- constraints ----------------------------------------------------------------------------------------------
CREATE TABLE PARENT(A INT, B INT, PRIMARY KEY(A, B));
> ok

CREATE TABLE CHILD(ID INT PRIMARY KEY, PA INT, PB INT, CONSTRAINT AB FOREIGN KEY(PA, PB) REFERENCES PARENT(A, B));
> ok

SELECT * FROM INFORMATION_SCHEMA.CROSS_REFERENCES;
> PKTABLE_CATALOG PKTABLE_SCHEMA PKTABLE_NAME PKCOLUMN_NAME FKTABLE_CATALOG FKTABLE_SCHEMA FKTABLE_NAME FKCOLUMN_NAME ORDINAL_POSITION UPDATE_RULE DELETE_RULE FK_NAME PK_NAME       DEFERRABILITY
> --------------- -------------- ------------ ------------- --------------- -------------- ------------ ------------- ---------------- ----------- ----------- ------- ------------- -------------
> SCRIPT          PUBLIC         PARENT       A             SCRIPT          PUBLIC         CHILD        PA            1                1           1           AB      PRIMARY_KEY_8 7
> SCRIPT          PUBLIC         PARENT       B             SCRIPT          PUBLIC         CHILD        PB            2                1           1           AB      PRIMARY_KEY_8 7
> rows: 2

DROP TABLE PARENT;
> ok

DROP TABLE CHILD;
> ok

drop table if exists test;
> ok

create table test(id int primary key, parent int, foreign key(id) references test(parent));
> ok

insert into test values(1, 1);
> update count: 1

delete from test;
> update count: 1

drop table test;
> ok

drop table if exists child;
> ok

drop table if exists parent;
> ok

create table child(a int, id int);
> ok

create table parent(id int primary key);
> ok

alter table child add foreign key(id) references parent;
> ok

insert into parent values(1);
> update count: 1

delete from parent;
> update count: 1

drop table if exists child;
> ok

drop table if exists parent;
> ok

CREATE MEMORY TABLE PARENT(ID INT PRIMARY KEY);
> ok

CREATE MEMORY TABLE CHILD(ID INT, PARENT_ID INT, FOREIGN KEY(PARENT_ID) REFERENCES PARENT);
> ok

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ------------------------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.CHILD;
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.PARENT;
> ALTER TABLE PUBLIC.CHILD ADD CONSTRAINT PUBLIC.CONSTRAINT_3 FOREIGN KEY(PARENT_ID) REFERENCES PUBLIC.PARENT(ID) NOCHECK;
> ALTER TABLE PUBLIC.PARENT ADD CONSTRAINT PUBLIC.CONSTRAINT_8 PRIMARY KEY(ID);
> CREATE MEMORY TABLE PUBLIC.CHILD( ID INT, PARENT_ID INT );
> CREATE MEMORY TABLE PUBLIC.PARENT( ID INT NOT NULL );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 7

DROP TABLE PARENT;
> ok

DROP TABLE CHILD;
> ok

CREATE TABLE TEST(ID INT, CONSTRAINT PK PRIMARY KEY(ID), NAME VARCHAR, PARENT INT, CONSTRAINT P FOREIGN KEY(PARENT) REFERENCES(ID));
> ok

ALTER TABLE TEST DROP PRIMARY KEY;
> exception

ALTER TABLE TEST DROP CONSTRAINT PK;
> ok

INSERT INTO TEST VALUES(1, 'Frank', 1);
> update count: 1

INSERT INTO TEST VALUES(2, 'Sue', 1);
> update count: 1

INSERT INTO TEST VALUES(3, 'Karin', 2);
> update count: 1

INSERT INTO TEST VALUES(4, 'Joe', 5);
> exception

INSERT INTO TEST VALUES(4, 'Joe', 3);
> update count: 1

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE TEST(A_INT INT NOT NULL, B_INT INT NOT NULL, PRIMARY KEY(A_INT, B_INT));
> ok

ALTER TABLE TEST ADD CONSTRAINT A_UNIQUE UNIQUE(A_INT);
> ok

ALTER TABLE TEST DROP PRIMARY KEY;
> ok

ALTER TABLE TEST DROP PRIMARY KEY;
> exception

ALTER TABLE TEST DROP CONSTRAINT A_UNIQUE;
> ok

ALTER TABLE TEST ADD CONSTRAINT C1 FOREIGN KEY(A_INT) REFERENCES TEST(B_INT);
> ok

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ----------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.C1 FOREIGN KEY(A_INT) REFERENCES PUBLIC.TEST(B_INT) NOCHECK;
> CREATE MEMORY TABLE PUBLIC.TEST( A_INT INT NOT NULL, B_INT INT NOT NULL );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 4

ALTER TABLE TEST DROP CONSTRAINT C1;
> ok

ALTER TABLE TEST DROP CONSTRAINT C1;
> exception

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE A_TEST(A_INT INT NOT NULL, A_VARCHAR VARCHAR(255) DEFAULT 'x', A_DATE DATE, A_DECIMAL DECIMAL(10,2));
> ok

ALTER TABLE A_TEST ADD PRIMARY KEY(A_INT);
> ok

ALTER TABLE A_TEST ADD CONSTRAINT MIN_LENGTH CHECK LENGTH(A_VARCHAR)>1;
> ok

ALTER TABLE A_TEST ADD CONSTRAINT DATE_UNIQUE UNIQUE(A_DATE);
> ok

ALTER TABLE A_TEST ADD CONSTRAINT DATE_UNIQUE_2 UNIQUE(A_DATE);
> ok

INSERT INTO A_TEST VALUES(NULL, NULL, NULL, NULL);
> exception

INSERT INTO A_TEST VALUES(1, 'A', NULL, NULL);
> exception

INSERT INTO A_TEST VALUES(1, 'AB', NULL, NULL);
> update count: 1

INSERT INTO A_TEST VALUES(1, 'AB', NULL, NULL);
> exception

INSERT INTO A_TEST VALUES(2, 'AB', NULL, NULL);
> update count: 1

INSERT INTO A_TEST VALUES(3, 'AB', '2004-01-01', NULL);
> update count: 1

INSERT INTO A_TEST VALUES(4, 'AB', '2004-01-01', NULL);
> exception

INSERT INTO A_TEST VALUES(5, 'ABC', '2004-01-02', NULL);
> update count: 1

CREATE MEMORY TABLE B_TEST(B_INT INT DEFAULT -1 NOT NULL , B_VARCHAR VARCHAR(255) DEFAULT NULL NULL, CONSTRAINT B_UNIQUE UNIQUE(B_INT));
> ok

ALTER TABLE B_TEST ADD CHECK LENGTH(B_VARCHAR)>1;
> ok

ALTER TABLE B_TEST ADD CONSTRAINT C1 FOREIGN KEY(B_INT) REFERENCES A_TEST(A_INT) ON DELETE CASCADE ON UPDATE CASCADE;
> ok

ALTER TABLE B_TEST ADD PRIMARY KEY(B_INT);
> ok

INSERT INTO B_TEST VALUES(10, 'X');
> exception

INSERT INTO B_TEST VALUES(1, 'X');
> exception

INSERT INTO B_TEST VALUES(1, 'XX');
> update count: 1

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> 1     XX
> rows: 1

UPDATE A_TEST SET A_INT = A_INT*10;
> update count: 4

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> 10    XX
> rows: 1

ALTER TABLE B_TEST DROP CONSTRAINT C1;
> ok

ALTER TABLE B_TEST ADD CONSTRAINT C2 FOREIGN KEY(B_INT) REFERENCES A_TEST(A_INT) ON DELETE SET NULL ON UPDATE SET NULL;
> ok

UPDATE A_TEST SET A_INT = A_INT*10;
> exception

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> 10    XX
> rows: 1

ALTER TABLE B_TEST DROP CONSTRAINT C2;
> ok

UPDATE B_TEST SET B_INT = 20;
> update count: 1

SELECT A_INT FROM A_TEST;
> A_INT
> -----
> 10
> 20
> 30
> 50
> rows: 4

ALTER TABLE B_TEST ADD CONSTRAINT C3 FOREIGN KEY(B_INT) REFERENCES A_TEST(A_INT) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
> ok

UPDATE A_TEST SET A_INT = A_INT*10;
> update count: 4

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> -1    XX
> rows: 1

DELETE FROM A_TEST;
> update count: 4

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> -1    XX
> rows: 1

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ----------------------------------------------------------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.A_TEST;
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.B_TEST;
> ALTER TABLE PUBLIC.A_TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_7 PRIMARY KEY(A_INT);
> ALTER TABLE PUBLIC.A_TEST ADD CONSTRAINT PUBLIC.DATE_UNIQUE UNIQUE(A_DATE);
> ALTER TABLE PUBLIC.A_TEST ADD CONSTRAINT PUBLIC.DATE_UNIQUE_2 UNIQUE(A_DATE);
> ALTER TABLE PUBLIC.A_TEST ADD CONSTRAINT PUBLIC.MIN_LENGTH CHECK(LENGTH(A_VARCHAR) > 1) NOCHECK;
> ALTER TABLE PUBLIC.B_TEST ADD CONSTRAINT PUBLIC.B_UNIQUE UNIQUE(B_INT);
> ALTER TABLE PUBLIC.B_TEST ADD CONSTRAINT PUBLIC.C3 FOREIGN KEY(B_INT) REFERENCES PUBLIC.A_TEST(A_INT) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT NOCHECK;
> ALTER TABLE PUBLIC.B_TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_76 CHECK(LENGTH(B_VARCHAR) > 1) NOCHECK;
> ALTER TABLE PUBLIC.B_TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_760 PRIMARY KEY(B_INT);
> CREATE MEMORY TABLE PUBLIC.A_TEST( A_INT INT NOT NULL, A_VARCHAR VARCHAR(255) DEFAULT 'x', A_DATE DATE, A_DECIMAL DECIMAL(10, 2) );
> CREATE MEMORY TABLE PUBLIC.B_TEST( B_INT INT DEFAULT -1 NOT NULL, B_VARCHAR VARCHAR(255) DEFAULT NULL );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.B_TEST(B_INT, B_VARCHAR) VALUES (-1, 'XX');
> rows: 14

DROP TABLE A_TEST;
> ok

DROP TABLE B_TEST;
> ok

CREATE MEMORY TABLE FAMILY(ID INT, NAME VARCHAR(20));
> ok

CREATE INDEX FAMILY_ID_NAME ON FAMILY(ID, NAME);
> ok

CREATE MEMORY TABLE PARENT(ID INT, FAMILY_ID INT, NAME VARCHAR(20));
> ok

ALTER TABLE PARENT ADD CONSTRAINT PARENT_FAMILY FOREIGN KEY(FAMILY_ID)
REFERENCES FAMILY(ID);
> ok

CREATE MEMORY TABLE CHILD(
ID INT,
PARENTID INT,
FAMILY_ID INT,
UNIQUE(ID, PARENTID),
CONSTRAINT PARENT_CHILD FOREIGN KEY(PARENTID, FAMILY_ID)
REFERENCES PARENT(ID, FAMILY_ID)
ON UPDATE CASCADE
ON DELETE SET NULL,
NAME VARCHAR(20));
> ok

INSERT INTO FAMILY VALUES(1, 'Capone');
> update count: 1

INSERT INTO CHILD VALUES(100, 1, 1, 'early');
> exception

INSERT INTO PARENT VALUES(1, 1, 'Sue');
> update count: 1

INSERT INTO PARENT VALUES(2, 1, 'Joe');
> update count: 1

INSERT INTO CHILD VALUES(100, 1, 1, 'Simon');
> update count: 1

INSERT INTO CHILD VALUES(101, 1, 1, 'Sabine');
> update count: 1

INSERT INTO CHILD VALUES(200, 2, 1, 'Jim');
> update count: 1

INSERT INTO CHILD VALUES(201, 2, 1, 'Johann');
> update count: 1

UPDATE PARENT SET ID=3 WHERE ID=1;
> update count: 1

SELECT * FROM CHILD;
> ID  PARENTID FAMILY_ID NAME
> --- -------- --------- ------
> 100 3        1         Simon
> 101 3        1         Sabine
> 200 2        1         Jim
> 201 2        1         Johann
> rows: 4

UPDATE CHILD SET PARENTID=-1 WHERE PARENTID IS NOT NULL;
> exception

DELETE FROM PARENT WHERE ID=2;
> update count: 1

SELECT * FROM CHILD;
> ID  PARENTID FAMILY_ID NAME
> --- -------- --------- ------
> 100 3        1         Simon
> 101 3        1         Sabine
> 200 null     null      Jim
> 201 null     null      Johann
> rows: 4

SCRIPT SIMPLE NOPASSWORDS NOSETTINGS;
> SCRIPT
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.FAMILY;
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.PARENT;
> -- 4 +/- SELECT COUNT(*) FROM PUBLIC.CHILD;
> ALTER TABLE PUBLIC.CHILD ADD CONSTRAINT PUBLIC.CONSTRAINT_3 UNIQUE(ID, PARENTID);
> ALTER TABLE PUBLIC.CHILD ADD CONSTRAINT PUBLIC.PARENT_CHILD FOREIGN KEY(PARENTID, FAMILY_ID) REFERENCES PUBLIC.PARENT(ID, FAMILY_ID) ON DELETE SET NULL ON UPDATE CASCADE NOCHECK;
> ALTER TABLE PUBLIC.PARENT ADD CONSTRAINT PUBLIC.PARENT_FAMILY FOREIGN KEY(FAMILY_ID) REFERENCES PUBLIC.FAMILY(ID) NOCHECK;
> CREATE INDEX PUBLIC.FAMILY_ID_NAME ON PUBLIC.FAMILY(ID, NAME);
> CREATE MEMORY TABLE PUBLIC.CHILD( ID INT, PARENTID INT, FAMILY_ID INT, NAME VARCHAR(20) );
> CREATE MEMORY TABLE PUBLIC.FAMILY( ID INT, NAME VARCHAR(20) );
> CREATE MEMORY TABLE PUBLIC.PARENT( ID INT, FAMILY_ID INT, NAME VARCHAR(20) );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.CHILD(ID, PARENTID, FAMILY_ID, NAME) VALUES(100, 3, 1, 'Simon');
> INSERT INTO PUBLIC.CHILD(ID, PARENTID, FAMILY_ID, NAME) VALUES(101, 3, 1, 'Sabine');
> INSERT INTO PUBLIC.CHILD(ID, PARENTID, FAMILY_ID, NAME) VALUES(200, NULL, NULL, 'Jim');
> INSERT INTO PUBLIC.CHILD(ID, PARENTID, FAMILY_ID, NAME) VALUES(201, NULL, NULL, 'Johann');
> INSERT INTO PUBLIC.FAMILY(ID, NAME) VALUES(1, 'Capone');
> INSERT INTO PUBLIC.PARENT(ID, FAMILY_ID, NAME) VALUES(3, 1, 'Sue');
> rows: 17

ALTER TABLE CHILD DROP CONSTRAINT PARENT_CHILD;
> ok

SCRIPT SIMPLE NOPASSWORDS NOSETTINGS;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.FAMILY;
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.PARENT;
> -- 4 +/- SELECT COUNT(*) FROM PUBLIC.CHILD;
> ALTER TABLE PUBLIC.CHILD ADD CONSTRAINT PUBLIC.CONSTRAINT_3 UNIQUE(ID, PARENTID);
> ALTER TABLE PUBLIC.PARENT ADD CONSTRAINT PUBLIC.PARENT_FAMILY FOREIGN KEY(FAMILY_ID) REFERENCES PUBLIC.FAMILY(ID) NOCHECK;
> CREATE INDEX PUBLIC.FAMILY_ID_NAME ON PUBLIC.FAMILY(ID, NAME);
> CREATE MEMORY TABLE PUBLIC.CHILD( ID INT, PARENTID INT, FAMILY_ID INT, NAME VARCHAR(20) );
> CREATE MEMORY TABLE PUBLIC.FAMILY( ID INT, NAME VARCHAR(20) );
> CREATE MEMORY TABLE PUBLIC.PARENT( ID INT, FAMILY_ID INT, NAME VARCHAR(20) );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.CHILD(ID, PARENTID, FAMILY_ID, NAME) VALUES(100, 3, 1, 'Simon');
> INSERT INTO PUBLIC.CHILD(ID, PARENTID, FAMILY_ID, NAME) VALUES(101, 3, 1, 'Sabine');
> INSERT INTO PUBLIC.CHILD(ID, PARENTID, FAMILY_ID, NAME) VALUES(200, NULL, NULL, 'Jim');
> INSERT INTO PUBLIC.CHILD(ID, PARENTID, FAMILY_ID, NAME) VALUES(201, NULL, NULL, 'Johann');
> INSERT INTO PUBLIC.FAMILY(ID, NAME) VALUES(1, 'Capone');
> INSERT INTO PUBLIC.PARENT(ID, FAMILY_ID, NAME) VALUES(3, 1, 'Sue');
> rows: 16

DELETE FROM PARENT;
> update count: 1

SELECT * FROM CHILD;
> ID  PARENTID FAMILY_ID NAME
> --- -------- --------- ------
> 100 3        1         Simon
> 101 3        1         Sabine
> 200 null     null      Jim
> 201 null     null      Johann
> rows: 4

DROP TABLE PARENT;
> ok

DROP TABLE CHILD;
> ok

DROP TABLE FAMILY;
> ok

CREATE TABLE INVOICE(CUSTOMER_ID INT, ID INT, TOTAL_AMOUNT DECIMAL(10,2), PRIMARY KEY(CUSTOMER_ID, ID));
> ok

CREATE TABLE INVOICE_LINE(CUSTOMER_ID INT, INVOICE_ID INT, LINE_ID INT, TEXT VARCHAR, AMOUNT DECIMAL(10,2));
> ok

CREATE INDEX ON INVOICE_LINE(CUSTOMER_ID);
> ok

ALTER TABLE INVOICE_LINE ADD FOREIGN KEY(CUSTOMER_ID, INVOICE_ID) REFERENCES INVOICE(CUSTOMER_ID, ID) ON DELETE CASCADE;
> ok

INSERT INTO INVOICE VALUES(1, 100, NULL), (1, 101, NULL);
> update count: 2

INSERT INTO INVOICE_LINE VALUES(1, 100, 10, 'Apples', 20.35), (1, 100, 20, 'Paper', 10.05), (1, 101, 10, 'Pencil', 1.10), (1, 101, 20, 'Chair', 540.40);
> update count: 4

INSERT INTO INVOICE_LINE VALUES(1, 102, 20, 'Nothing', 30.00);
> exception

DELETE FROM INVOICE WHERE ID = 100;
> update count: 1

SELECT * FROM INVOICE_LINE;
> CUSTOMER_ID INVOICE_ID LINE_ID TEXT   AMOUNT
> ----------- ---------- ------- ------ ------
> 1           101        10      Pencil 1.10
> 1           101        20      Chair  540.40
> rows: 2

DROP TABLE INVOICE;
> ok

DROP TABLE INVOICE_LINE;
> ok

CREATE MEMORY TABLE TEST(A INT, B INT, FOREIGN KEY (B) REFERENCES(A) ON UPDATE RESTRICT ON DELETE NO ACTION);
> ok

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ------------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 FOREIGN KEY(B) REFERENCES PUBLIC.TEST(A) NOCHECK;
> CREATE MEMORY TABLE PUBLIC.TEST( A INT, B INT );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 4

DROP TABLE TEST;
> ok

--- users ----------------------------------------------------------------------------------------------
CREATE USER TEST PASSWORD 'abc';
> ok

CREATE USER TEST_ADMIN_X PASSWORD 'def' ADMIN;
> ok

ALTER USER TEST_ADMIN_X RENAME TO TEST_ADMIN;
> ok

ALTER USER TEST_ADMIN ADMIN TRUE;
> ok

CREATE USER TEST2 PASSWORD '123' ADMIN;
> ok

ALTER USER TEST2 SET PASSWORD 'abc';
> ok

ALTER USER TEST2 ADMIN FALSE;
> ok

CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE MEMORY TABLE TEST2_X(ID INT);
> ok

CREATE INDEX IDX_ID ON TEST2_X(ID);
> ok

ALTER TABLE TEST2_X RENAME TO TEST2;
> ok

ALTER INDEX IDX_ID RENAME TO IDX_ID2;
> ok

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ---------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST2;
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CREATE INDEX PUBLIC.IDX_ID2 ON PUBLIC.TEST2(ID);
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, NAME VARCHAR(255) );
> CREATE MEMORY TABLE PUBLIC.TEST2( ID INT );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> CREATE USER IF NOT EXISTS TEST PASSWORD '';
> CREATE USER IF NOT EXISTS TEST2 PASSWORD '';
> CREATE USER IF NOT EXISTS TEST_ADMIN PASSWORD '' ADMIN;
> rows: 10

SELECT NAME, ADMIN FROM INFORMATION_SCHEMA.USERS;
> NAME       ADMIN
> ---------- -----
> SA         true
> TEST       false
> TEST2      false
> TEST_ADMIN true
> rows: 4

DROP TABLE TEST2;
> ok

DROP TABLE TEST;
> ok

DROP USER TEST;
> ok

DROP USER IF EXISTS TEST;
> ok

DROP USER IF EXISTS TEST2;
> ok

DROP USER TEST_ADMIN;
> ok

SET AUTOCOMMIT FALSE;
> ok

SET SALT '' HASH '';
> ok

CREATE USER SECURE SALT '001122' HASH '1122334455';
> ok

ALTER USER SECURE SET SALT '112233' HASH '2233445566';
> ok

SCRIPT NOSETTINGS;
> SCRIPT
> -----------------------------------------------------------------
> CREATE USER IF NOT EXISTS SA SALT '' HASH '' ADMIN;
> CREATE USER IF NOT EXISTS SECURE SALT '112233' HASH '2233445566';
> rows: 2

SET PASSWORD '123';
> ok

SET AUTOCOMMIT TRUE;
> ok

DROP USER SECURE;
> ok

--- sequence with manual value ------------------
drop table if exists test;
> ok

CREATE TABLE TEST(ID bigint generated by default as identity (start with 1), name varchar);
> ok

SET AUTOCOMMIT FALSE;
> ok

insert into test(name) values('Hello');
> update count: 1

insert into test(name) values('World');
> update count: 1

call identity();
> IDENTITY()
> ----------
> 2
> rows: 1

insert into test(id, name) values(1234567890123456, 'World');
> update count: 1

call identity();
> IDENTITY()
> ----------------
> 1234567890123456
> rows: 1

insert into test(name) values('World');
> update count: 1

call identity();
> IDENTITY()
> ----------------
> 1234567890123457
> rows: 1

select * from test order by id;
> ID               NAME
> ---------------- -----
> 1                Hello
> 2                World
> 1234567890123456 World
> 1234567890123457 World
> rows (ordered): 4

SET AUTOCOMMIT TRUE;
> ok

drop table if exists test;
> ok

CREATE TABLE TEST(ID bigint generated by default as identity (start with 1), name varchar);
> ok

SET AUTOCOMMIT FALSE;
> ok

insert into test(name) values('Hello');
> update count: 1

insert into test(name) values('World');
> update count: 1

call identity();
> IDENTITY()
> ----------
> 2
> rows: 1

insert into test(id, name) values(1234567890123456, 'World');
> update count: 1

call identity();
> IDENTITY()
> ----------------
> 1234567890123456
> rows: 1

insert into test(name) values('World');
> update count: 1

call identity();
> IDENTITY()
> ----------------
> 1234567890123457
> rows: 1

select * from test order by id;
> ID               NAME
> ---------------- -----
> 1                Hello
> 2                World
> 1234567890123456 World
> 1234567890123457 World
> rows (ordered): 4

SET AUTOCOMMIT TRUE;
> ok

drop table test;
> ok

--- test cases ---------------------------------------------------------------------------------------------
create memory table word(word_id integer, name varchar);
> ok

alter table word alter column word_id integer(10) auto_increment;
> ok

insert into word(name) values('Hello');
> update count: 1

alter table word alter column word_id restart with 30872;
> ok

insert into word(name) values('World');
> update count: 1

select * from word;
> WORD_ID NAME
> ------- -----
> 1       Hello
> 30872   World
> rows: 2

drop table word;
> ok

create table test(id int, name varchar);
> ok

insert into test values(5, 'b'), (5, 'b'), (20, 'a');
> update count: 3

drop table test;
> ok

select 0 from ((
    select 0 as f from dual u1 where null in (?, ?, ?, ?, ?)
) union all (
    select u2.f from (
        select 0 as f from (
            select 0 from dual u2f1f1 where now() = ?
        ) u2f1
    ) u2
)) where f = 12345;
{
11, 22, 33, 44, 55, null
> 0
> -
> rows: 0
};
> update count: 0

create table x(id int not null);
> ok

alter table if exists y add column a varchar;
> ok

alter table if exists x add column a varchar;
> ok

alter table if exists x add column a varchar;
> exception

alter table if exists y alter column a rename to b;
> ok

alter table if exists x alter column a rename to b;
> ok

alter table if exists x alter column a rename to b;
> exception

alter table if exists y alter column b set default 'a';
> ok

alter table if exists x alter column b set default 'a';
> ok

insert into x(id) values(1);
> update count: 1

select b from x;
> B
> -
> a
> rows: 1

delete from x;
> update count: 1

alter table if exists y alter column b drop default;
> ok

alter table if exists x alter column b drop default;
> ok

alter table if exists y alter column b set not null;
> ok

alter table if exists x alter column b set not null;
> ok

insert into x(id) values(1);
> exception

alter table if exists y alter column b drop not null;
> ok

alter table if exists x alter column b drop not null;
> ok

insert into x(id) values(1);
> update count: 1

select b from x;
> B
> ----
> null
> rows: 1

delete from x;
> update count: 1

alter table if exists y add constraint x_pk primary key (id);
> ok

alter table if exists x add constraint x_pk primary key (id);
> ok

alter table if exists x add constraint x_pk primary key (id);
> exception

insert into x(id) values(1);
> update count: 1

insert into x(id) values(1);
> exception

delete from x;
> update count: 1

alter table if exists y add constraint x_check check (b = 'a');
> ok

alter table if exists x add constraint x_check check (b = 'a');
> ok

alter table if exists x add constraint x_check check (b = 'a');
> exception

insert into x(id, b) values(1, 'b');
> exception

alter table if exists y rename constraint x_check to x_check1;
> ok

alter table if exists x rename constraint x_check to x_check1;
> ok

alter table if exists x rename constraint x_check to x_check1;
> exception

alter table if exists y drop constraint x_check1;
> ok

alter table if exists x drop constraint x_check1;
> ok

alter table if exists y rename to z;
> ok

alter table if exists x rename to z;
> ok

alter table if exists x rename to z;
> ok

insert into z(id, b) values(1, 'b');
> update count: 1

delete from z;
> update count: 1

alter table if exists y add constraint z_uk unique (b);
> ok

alter table if exists z add constraint z_uk unique (b);
> ok

alter table if exists z add constraint z_uk unique (b);
> exception

insert into z(id, b) values(1, 'b');
> update count: 1

insert into z(id, b) values(1, 'b');
> exception

delete from z;
> update count: 1

alter table if exists y drop column b;
> ok

alter table if exists z drop column b;
> ok

alter table if exists z drop column b;
> exception

alter table if exists y drop primary key;
> ok

alter table if exists z drop primary key;
> ok

alter table if exists z drop primary key;
> exception

create table x (id int not null primary key);
> ok

alter table if exists y add constraint z_fk foreign key (id) references x (id);
> ok

alter table if exists z add constraint z_fk foreign key (id) references x (id);
> ok

alter table if exists z add constraint z_fk foreign key (id) references x (id);
> exception

insert into z (id) values (1);
> exception

alter table if exists y drop foreign key z_fk;
> ok

alter table if exists z drop foreign key z_fk;
> ok

alter table if exists z drop foreign key z_fk;
> exception

insert into z (id) values (1);
> update count: 1

delete from z;
> update count: 1

drop table x;
> ok

drop table z;
> ok

create schema x;
> ok

alter schema if exists y rename to z;
> ok

alter schema if exists x rename to z;
> ok

alter schema if exists x rename to z;
> ok

create table z.z (id int);
> ok

drop schema z cascade;
> ok

----- Issue#493 -----
create table test (year int, action varchar(10));
> ok

insert into test values (2015, 'order'), (2016, 'order'), (2014, 'order');
> update count: 3

insert into test values (2014, 'execution'), (2015, 'execution'), (2016, 'execution');
> update count: 3

select * from test where year in (select distinct year from test order by year desc limit 1 offset 0);
> YEAR ACTION
> ---- ---------
> 2016 order
> 2016 execution

drop table test;
> ok

