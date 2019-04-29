-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
select 1000L / 10;
>> 100

select * from (select x as y from dual order by y);
>> 1

select a.x from dual a, dual b order by x;
>> 1

select 1 from(select 2 from(select 1) a right join dual b) c;
>> 1

select 1.00 / 3 * 0.00;
>> 0.00000000000000000000000000000

select 1.00000 / 3 * 0.0000;
>> 0.0000000000000000000000000000000000

select 1.0000000 / 3 * 0.00000;
>> 0.0000000000000000000000000000000000000

select 1.0000000 / 3 * 0.000000;
>> 0E-38

create table test(id null);
> ok

drop table test;
> ok

select * from (select group_concat(distinct 1) from system_range(1, 3));
>> 1

select sum(mod(x, 2) = 1) from system_range(1, 10);
>> 5

create table a(x int);
> ok

create table b(x int);
> ok

select count(*) from (select b.x from a left join b);
>> 0

drop table a, b;
> ok

select count(distinct now()) c from system_range(1, 100), system_range(1, 1000);
>> 1

select {fn TIMESTAMPADD(SQL_TSI_DAY, 1, {ts '2011-10-20 20:30:40.001'})};
>> 2011-10-21 20:30:40.001

select {fn TIMESTAMPADD(SQL_TSI_SECOND, 1, cast('2011-10-20 20:30:40.001' as timestamp))};
>> 2011-10-20 20:30:41.001

select N'test';
>> test

select E'test\\test';
>> test\test

create table a(id int) as select null;
> ok

create table b(id int references a(id)) as select null;
> ok

delete from a;
> update count: 1

drop table a, b;
> ok

create table test(a int, b int) as select 2, 0;
> ok

create index idx on test(b, a);
> ok

select count(*) from test where a in(2, 10) and b in(0, null);
>> 1

drop table test;
> ok

create table test(a int, b int) as select 1, 0;
> ok

create index idx on test(b, a);
> ok

select count(*) from test where b in(null, 0) and a in(1, null);
>> 1

drop table test;
> ok

create cached temp table test(id identity) not persistent;
> ok

drop table test;
> ok

create table test(a int, b int, unique(a, b));
> ok

insert into test values(1,1), (1,2);
> update count: 2

select count(*) from test where a in(1,2) and b in(1,2);
>> 2

drop table test;
> ok

create table test(id int);
> ok

alter table test alter column id set default 'x';
> ok

select column_default from information_schema.columns c where c.table_name = 'TEST' and c.column_name = 'ID';
>> 'x'

alter table test alter column id set not null;
> ok

select is_nullable from information_schema.columns c where c.table_name = 'TEST' and c.column_name = 'ID';
>> NO

alter table test alter column id set data type varchar;
> ok

select type_name from information_schema.columns c where c.table_name = 'TEST' and c.column_name = 'ID';
>> VARCHAR

alter table test alter column id type int;
> ok

select type_name from information_schema.columns c where c.table_name = 'TEST' and c.column_name = 'ID';
>> INTEGER

alter table test alter column id drop default;
> ok

select column_default from information_schema.columns c where c.table_name = 'TEST' and c.column_name = 'ID';
>> null

alter table test alter column id drop not null;
> ok

select is_nullable from information_schema.columns c where c.table_name = 'TEST' and c.column_name = 'ID';
>> YES

drop table test;
> ok

select x from (select *, rownum as r from system_range(1, 3)) where r=2;
>> 2

create table test(name varchar(255)) as select 'Hello+World+';
> ok

select count(*) from test where name like 'Hello++World++' escape '+';
>> 1

select count(*) from test where name like '+H+e+l+l+o++World++' escape '+';
>> 1

select count(*) from test where name like 'Hello+World++' escape '+';
>> 0

select count(*) from test where name like 'Hello++World+' escape '+';
>> 0

drop table test;
> ok

select count(*) from system_range(1, 1);
>> 1

select count(*) from system_range(1, -1);
>> 0

select 1 from dual where '\' like '\' escape '';
>> 1

select left(timestamp '2001-02-03 08:20:31+04', 4);
>> 2001

create table t1$2(id int);
> ok

drop table t1$2;
> ok

create table test(id int primary key) as select x from system_range(1, 200);
> ok

delete from test;
> update count: 200

insert into test(id) values(1);
> update count: 1

select * from test order by id;
>> 1

drop table test;
> ok

create memory table test(id int) not persistent as select 1 from dual;
> ok

insert into test values(1);
> update count: 1

select count(1) from test;
>> 2

@reconnect

select count(1) from test;
>> 0

drop table test;
> ok

create table test(t clob) as select 1;
> ok

select distinct t from test;
>> 1

drop table test;
> ok

create table test(id int unique not null);
> ok

drop table test;
> ok

create table test(id int not null unique);
> ok

drop table test;
> ok

select count(*)from((select 1 from dual limit 1)union(select 2 from dual limit 1));
>> 2

select sum(cast(x as int)) from system_range(2147483547, 2147483637);
>> 195421006872

select sum(x) from system_range(9223372036854775707, 9223372036854775797);
>> 839326855353784593432

select sum(cast(100 as tinyint)) from system_range(1, 1000);
>> 100000

select sum(cast(100 as smallint)) from system_range(1, 1000);
>> 100000

select avg(cast(x as int)) from system_range(2147483547, 2147483637);
>> 2147483592

select avg(x) from system_range(9223372036854775707, 9223372036854775797);
>> 9223372036854775752

select avg(cast(100 as tinyint)) from system_range(1, 1000);
>> 100

select avg(cast(100 as smallint)) from system_range(1, 1000);
>> 100

select datediff(yyyy, now(), now());
>> 0

create table t(d date) as select '2008-11-01' union select '2008-11-02';
> ok

select 1 from t group by year(d) order by year(d);
>> 1

drop table t;
> ok

create table t(d int) as select 2001 union select 2002;
> ok

select 1 from t group by d/10 order by d/10;
>> 1

drop table t;
> ok

create schema test;
> ok

create sequence test.report_id_seq;
> ok

select nextval('"test".REPORT_ID_SEQ');
>> 1

select nextval('"test"."report_id_seq"');
>> 2

select nextval('test.report_id_seq');
>> 3

drop schema test cascade;
> ok

create table master(id int primary key);
> ok

create table detail(id int primary key, x bigint, foreign key(x) references master(id) on delete cascade);
> ok

alter table detail alter column x bigint;
> ok

insert into master values(0);
> update count: 1

insert into detail values(0,0);
> update count: 1

delete from master;
> update count: 1

drop table master, detail;
> ok

drop all objects;
> ok

create table test(id int, parent int references test(id) on delete cascade);
> ok

insert into test values(0, 0);
> update count: 1

alter table test rename to test2;
> ok

delete from test2;
> update count: 1

drop table test2;
> ok

SELECT X FROM dual GROUP BY X HAVING X=AVG(X);
>> 1

create view test_view(id,) as select * from dual;
> ok

drop view test_view;
> ok

create table test(id int,);
> ok

insert into test(id,) values(1,);
> update count: 1

merge into test(id,) key(id,) values(1,);
> update count: 1

drop table test;
> ok

SET MODE DB2;
> ok

SELECT * FROM SYSTEM_RANGE(1, 100) OFFSET 99 ROWS;
>> 100

SELECT * FROM SYSTEM_RANGE(1, 100) OFFSET 50 ROWS FETCH FIRST 1 ROW ONLY;
>> 51

SELECT * FROM SYSTEM_RANGE(1, 100) FETCH FIRST 1 ROWS ONLY;
>> 1

SELECT * FROM SYSTEM_RANGE(1, 100) FETCH FIRST ROW ONLY;
>> 1

SET MODE REGULAR;
> ok

create domain email as varchar comment 'e-mail';
> ok

create table test(e email);
> ok

select remarks from INFORMATION_SCHEMA.COLUMNS where table_name='TEST';
>> e-mail

drop table test;
> ok

drop domain email;
> ok

create table test$test(id int);
> ok

drop table test$test;
> ok

create table test$$test(id int);
> ok

drop table test$$test;
> ok

create table test (id varchar(36) as random_uuid() primary key);
> ok

insert into test() values();
> update count: 1

delete from test where id = select id from test;
> update count: 1

drop table test;
> ok

create table test (id varchar(36) as now() primary key);
> ok

insert into test() values();
> update count: 1

delete from test where id = select id from test;
> update count: 1

drop table test;
> ok

SELECT SOME(X>4) FROM SYSTEM_RANGE(1,6);
>> TRUE

SELECT EVERY(X>4) FROM SYSTEM_RANGE(1,6);
>> FALSE

SELECT BOOL_OR(X>4) FROM SYSTEM_RANGE(1,6);
>> TRUE

SELECT BOOL_AND(X>4) FROM SYSTEM_RANGE(1,6);
>> FALSE

SELECT BIT_OR(X) FROM SYSTEM_RANGE(1,6);
>> 7

SELECT BIT_AND(X) FROM SYSTEM_RANGE(1,6);
>> 0

SELECT BIT_AND(X) FROM SYSTEM_RANGE(1,1);
>> 1

CREATE TABLE TEST(ID IDENTITY);
> ok

ALTER TABLE TEST ALTER COLUMN ID RESTART WITH ?;
{
10
};
> update count: 0

INSERT INTO TEST VALUES(NULL);
> update count: 1

SELECT * FROM TEST;
>> 10

DROP TABLE TEST;
> ok

CREATE SEQUENCE TEST_SEQ;
> ok

ALTER SEQUENCE TEST_SEQ RESTART WITH ? INCREMENT BY ?;
{
20, 3
};
> update count: 0

SELECT NEXT VALUE FOR TEST_SEQ;
>> 20

SELECT NEXT VALUE FOR TEST_SEQ;
>> 23

DROP SEQUENCE TEST_SEQ;
> ok

create schema Contact;
> ok

CREATE TABLE Account (id BIGINT);
> ok

CREATE TABLE Person (id BIGINT, FOREIGN KEY (id) REFERENCES Account(id));
> ok

CREATE TABLE Contact.Contact (id BIGINT, FOREIGN KEY (id) REFERENCES public.Person(id));
> ok

drop schema contact cascade;
> ok

drop table account, person;
> ok

create schema Contact;
> ok

CREATE TABLE Account (id BIGINT primary key);
> ok

CREATE TABLE Person (id BIGINT primary key, FOREIGN KEY (id) REFERENCES Account);
> ok

CREATE TABLE Contact.Contact (id BIGINT primary key, FOREIGN KEY (id) REFERENCES public.Person);
> ok

drop schema contact cascade;
> ok

drop table account, person;
> ok

CREATE TABLE TEST(A int NOT NULL, B int NOT NULL, C int) ;
> ok

ALTER TABLE TEST ADD CONSTRAINT CON UNIQUE(A,B);
> ok

ALTER TABLE TEST DROP C;
> ok

ALTER TABLE TEST DROP CONSTRAINT CON;
> ok

ALTER TABLE TEST DROP B;
> ok

DROP TABLE TEST;
> ok

select count(d.*) from dual d group by d.x;
>> 1

create table test(id int);
> ok

select count(*) from (select * from ((select * from test) union (select * from test)) a) b where id = 0;
>> 0

select count(*) from (select * from ((select * from test) union select * from test) a) b where id = 0;
>> 0

select count(*) from (select * from (select * from test union select * from test) a) b where id = 0;
>> 0

select 1 from ((test d1 inner join test d2 on d1.id = d2.id) inner join test d3 on d1.id = d3.id) inner join test d4 on d4.id = d1.id;
> 1
> -
> rows: 0

drop table test;
> ok

select replace(lpad('string', 10), ' ', '*');
>> ****string

select count(*) from (select * from dual union select * from dual) where x = 0;
>> 0

select count(*) from (select * from (select * from dual union select * from dual)) where x = 0;
>> 0

select instr('abcisj','s', -1) from dual;
>> 5

CREATE TABLE TEST(ID INT);
> ok

INSERT INTO TEST VALUES(1), (2), (3);
> update count: 3

create index idx_desc on test(id desc);
> ok

select * from test where id between 0 and 1;
>> 1

select * from test where id between 3 and 4;
>> 3

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'HelloWorld'), (3, 'HelloWorldWorld');
> update count: 3

SELECT COUNT(*) FROM TEST WHERE NAME REGEXP 'World';
>> 2

SELECT NAME FROM TEST WHERE NAME REGEXP 'WorldW';
>> HelloWorldWorld

drop table test;
> ok

select * from (select x from (select x from dual)) where 1=x;
>> 1

CREATE VIEW TEST_VIEW AS SELECT X FROM (SELECT X FROM DUAL);
> ok

SELECT * FROM TEST_VIEW;
>> 1

SELECT * FROM TEST_VIEW;
>> 1

DROP VIEW TEST_VIEW;
> ok

SELECT X FROM (SELECT X, X AS "XY" FROM DUAL) WHERE X=1;
>> 1

SELECT X FROM (SELECT X, X AS "X Y" FROM DUAL) WHERE X=1;
>> 1

SELECT X FROM (SELECT X, X AS "X Y" FROM DUAL AS "D Z") WHERE X=1;
>> 1

select * from (select x from dual union select convert(x, int) from dual) where x=0;
> X
> -
> rows: 0

create table test(id int);
> ok

insert into script.public.test(id) values(1), (2);
> update count: 2

update test t set t.id=t.id+1;
> update count: 2

update public.test set public.test.id=1;
> update count: 2

select count(script.public.test.id) from script.public.test;
>> 2

update script.public.test set script.public.test.id=1;
> update count: 2

drop table script.public.test;
> ok

select year(timestamp '2007-07-26T18:44:26.109000+02:00');
>> 2007

create table test(id int primary key);
> ok

begin;
> ok

insert into test values(1);
> update count: 1

rollback;
> ok

insert into test values(2);
> update count: 1

rollback;
> ok

begin;
> ok

insert into test values(3);
> update count: 1

commit;
> ok

insert into test values(4);
> update count: 1

rollback;
> ok

select group_concat(id order by id) from test;
>> 2,3,4

drop table test;
> ok

create table test();
> ok

insert into test values();
> update count: 1

ALTER TABLE TEST ADD ID INTEGER;
> ok

select count(*) from test;
>> 1

drop table test;
> ok

select * from dual where 'a_z' like '%=_%' escape '=';
>> 1

create table test as select 1 from dual union all select 2 from dual;
> ok

drop table test;
> ok

create table test_table(column_a integer);
> ok

insert into test_table values(1);
> update count: 1

create view test_view AS SELECT * FROM (SELECT DISTINCT * FROM test_table) AS subquery;
> ok

select * FROM test_view;
>> 1

drop view test_view;
> ok

drop table test_table;
> ok

CREATE TABLE TEST(ID INT);
> ok

INSERT INTO TEST VALUES(1);
> update count: 1

CREATE VIEW TEST_VIEW AS SELECT COUNT(ID) X FROM TEST;
> ok

explain SELECT * FROM TEST_VIEW WHERE X>1;
>> SELECT "TEST_VIEW"."X" FROM "PUBLIC"."TEST_VIEW" /* SELECT COUNT(ID) AS X FROM PUBLIC.TEST /++ PUBLIC.TEST.tableScan ++/ HAVING COUNT("ID") >= ?1: X > 1 */ WHERE "X" > 1

DROP VIEW TEST_VIEW;
> ok

DROP TABLE TEST;
> ok

create table test1(id int);
> ok

insert into test1 values(1), (1), (2), (3);
> update count: 4

select sum(C0) from (select count(*) AS C0 from (select distinct * from test1) as temp);
>> 3

drop table test1;
> ok

create table test(id int primary key check id>1);
> ok

drop table test;
> ok

create table table1(f1 int not null primary key);
> ok

create table table2(f2 int not null references table1(f1) on delete cascade);
> ok

drop table table2;
> ok

drop table table1;
> ok

create table table1(f1 int not null primary key);
> ok

create table table2(f2 int not null primary key references table1(f1));
> ok

drop table table1;
> ok

drop table table2;
> ok

select case when 1=null then 1 else 2 end;
>> 2

select case (1) when 1 then 1 else 2 end;
>> 1

create table test(id int);
> ok

insert into test values(1);
> update count: 1

select distinct id from test a order by a.id;
>> 1

drop table test;
> ok

create table FOO (ID int, A number(18, 2));
> ok

insert into FOO (ID, A) values (1, 10.0), (2, 20.0);
> update count: 2

select SUM (CASE when ID=1 then 0 ELSE A END) col0 from Foo;
>> 20.00

drop table FOO;
> ok

select (SELECT true)+1 GROUP BY 1;
>> 2

create table FOO (ID int, A number(18, 2));
> ok

insert into FOO (ID, A) values (1, 10.0), (2, 20.0);
> update count: 2

select SUM (CASE when ID=1 then A ELSE 0 END) col0 from Foo;
>> 10.00

drop table FOO;
> ok

create table A ( ID integer,  a1 varchar(20) );
> ok

create table B ( ID integer,  AID integer,  b1 varchar(20));
> ok

create table C ( ID integer,  BId integer,  c1 varchar(20));
> ok

insert into A (ID, a1) values (1, 'a1');
> update count: 1

insert into A (ID, a1) values (2, 'a2');
> update count: 1

select count(*) from A left outer join (B  inner join C on C.BID=B.ID )  on B.AID=A.ID where A.id=1;
>> 1

select count(*) from A left outer join (B  left join C on C.BID=B.ID )  on B.AID=A.ID where A.id=1;
>> 1

select count(*) from A left outer join B on B.AID=A.ID inner join C on C.BID=B.ID where A.id=1;
>> 0

select count(*) from (A left outer join B on B.AID=A.ID) inner join C on C.BID=B.ID where A.id=1;
>> 0

drop table a, b, c;
> ok

create schema a;
> ok

create table a.test(id int);
> ok

insert into a.test values(1);
> update count: 1

create schema b;
> ok

create table b.test(id int);
> ok

insert into b.test values(2);
> update count: 1

select a.test.id + b.test.id from a.test, b.test;
>> 3

drop schema a cascade;
> ok

drop schema b cascade;
> ok

select date '+0011-01-01';
>> 0011-01-01

select date'-0010-01-01';
>> -10-01-01

create schema TEST_SCHEMA;
> ok

create table TEST_SCHEMA.test(id int);
> ok

create sequence TEST_SCHEMA.TEST_SEQ;
> ok

select TEST_SCHEMA.TEST_SEQ.CURRVAL;
>> 0

select TEST_SCHEMA.TEST_SEQ.nextval;
>> 1

drop schema TEST_SCHEMA cascade;
> ok

create table test(id int);
> ok

create trigger TEST_TRIGGER before insert on test call "org.h2.test.db.TestTriggersConstraints";
> ok

comment on trigger TEST_TRIGGER is 'just testing';
> ok

select remarks from information_schema.triggers where trigger_name = 'TEST_TRIGGER';
>> just testing

@reconnect

select remarks from information_schema.triggers where trigger_name = 'TEST_TRIGGER';
>> just testing

drop trigger TEST_TRIGGER;
> ok

@reconnect

create alias parse_long for "java.lang.Long.parseLong(java.lang.String)";
> ok

comment on alias parse_long is 'Parse a long with base';
> ok

select remarks from information_schema.function_aliases where alias_name = 'PARSE_LONG';
>> Parse a long with base

@reconnect

select remarks from information_schema.function_aliases where alias_name = 'PARSE_LONG';
>> Parse a long with base

drop alias parse_long;
> ok

@reconnect

create role hr;
> ok

comment on role hr is 'Human Resources';
> ok

select remarks from information_schema.roles where name = 'HR';
>> Human Resources

@reconnect

select remarks from information_schema.roles where name = 'HR';
>> Human Resources

create user abc password 'x';
> ok

grant hr to abc;
> ok

drop role hr;
> ok

@reconnect

drop user abc;
> ok

create domain email as varchar(100) check instr(value, '@') > 0;
> ok

comment on domain email is 'must contain @';
> ok

select remarks from information_schema.domains where domain_name = 'EMAIL';
>> must contain @

@reconnect

select remarks from information_schema.domains where domain_name = 'EMAIL';
>> must contain @

drop domain email;
> ok

@reconnect

create schema tests;
> ok

set schema tests;
> ok

create sequence walk;
> ok

comment on schema tests is 'Test Schema';
> ok

comment on sequence walk is 'Walker';
> ok

select remarks from information_schema.schemata where schema_name = 'TESTS';
>> Test Schema

select remarks from information_schema.sequences where sequence_name = 'WALK';
>> Walker

@reconnect

select remarks from information_schema.schemata where schema_name = 'TESTS';
>> Test Schema

select remarks from information_schema.sequences where sequence_name = 'WALK';
>> Walker

drop schema tests cascade;
> ok

@reconnect

create constant abc value 1;
> ok

comment on constant abc is 'One';
> ok

select remarks from information_schema.constants where constant_name = 'ABC';
>> One

@reconnect

select remarks from information_schema.constants where constant_name = 'ABC';
>> One

drop constant abc;
> ok

drop table test;
> ok

@reconnect

create table test(id int);
> ok

alter table test add constraint const1 unique(id);
> ok

create index IDX_ID on test(id);
> ok

comment on constraint const1 is 'unique id';
> ok

comment on index IDX_ID is 'id_index';
> ok

select remarks from information_schema.constraints where constraint_name = 'CONST1';
>> unique id

select remarks from information_schema.indexes where index_name = 'IDX_ID';
>> id_index

@reconnect

select remarks from information_schema.constraints where constraint_name = 'CONST1';
>> unique id

select remarks from information_schema.indexes where index_name = 'IDX_ID';
>> id_index

drop table test;
> ok

@reconnect

create user sales password '1';
> ok

comment on user sales is 'mr. money';
> ok

select remarks from information_schema.users where name = 'SALES';
>> mr. money

@reconnect

select remarks from information_schema.users where name = 'SALES';
>> mr. money

alter user sales rename to SALES_USER;
> ok

select remarks from information_schema.users where name = 'SALES_USER';
>> mr. money

@reconnect

select remarks from information_schema.users where name = 'SALES_USER';
>> mr. money

create table test(id int);
> ok

create linked table test_link('org.h2.Driver', 'jdbc:h2:mem:', 'sa', 'sa', 'DUAL');
> ok

comment on table test_link is '123';
> ok

select remarks from information_schema.tables where table_name = 'TEST_LINK';
>> 123

@reconnect

select remarks from information_schema.tables where table_name = 'TEST_LINK';
>> 123

comment on table test_link is 'xyz';
> ok

select remarks from information_schema.tables where table_name = 'TEST_LINK';
>> xyz

alter table test_link rename to test_l;
> ok

select remarks from information_schema.tables where table_name = 'TEST_L';
>> xyz

@reconnect

select remarks from information_schema.tables where table_name = 'TEST_L';
>> xyz

drop table test;
> ok

@reconnect

create table test(id int);
> ok

create view test_v as select * from test;
> ok

comment on table test_v is 'abc';
> ok

select remarks from information_schema.tables where table_name = 'TEST_V';
>> abc

@reconnect

select remarks from information_schema.tables where table_name = 'TEST_V';
>> abc

alter table test_v rename to TEST_VIEW;
> ok

select remarks from information_schema.tables where table_name = 'TEST_VIEW';
>> abc

@reconnect

select remarks from information_schema.tables where table_name = 'TEST_VIEW';
>> abc

drop table test cascade;
> ok

@reconnect

create table test(a int);
> ok

comment on table test is 'hi';
> ok

select remarks from information_schema.tables where table_name = 'TEST';
>> hi

alter table test add column b int;
> ok

select remarks from information_schema.tables where table_name = 'TEST';
>> hi

alter table test rename to test1;
> ok

select remarks from information_schema.tables where table_name = 'TEST1';
>> hi

@reconnect

select remarks from information_schema.tables where table_name = 'TEST1';
>> hi

comment on table test1 is 'ho';
> ok

@reconnect

select remarks from information_schema.tables where table_name = 'TEST1';
>> ho

drop table test1;
> ok

create table test(a int, b int);
> ok

comment on column test.b is 'test';
> ok

select remarks from information_schema.columns where table_name = 'TEST' and column_name = 'B';
>> test

@reconnect

select remarks from information_schema.columns where table_name = 'TEST' and column_name = 'B';
>> test

alter table test drop column b;
> ok

@reconnect

comment on column test.a is 'ho';
> ok

select remarks from information_schema.columns where table_name = 'TEST' and column_name = 'A';
>> ho

@reconnect

select remarks from information_schema.columns where table_name = 'TEST' and column_name = 'A';
>> ho

drop table test;
> ok

@reconnect

create table test(a int);
> ok

comment on column test.a is 'test';
> ok

alter table test rename to test2;
> ok

@reconnect

select remarks from information_schema.columns where table_name = 'TEST2';
>> test

@reconnect

select remarks from information_schema.columns where table_name = 'TEST2';
>> test

drop table test2;
> ok

@reconnect

create table test1 (a varchar(10));
> ok

create hash index x1 on test1(a);
> ok

insert into test1 values ('abcaaaa'),('abcbbbb'),('abccccc'),('abcdddd');
> update count: 4

insert into test1 values ('abcaaaa'),('abcbbbb'),('abccccc'),('abcdddd');
> update count: 4

insert into test1 values ('abcaaaa'),('abcbbbb'),('abccccc'),('abcdddd');
> update count: 4

insert into test1 values ('abcaaaa'),('abcbbbb'),('abccccc'),('abcdddd');
> update count: 4

select count(*) from test1 where a='abcaaaa';
>> 4

select count(*) from test1 where a='abcbbbb';
>> 4

@reconnect

select count(*) from test1 where a='abccccc';
>> 4

select count(*) from test1 where a='abcdddd';
>> 4

update test1 set a='abccccc' where a='abcdddd';
> update count: 4

select count(*) from test1 where a='abccccc';
>> 8

select count(*) from test1 where a='abcdddd';
>> 0

delete from test1 where a='abccccc';
> update count: 8

select count(*) from test1 where a='abccccc';
>> 0

truncate table test1;
> ok

insert into test1 values ('abcaaaa');
> update count: 1

insert into test1 values ('abcaaaa');
> update count: 1

delete from test1;
> update count: 2

drop table test1;
> ok

@reconnect

drop table if exists test;
> ok

create table if not exists test(col1 int primary key);
> ok

insert into test values(1);
> update count: 1

insert into test values(2);
> update count: 1

insert into test values(3);
> update count: 1

select count(*) from test;
>> 3

select max(col1) from test;
>> 3

update test set col1 = col1 + 1 order by col1 asc limit 100;
> update count: 3

select count(*) from test;
>> 3

select max(col1) from test;
>> 4

drop table if exists test;
> ok
