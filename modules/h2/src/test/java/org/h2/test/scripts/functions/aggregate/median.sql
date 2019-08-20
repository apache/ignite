-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- ASC
create table test(v tinyint);
> ok

create index test_idx on test(v asc);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

-- ASC NULLS FIRST
create table test(v tinyint);
> ok

create index test_idx on test(v asc nulls first);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

-- ASC NULLS LAST
create table test(v tinyint);
> ok

create index test_idx on test(v asc nulls last);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

-- DESC
create table test(v tinyint);
> ok

create index test_idx on test(v desc);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

-- DESC NULLS FIRST
create table test(v tinyint);
> ok

create index test_idx on test(v desc nulls first);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

-- DESC NULLS LAST
create table test(v tinyint);
> ok

create index test_idx on test(v desc nulls last);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

create table test(v tinyint);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

create table test(v smallint);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

create table test(v int);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

create table test(v bigint);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
>> 20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20

select median(distinct v) from test;
>> 15

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15

drop table test;
> ok

create table test(v real);
> ok

insert into test values (2), (2), (1);
> update count: 3

select median(v) from test;
>> 2.0

insert into test values (null);
> update count: 1

select median(v) from test;
>> 2.0

select median(distinct v) from test;
>> 1.5

insert into test values (1);
> update count: 1

select median(v) from test;
>> 1.5

drop table test;
> ok

create table test(v double);
> ok

insert into test values (2), (2), (1);
> update count: 3

select median(v) from test;
>> 2.0

insert into test values (null);
> update count: 1

select median(v) from test;
>> 2.0

select median(distinct v) from test;
>> 1.5

insert into test values (1);
> update count: 1

select median(v) from test;
>> 1.5

drop table test;
> ok

create table test(v numeric(1));
> ok

insert into test values (2), (2), (1);
> update count: 3

select median(v) from test;
>> 2

insert into test values (null);
> update count: 1

select median(v) from test;
>> 2

select median(distinct v) from test;
>> 1.5

insert into test values (1);
> update count: 1

select median(v) from test;
>> 1.5

drop table test;
> ok

create table test(v time);
> ok

insert into test values ('20:00:00'), ('20:00:00'), ('10:00:00');
> update count: 3

select median(v) from test;
>> 20:00:00

insert into test values (null);
> update count: 1

select median(v) from test;
>> 20:00:00

select median(distinct v) from test;
>> 15:00:00

insert into test values ('10:00:00');
> update count: 1

select median(v) from test;
>> 15:00:00

drop table test;
> ok

create table test(v date);
> ok

insert into test values ('2000-01-20'), ('2000-01-20'), ('2000-01-10');
> update count: 3

select median(v) from test;
>> 2000-01-20

insert into test values (null);
> update count: 1

select median(v) from test;
>> 2000-01-20

select median(distinct v) from test;
>> 2000-01-15

insert into test values ('2000-01-10');
> update count: 1

select median(v) from test;
>> 2000-01-15

drop table test;
> ok

create table test(v timestamp);
> ok

insert into test values ('2000-01-20 20:00:00'), ('2000-01-20 20:00:00'), ('2000-01-10 10:00:00');
> update count: 3

select median(v) from test;
>> 2000-01-20 20:00:00

insert into test values (null);
> update count: 1

select median(v) from test;
>> 2000-01-20 20:00:00

select median(distinct v) from test;
>> 2000-01-15 15:00:00

insert into test values ('2000-01-10 10:00:00');
> update count: 1

select median(v) from test;
>> 2000-01-15 15:00:00

delete from test;
> update count: 5

insert into test values ('2000-01-20 20:00:00'), ('2000-01-21 20:00:00');
> update count: 2

select median(v) from test;
>> 2000-01-21 08:00:00

drop table test;
> ok

create table test(v timestamp with time zone);
> ok

insert into test values ('2000-01-20 20:00:00+04'), ('2000-01-20 20:00:00+04'), ('2000-01-10 10:00:00+02');
> update count: 3

select median(v) from test;
>> 2000-01-20 20:00:00+04

insert into test values (null);
> update count: 1

select median(v) from test;
>> 2000-01-20 20:00:00+04

select median(distinct v) from test;
>> 2000-01-15 15:00:00+03

insert into test values ('2000-01-10 10:00:00+02');
> update count: 1

select median(v) from test;
>> 2000-01-15 15:00:00+03

delete from test;
> update count: 5

insert into test values ('2000-01-20 20:00:00+10:15'), ('2000-01-21 20:00:00-09');
> update count: 2

select median(v) from test;
>> 2000-01-21 08:00:30+00:37

drop table test;
> ok

-- with group by
create table test(name varchar, value int);
> ok

insert into test values ('Group 2A', 10), ('Group 2A', 10), ('Group 2A', 20),
    ('Group 1X', 40), ('Group 1X', 50), ('Group 3B', null);
> update count: 6

select name, median(value) from test group by name order by name;
> NAME     MEDIAN(VALUE)
> -------- -------------
> Group 1X 45
> Group 2A 10
> Group 3B null
> rows (ordered): 3

drop table test;
> ok

-- with filter
create table test(v int);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test where v <> 20;
>> 10

create index test_idx on test(v asc);
> ok

select median(v) from test where v <> 20;
>> 10

drop table test;
> ok

-- two-column index
create table test(v int, v2 int);
> ok

create index test_idx on test(v, v2);
> ok

insert into test values (20, 1), (10, 2), (20, 3);
> update count: 3

select median(v) from test;
>> 20

drop table test;
> ok

-- not null column
create table test (v int not null);
> ok

create index test_idx on test(v desc);
> ok

select median(v) from test;
>> null

insert into test values (10), (20);
> update count: 2

select median(v) from test;
>> 15

insert into test values (20), (10), (20);
> update count: 3

select median(v) from test;
>> 20

drop table test;
> ok

-- with filter condition

create table test(v int);
> ok

insert into test values (10), (20), (30), (40), (50), (60), (70), (80), (90), (100), (110), (120);
> update count: 12

select median(v), median(v) filter (where v >= 40) from test where v <= 100;
> MEDIAN(V) MEDIAN(V) FILTER (WHERE (V >= 40))
> --------- ----------------------------------
> 55        70
> rows: 1

create index test_idx on test(v);

select median(v), median(v) filter (where v >= 40) from test where v <= 100;
> MEDIAN(V) MEDIAN(V) FILTER (WHERE (V >= 40))
> --------- ----------------------------------
> 55        70
> rows: 1

select median(v), median(v) filter (where v >= 40) from test;
> MEDIAN(V) MEDIAN(V) FILTER (WHERE (V >= 40))
> --------- ----------------------------------
> 65        80
> rows: 1

drop table test;
> ok

-- with filter and group by

create table test(dept varchar, amount int);
> ok

insert into test values
    ('First', 10), ('First', 10), ('First', 20), ('First', 30), ('First', 30),
    ('Second', 5), ('Second', 4), ('Second', 20), ('Second', 22), ('Second', 300),
    ('Third', 3), ('Third', 100), ('Third', 150), ('Third', 170), ('Third', 400);

select dept, median(amount) from test group by dept order by dept;
> DEPT   MEDIAN(AMOUNT)
> ------ --------------
> First  20
> Second 20
> Third  150
> rows (ordered): 3

select dept, median(amount) filter (where amount >= 20) from test group by dept order by dept;
> DEPT   MEDIAN(AMOUNT) FILTER (WHERE (AMOUNT >= 20))
> ------ --------------------------------------------
> First  30
> Second 22
> Third  160
> rows (ordered): 3

select dept, median(amount) filter (where amount >= 20) from test
    where (amount < 200) group by dept order by dept;
> DEPT   MEDIAN(AMOUNT) FILTER (WHERE (AMOUNT >= 20))
> ------ --------------------------------------------
> First  30
> Second 21
> Third  150
> rows (ordered): 3

drop table test;
> ok
