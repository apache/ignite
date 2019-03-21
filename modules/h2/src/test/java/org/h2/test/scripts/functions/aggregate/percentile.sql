-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
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

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

insert into test values (null);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

select median(distinct v) from test;
>> 15.0

insert into test values (10);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- ----
> 10   20   15.0
> rows: 1

drop table test;
> ok

-- ASC NULLS FIRST
create table test(v tinyint);
> ok

create index test_idx on test(v asc nulls first);
> ok

insert into test values (20), (20), (10);
> update count: 3

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

insert into test values (null);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

select median(distinct v) from test;
>> 15.0

insert into test values (10);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- ----
> 10   20   15.0
> rows: 1

drop table test;
> ok

-- ASC NULLS LAST
create table test(v tinyint);
> ok

create index test_idx on test(v asc nulls last);
> ok

insert into test values (20), (20), (10);
> update count: 3

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

insert into test values (null);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

select median(distinct v) from test;
>> 15.0

insert into test values (10);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- ----
> 10   20   15.0
> rows: 1

drop table test;
> ok

-- DESC
create table test(v tinyint);
> ok

create index test_idx on test(v desc);
> ok

insert into test values (20), (20), (10);
> update count: 3

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

insert into test values (null);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

select median(distinct v) from test;
>> 15.0

insert into test values (10);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- ----
> 10   20   15.0
> rows: 1

drop table test;
> ok

-- DESC NULLS FIRST
create table test(v tinyint);
> ok

create index test_idx on test(v desc nulls first);
> ok

insert into test values (20), (20), (10);
> update count: 3

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

insert into test values (null);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

select median(distinct v) from test;
>> 15.0

insert into test values (10);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- ----
> 10   20   15.0
> rows: 1

drop table test;
> ok

-- DESC NULLS LAST
create table test(v tinyint);
> ok

create index test_idx on test(v desc nulls last);
> ok

insert into test values (20), (20), (10);
> update count: 3

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

insert into test values (null);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- --
> 20   20   20
> rows: 1

select median(distinct v) from test;
>> 15.0

insert into test values (10);
> update count: 1

select
    percentile_disc(0.5) within group (order by v) d50a,
    percentile_disc(0.5) within group (order by v desc) d50d,
    median(v) m from test;
> D50A D50D M
> ---- ---- ----
> 10   20   15.0
> rows: 1

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
>> 15.0

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15.0

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
>> 15.0

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15.0

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
>> 15.0

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15.0

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
>> 15.0

insert into test values (10);
> update count: 1

select median(v) from test;
>> 15.0

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
>> 1.50

insert into test values (1);
> update count: 1

select median(v) from test;
>> 1.50

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
>> 1.50

insert into test values (1);
> update count: 1

select median(v) from test;
>> 1.50

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

insert into test values ('-2000-01-10 10:00:00'), ('-2000-01-10 10:00:01');
> update count: 2

select percentile_cont(0.16) within group (order by v) from test;
>> -2000-01-10 10:00:00.48

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

delete from test;
> update count: 2

insert into test values ('-2000-01-20 20:00:00+10:15'), ('-2000-01-21 20:00:00-09');
> update count: 2

select median(v) from test;
>> -2000-01-21 08:00:30+00:37

drop table test;
> ok

create table test(v interval day to second);
> ok

insert into test values ('0 1'), ('0 2'), ('0 2'), ('0 2'), ('-0 1'), ('-0 1');
> update count: 6

select median (v) from test;
>> INTERVAL '0 01:30:00' DAY TO SECOND

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
> Group 1X 45.0
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
>> 15.0

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
> 55.0      70
> rows: 1

create index test_idx on test(v);
> ok

select median(v), median(v) filter (where v >= 40) from test where v <= 100;
> MEDIAN(V) MEDIAN(V) FILTER (WHERE (V >= 40))
> --------- ----------------------------------
> 55.0      70
> rows: 1

select median(v), median(v) filter (where v >= 40) from test;
> MEDIAN(V) MEDIAN(V) FILTER (WHERE (V >= 40))
> --------- ----------------------------------
> 65.0      80
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
> update count: 15

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
> Third  160.0
> rows (ordered): 3

select dept, median(amount) filter (where amount >= 20) from test
    where (amount < 200) group by dept order by dept;
> DEPT   MEDIAN(AMOUNT) FILTER (WHERE (AMOUNT >= 20))
> ------ --------------------------------------------
> First  30
> Second 21.0
> Third  150
> rows (ordered): 3

drop table test;
> ok

create table test(g int, v int);
> ok

insert into test values (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), (1, 9), (1, 10),
    (2, 10), (2, 20), (2, 30), (2, 100);
> update count: 14

select
    percentile_cont(0.05) within group (order by v) c05a,
    percentile_cont(0.05) within group (order by v desc) c05d,
    percentile_cont(0.5) within group (order by v) c50,
    percentile_cont(0.5) within group (order by v desc) c50d,
    percentile_cont(0.95) within group (order by v) c95a,
    percentile_cont(0.95) within group (order by v desc) c95d,
    g from test group by g;
> C05A  C05D  C50  C50D C95A  C95D  G
> ----- ----- ---- ---- ----- ----- -
> 1.45  9.55  5.5  5.5  9.55  1.45  1
> 11.50 89.50 25.0 25.0 89.50 11.50 2
> rows: 2

select
    percentile_disc(0.05) within group (order by v) d05a,
    percentile_disc(0.05) within group (order by v desc) d05d,
    percentile_disc(0.5) within group (order by v) d50,
    percentile_disc(0.5) within group (order by v desc) d50d,
    percentile_disc(0.95) within group (order by v) d95a,
    percentile_disc(0.95) within group (order by v desc) d95d,
    g from test group by g;
> D05A D05D D50 D50D D95A D95D G
> ---- ---- --- ---- ---- ---- -
> 1    10   5   6    10   1    1
> 10   100  20  30   100  10   2
> rows: 2

select
    percentile_disc(0.05) within group (order by v) over (partition by g order by v) d05a,
    percentile_disc(0.05) within group (order by v desc) over (partition by g order by v) d05d,
    percentile_disc(0.5) within group (order by v) over (partition by g order by v) d50,
    percentile_disc(0.5) within group (order by v desc) over (partition by g order by v) d50d,
    percentile_disc(0.95) within group (order by v) over (partition by g order by v) d95a,
    percentile_disc(0.95) within group (order by v desc) over (partition by g order by v) d95d,
    g, v from test order by g, v;
> D05A D05D D50 D50D D95A D95D G V
> ---- ---- --- ---- ---- ---- - ---
> 1    1    1   1    1    1    1 1
> 1    2    1   2    2    1    1 2
> 1    3    2   2    3    1    1 3
> 1    4    2   3    4    1    1 4
> 1    5    3   3    5    1    1 5
> 1    6    3   4    6    1    1 6
> 1    7    4   4    7    1    1 7
> 1    8    4   5    8    1    1 8
> 1    9    5   5    9    1    1 9
> 1    10   5   6    10   1    1 10
> 10   10   10  10   10   10   2 10
> 10   20   10  20   20   10   2 20
> 10   30   20  20   30   10   2 30
> 10   100  20  30   100  10   2 100
> rows (ordered): 14

delete from test where g <> 1;
> update count: 4

create index test_idx on test(v);
> ok

select
    percentile_disc(0.05) within group (order by v) d05a,
    percentile_disc(0.05) within group (order by v desc) d05d,
    percentile_disc(0.5) within group (order by v) d50,
    percentile_disc(0.5) within group (order by v desc) d50d,
    percentile_disc(0.95) within group (order by v) d95a,
    percentile_disc(0.95) within group (order by v desc) d95d
    from test;
> D05A D05D D50 D50D D95A D95D
> ---- ---- --- ---- ---- ----
> 1    10   5   6    10   1
> rows: 1

SELECT percentile_disc(null) within group (order by v) from test;
>> null

SELECT percentile_disc(-0.01) within group (order by v) from test;
> exception INVALID_VALUE_2

SELECT percentile_disc(1.01) within group (order by v) from test;
> exception INVALID_VALUE_2

SELECT percentile_disc(v) within group (order by v) from test;
> exception INVALID_VALUE_2

drop index test_idx;
> ok

SELECT percentile_disc(null) within group (order by v) from test;
>> null

SELECT percentile_disc(-0.01) within group (order by v) from test;
> exception INVALID_VALUE_2

SELECT percentile_disc(1.01) within group (order by v) from test;
> exception INVALID_VALUE_2

SELECT percentile_disc(v) within group (order by v) from test;
> exception INVALID_VALUE_2

drop table test;
> ok
