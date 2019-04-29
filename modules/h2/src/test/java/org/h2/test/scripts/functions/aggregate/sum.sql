-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v int);
> ok

insert into test values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12);
> update count: 12

select sum(v), sum(v) filter (where v >= 4) from test where v <= 10;
> SUM(V) SUM(V) FILTER (WHERE (V >= 4))
> ------ ------------------------------
> 55     49
> rows: 1

create index test_idx on test(v);
> ok

select sum(v), sum(v) filter (where v >= 4) from test where v <= 10;
> SUM(V) SUM(V) FILTER (WHERE (V >= 4))
> ------ ------------------------------
> 55     49
> rows: 1

insert into test values (1), (2), (8);
> update count: 3

select sum(v), sum(all v), sum(distinct v) from test;
> SUM(V) SUM(V) SUM(DISTINCT V)
> ------ ------ ---------------
> 89     89     78
> rows: 1

drop table test;
> ok

create table test(v interval day to second);
> ok

insert into test values ('0 1'), ('0 2'), ('0 2'), ('0 2'), ('-0 1'), ('-0 1');
> update count: 6

select sum(v) from test;
>> INTERVAL '0 05:00:00' DAY TO SECOND

drop table test;
> ok

SELECT X, COUNT(*), SUM(COUNT(*)) OVER() FROM VALUES (1), (1), (1), (1), (2), (2), (3) T(X) GROUP BY X;
> X COUNT(*) SUM(COUNT(*)) OVER ()
> - -------- ---------------------
> 1 4        7
> 2 2        7
> 3 1        7
> rows: 3

CREATE TABLE TEST(ID INT);
> ok

SELECT SUM(ID) FROM TEST;
>> null

SELECT SUM(ID) OVER () FROM TEST;
> SUM(ID) OVER ()
> ---------------
> rows: 0

DROP TABLE TEST;
> ok

SELECT
    ID,
    SUM(ID) OVER (ORDER BY ID) S,
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) S_U_C,
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) S_C_U,
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) S_U_U
    FROM (SELECT X ID FROM SYSTEM_RANGE(1, 8));
> ID S  S_U_C S_C_U S_U_U
> -- -- ----- ----- -----
> 1  1  1     36    36
> 2  3  3     35    36
> 3  6  6     33    36
> 4  10 10    30    36
> 5  15 15    26    36
> 6  21 21    21    36
> 7  28 28    15    36
> 8  36 36    8     36
> rows: 8

SELECT I, V, SUM(V) OVER W S, SUM(DISTINCT V) OVER W D FROM
    VALUES (1, 1), (2, 1), (3, 1), (4, 1), (5, 2), (6, 2), (7, 3) T(I, V)
    WINDOW W AS (ORDER BY I);
> I V S  D
> - - -- -
> 1 1 1  1
> 2 1 2  1
> 3 1 3  1
> 4 1 4  1
> 5 2 6  3
> 6 2 8  3
> 7 3 11 6
> rows: 7
