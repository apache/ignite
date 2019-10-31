-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v int);
> ok

insert into test values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12);
> update count: 12

select min(v), min(v) filter (where v >= 4) from test where v >= 2;
> MIN(V) MIN(V) FILTER (WHERE (V >= 4))
> ------ ------------------------------
> 2      4
> rows: 1

create index test_idx on test(v);

select min(v), min(v) filter (where v >= 4) from test where v >= 2;
> MIN(V) MIN(V) FILTER (WHERE (V >= 4))
> ------ ------------------------------
> 2      4
> rows: 1

select min(v), min(v) filter (where v >= 4) from test;
> MIN(V) MIN(V) FILTER (WHERE (V >= 4))
> ------ ------------------------------
> 1      4
> rows: 1

drop table test;
> ok
