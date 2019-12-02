-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v int);
> ok

insert into test values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12);
> update count: 12

select count(v), count(v) filter (where v >= 4) from test where v <= 10;
> COUNT(V) COUNT(V) FILTER (WHERE (V >= 4))
> -------- --------------------------------
> 10       7
> rows: 1

create index test_idx on test(v);

select count(v), count(v) filter (where v >= 4) from test where v <= 10;
> COUNT(V) COUNT(V) FILTER (WHERE (V >= 4))
> -------- --------------------------------
> 10       7
> rows: 1

select count(v), count(v) filter (where v >= 4) from test;
> COUNT(V) COUNT(V) FILTER (WHERE (V >= 4))
> -------- --------------------------------
> 12       9
> rows: 1

drop table test;
> ok
