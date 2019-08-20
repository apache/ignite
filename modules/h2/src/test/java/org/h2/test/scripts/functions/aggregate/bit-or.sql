-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

-- with filter condition

create table test(v bigint);
> ok

insert into test values (1), (2), (4), (8), (16), (32), (64), (128), (256), (512), (1024), (2048);
> update count: 12

select bit_or(v), bit_or(v) filter (where v >= 8) from test where v <= 512;
> BIT_OR(V) BIT_OR(V) FILTER (WHERE (V >= 8))
> --------- ---------------------------------
> 1023      1016
> rows: 1

create index test_idx on test(v);

select bit_or(v), bit_or(v) filter (where v >= 8) from test where v <= 512;
> BIT_OR(V) BIT_OR(V) FILTER (WHERE (V >= 8))
> --------- ---------------------------------
> 1023      1016
> rows: 1

drop table test;
> ok
