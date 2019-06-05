-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v bigint);
> ok

insert into test values
    (0xfffffffffff0), (0xffffffffff0f), (0xfffffffff0ff), (0xffffffff0fff),
    (0xfffffff0ffff), (0xffffff0fffff), (0xfffff0ffffff), (0xffff0fffffff),
    (0xfff0ffffffff), (0xff0fffffffff), (0xf0ffffffffff), (0x0fffffffffff);
> update count: 12

select bit_and(v), bit_and(v) filter (where v <= 0xffffffff0fff) from test where v >= 0xff0fffffffff;
> BIT_AND(V)      BIT_AND(V) FILTER (WHERE (V <= 281474976649215))
> --------------- ------------------------------------------------
> 280375465082880 280375465086975
> rows: 1

create index test_idx on test(v);

select bit_and(v), bit_and(v) filter (where v <= 0xffffffff0fff) from test where v >= 0xff0fffffffff;
> BIT_AND(V)      BIT_AND(V) FILTER (WHERE (V <= 281474976649215))
> --------------- ------------------------------------------------
> 280375465082880 280375465086975
> rows: 1

drop table test;
> ok
