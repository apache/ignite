-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

-- Truncate least significant digits because implementations returns slightly
-- different results depending on Java version
select radians(null) vn, truncate(radians(1), 10) v1, truncate(radians(1.1), 10) v2,
    truncate(radians(-1.1), 10) v3, truncate(radians(1.9), 10) v4,
    truncate(radians(-1.9), 10) v5 from test;
> VN   V1           V2           V3            V4           V5
> ---- ------------ ------------ ------------- ------------ -------------
> null 0.0174532925 0.0191986217 -0.0191986217 0.0331612557 -0.0331612557
> rows: 1
