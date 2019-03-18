-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select sqrt(null) vn, sqrt(0) e0, sqrt(1) e1, sqrt(4) e2, sqrt(100) e10, sqrt(0.25) e05 from test;
> VN   E0  E1  E2  E10  E05
> ---- --- --- --- ---- ---
> null 0.0 1.0 2.0 10.0 0.5
> rows: 1



