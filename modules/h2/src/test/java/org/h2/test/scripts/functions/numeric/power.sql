-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select power(null, null) en, power(2, 3) e8, power(16, 0.5) e4 from test;
> EN   E8  E4
> ---- --- ---
> null 8.0 4.0
> rows: 1




