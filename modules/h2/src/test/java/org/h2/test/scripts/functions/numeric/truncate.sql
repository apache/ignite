-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select truncate(null, null) en, truncate(1.99, 0) e1, truncate(-10.9, 0) em10 from test;
> EN   E1  EM10
> ---- --- -----
> null 1.0 -10.0
> rows: 1

select trunc(null, null) en, trunc(1.99, 0) e1, trunc(-10.9, 0) em10 from test;
> EN   E1  EM10
> ---- --- -----
> null 1.0 -10.0
> rows: 1

select trunc(1.3);
>> 1.0
