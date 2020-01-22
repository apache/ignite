-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select sign(null) en, sign(10) e1, sign(0) e0, sign(-0.1) em1 from test;
> EN   E1 E0 EM1
> ---- -- -- ---
> null 1  0  -1
> rows: 1




