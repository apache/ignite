-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select round(null, null) en, round(10.49, 0) e10, round(10.05, 1) e101 from test;
> EN   E10  E101
> ---- ---- ----
> null 10.0 10.1
> rows: 1

select round(null) en, round(0.6, null) en2, round(1.05) e1, round(-1.51) em2 from test;
> EN   EN2  E1  EM2
> ---- ---- --- ----
> null null 1.0 -2.0
> rows: 1

select roundmagic(null) en, roundmagic(cast(3.11 as double) - 3.1) e001, roundmagic(3.11-3.1-0.01) e000, roundmagic(2000000000000) e20x from test;
> EN   E001 E000 E20X
> ---- ---- ---- ------
> null 0.01 0.0  2.0E12
> rows: 1



