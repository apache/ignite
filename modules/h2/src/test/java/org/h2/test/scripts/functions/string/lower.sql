-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select lower(null) en, lower('Hello') hello, lower('ABC') abc from test;
> EN   HELLO ABC
> ---- ----- ---
> null hello abc
> rows: 1

select lcase(null) en, lcase('Hello') hello, lcase('ABC') abc from test;
> EN   HELLO ABC
> ---- ----- ---
> null hello abc
> rows: 1
