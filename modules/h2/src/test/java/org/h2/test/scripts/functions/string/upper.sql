-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select ucase(null) en, ucase('Hello') hello, ucase('ABC') abc from test;
> EN   HELLO ABC
> ---- ----- ---
> null HELLO ABC
> rows: 1

select upper(null) en, upper('Hello') hello, upper('ABC') abc from test;
> EN   HELLO ABC
> ---- ----- ---
> null HELLO ABC
> rows: 1
