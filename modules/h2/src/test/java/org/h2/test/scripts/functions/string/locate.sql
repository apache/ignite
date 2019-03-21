-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select locate(null, null) en, locate(null, null, null) en1 from test;
> EN   EN1
> ---- ----
> null null
> rows: 1

select locate('World', 'Hello World') e7, locate('hi', 'abchihihi', 2) e3 from test;
> E7 E3
> -- --
> 7  4
> rows: 1

SELECT CHARINDEX('test', 'test');
> exception FUNCTION_NOT_FOUND_1

SET MODE MSSQLServer;
> ok

select charindex('World', 'Hello World') e7, charindex('hi', 'abchihihi', 2) e3 from test;
> E7 E3
> -- --
> 7  4
> rows: 1

SET MODE Regular;
> ok
