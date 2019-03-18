-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

call select 1 from dual where regexp_like('x', 'x', '\');
> exception

select x from dual where REGEXP_LIKE('A', '[a-z]', 'i');
>> 1

select x from dual where REGEXP_LIKE('A', '[a-z]', 'c');
> X
> -
> rows: 0
