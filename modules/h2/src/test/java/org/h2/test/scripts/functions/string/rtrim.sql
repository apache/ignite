-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select rtrim(null) en, '>' || rtrim('a') || '<' ea, '>' || rtrim(' a ') || '<' es from test;
> EN   EA  ES
> ---- --- ----
> null >a< > a<
> rows: 1

select rtrim() from dual;
> exception INVALID_PARAMETER_COUNT_2
