-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE test (id INT NOT NULL, name VARCHAR);
> ok

select * from test where id = ARRAY [1, 2];
> exception COMPARING_ARRAY_TO_SCALAR

insert into test values (1, 't');
> update count: 1

select * from test where id = (1, 2);
> exception COLUMN_COUNT_DOES_NOT_MATCH

drop table test;
> ok

SELECT 1 + 2 NOT;
> exception SYNTAX_ERROR_2

SELECT 1 NOT > 2;
> exception SYNTAX_ERROR_2
