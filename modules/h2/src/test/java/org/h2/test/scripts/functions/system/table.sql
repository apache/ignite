-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select * from table(a int=(1)), table(b int=(2));
> A B
> - -
> 1 2
> rows: 1

create table test as select * from table(id int=(1, 2, 3));
> ok

SELECT * FROM (SELECT * FROM TEST) ORDER BY id;
> ID
> --
> 1
> 2
> 3
> rows (ordered): 3

SELECT * FROM (SELECT * FROM TEST) x ORDER BY id;
> ID
> --
> 1
> 2
> 3
> rows (ordered): 3

drop table test;
> ok

explain select * from table(id int = (1, 2), name varchar=('Hello', 'World'));
> PLAN
> -----------------------------------------------------------------------------------------------------
> SELECT TABLE.ID, TABLE.NAME FROM TABLE(ID INT=(1, 2), NAME VARCHAR=('Hello', 'World')) /* function */
> rows: 1

select * from table(id int=(1, 2), name varchar=('Hello', 'World')) x order by id;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2
