-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select * from table(a int=(1)), table(b int=2), table(c int=row(3));
> A B C
> - - -
> 1 2 3
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

call table(id int = (1));
> ID
> --
> 1
> rows: 1

explain select * from table(id int = (1, 2), name varchar=('Hello', 'World'));
>> SELECT "TABLE"."ID", "TABLE"."NAME" FROM TABLE("ID" INT=ROW (1, 2), "NAME" VARCHAR=ROW ('Hello', 'World')) /* function */

explain select * from table(id int = ARRAY[1, 2], name varchar=ARRAY['Hello', 'World']);
>> SELECT "TABLE"."ID", "TABLE"."NAME" FROM TABLE("ID" INT=ARRAY [1, 2], "NAME" VARCHAR=ARRAY ['Hello', 'World']) /* function */

select * from table(id int=(1, 2), name varchar=('Hello', 'World')) x order by id;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

SELECT * FROM (TABLE(ID INT = (1, 2)));
> ID
> --
> 1
> 2
> rows: 2
