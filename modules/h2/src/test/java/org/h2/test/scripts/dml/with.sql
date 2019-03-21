-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table folder(id int primary key, name varchar(255), parent int);
> ok

insert into folder values(1, null, null), (2, 'bin', 1), (3, 'docs', 1), (4, 'html', 3), (5, 'javadoc', 3), (6, 'ext', 1), (7, 'service', 1), (8, 'src', 1), (9, 'docsrc', 8), (10, 'installer', 8), (11, 'main', 8), (12, 'META-INF', 11), (13, 'org', 11), (14, 'h2', 13), (15, 'test', 8), (16, 'tools', 8);
> update count: 16

with link(id, name, level) as (select id, name, 0 from folder where parent is null union all select folder.id, ifnull(link.name || '/', '') || folder.name, level + 1 from link inner join folder on link.id = folder.parent) select name from link where name is not null order by cast(id as int);
> NAME
> -----------------
> bin
> docs
> docs/html
> docs/javadoc
> ext
> service
> src
> src/docsrc
> src/installer
> src/main
> src/main/META-INF
> src/main/org
> src/main/org/h2
> src/test
> src/tools
> rows (ordered): 15

drop table folder;
> ok

explain with recursive r(n) as (
    (select 1) union all (select n+1 from r where n < 3)
)
select n from r;
>> WITH RECURSIVE "PUBLIC"."R"("N") AS ( (SELECT 1 FROM SYSTEM_RANGE(1, 1) /* PUBLIC.RANGE_INDEX */) UNION ALL (SELECT ("N" + 1) FROM "PUBLIC"."R" /* PUBLIC.R.tableScan */ WHERE "N" < 3) ) SELECT "N" FROM "PUBLIC"."R" "R" /* null */

explain with recursive "r"(n) as (
    (select 1) union all (select n+1 from "r" where n < 3)
)
select n from "r";
>> WITH RECURSIVE "PUBLIC"."r"("N") AS ( (SELECT 1 FROM SYSTEM_RANGE(1, 1) /* PUBLIC.RANGE_INDEX */) UNION ALL (SELECT ("N" + 1) FROM "PUBLIC"."r" /* PUBLIC.r.tableScan */ WHERE "N" < 3) ) SELECT "N" FROM "PUBLIC"."r" "r" /* null */

select sum(n) from (
    with recursive r(n) as (
        (select 1) union all (select n+1 from r where n < 3)
    )
    select n from r
);
>> 6

select sum(n) from (
    with recursive "r"(n) as (
        (select 1) union all (select n+1 from "r" where n < 3)
    )
    select n from "r"
);
>> 6

select sum(n) from (select 0) join (
    with recursive r(n) as (
        (select 1) union all (select n+1 from r where n < 3)
    )
    select n from r
) on 1=1;
>> 6

select 0 from (
    select 0 where 0 in (
        with recursive r(n) as (
            (select 1) union all (select n+1 from r where n < 3)
        )
        select n from r
    )
);
> 0
> -
> rows: 0

with
    r0(n,k) as (select -1, 0),
    r1(n,k) as ((select 1, 0) union all (select n+1,k+1 from r1 where n <= 3)),
    r2(n,k) as ((select 10,0) union all (select n+1,k+1 from r2 where n <= 13))
    select r1.k, r0.n as N0, r1.n AS N1, r2.n AS n2 from r0 inner join r1 ON r1.k= r0.k inner join r2 ON r1.k= r2.k;
> K N0 N1 N2
> - -- -- --
> 0 -1 1  10
> rows: 1

CREATE SCHEMA SCH;
> ok

CREATE FORCE VIEW TABLE_EXPRESSION SCH.R1(N) AS
(SELECT 1)
UNION ALL
(SELECT (N + 1) FROM SCH.R1 WHERE N < 3);
> ok

CREATE VIEW SCH.R2(N) AS
(SELECT 1)
UNION ALL
(SELECT (N + 1) FROM SCH.R1 WHERE N < 3);
> ok

SELECT * FROM SCH.R2;
> N
> -
> 1
> 2
> 3
> rows: 3

WITH CTE_TEST AS (SELECT 1, 2) SELECT * FROM CTE_TEST;
> 1 2
> - -
> 1 2
> rows: 1

WITH CTE_TEST AS (SELECT 1, 2) (SELECT * FROM CTE_TEST);
> 1 2
> - -
> 1 2
> rows: 1

WITH CTE_TEST AS (SELECT 1, 2) ((SELECT * FROM CTE_TEST));
> 1 2
> - -
> 1 2
> rows: 1

CREATE TABLE TEST(A INT, B INT) AS SELECT 1, 2;
> ok

WITH CTE_TEST AS (TABLE TEST) ((SELECT * FROM CTE_TEST));
> A B
> - -
> 1 2
> rows: 1

WITH CTE_TEST AS (TABLE TEST) ((TABLE CTE_TEST));
> A B
> - -
> 1 2
> rows: 1

WITH CTE_TEST AS (VALUES (1, 2)) ((SELECT * FROM CTE_TEST));
> C1 C2
> -- --
> 1  2
> rows: 1

WITH CTE_TEST AS (TABLE TEST) ((SELECT A, B FROM CTE_TEST2));
> exception TABLE_OR_VIEW_NOT_FOUND_1

WITH CTE_TEST AS (TABLE TEST) ((SELECT A, B, C FROM CTE_TEST));
> exception COLUMN_NOT_FOUND_1

DROP TABLE TEST;
> ok
