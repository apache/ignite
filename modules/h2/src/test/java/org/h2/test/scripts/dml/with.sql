-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
explain with recursive r(n) as (
    (select 1) union all (select n+1 from r where n < 3)
)
select n from r;
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> WITH RECURSIVE R(N) AS ( (SELECT 1 FROM SYSTEM_RANGE(1, 1) /* PUBLIC.RANGE_INDEX */) UNION ALL (SELECT (N + 1) FROM PUBLIC.R /* PUBLIC.R.tableScan */ WHERE N < 3) ) SELECT N FROM R R /* null */
> rows: 1

select sum(n) from (
    with recursive r(n) as (
        (select 1) union all (select n+1 from r where n < 3)
    )
    select n from r
);
> SUM(N)
> ------
> 6
> rows: 1

select sum(n) from (select 0) join (
    with recursive r(n) as (
        (select 1) union all (select n+1 from r where n < 3)
    )
    select n from r
) on 1=1;
> SUM(N)
> ------
> 6
> rows: 1

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