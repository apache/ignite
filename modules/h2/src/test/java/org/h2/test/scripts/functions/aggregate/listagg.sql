-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v varchar);
> ok

insert into test values ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7'), ('8'), ('9');
> update count: 9

select listagg(v, '-') within group (order by v asc),
    listagg(v, '-') within group (order by v desc) filter (where v >= '4')
    from test where v >= '2';
> LISTAGG(V, '-') WITHIN GROUP (ORDER BY V) LISTAGG(V, '-') WITHIN GROUP (ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> ----------------------------------------- ------------------------------------------------------------------------
> 2-3-4-5-6-7-8-9                           9-8-7-6-5-4
> rows: 1

select group_concat(v order by v asc separator '-'),
    group_concat(v order by v desc separator '-') filter (where v >= '4')
    from test where v >= '2';
> LISTAGG(V, '-') WITHIN GROUP (ORDER BY V) LISTAGG(V, '-') WITHIN GROUP (ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> ----------------------------------------- ------------------------------------------------------------------------
> 2-3-4-5-6-7-8-9                           9-8-7-6-5-4
> rows: 1

create index test_idx on test(v);
> ok

select group_concat(v order by v asc separator '-'),
    group_concat(v order by v desc separator '-') filter (where v >= '4')
    from test where v >= '2';
> LISTAGG(V, '-') WITHIN GROUP (ORDER BY V) LISTAGG(V, '-') WITHIN GROUP (ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> ----------------------------------------- ------------------------------------------------------------------------
> 2-3-4-5-6-7-8-9                           9-8-7-6-5-4
> rows: 1

select group_concat(v order by v asc separator '-'),
    group_concat(v order by v desc separator '-') filter (where v >= '4')
    from test;
> LISTAGG(V, '-') WITHIN GROUP (ORDER BY V) LISTAGG(V, '-') WITHIN GROUP (ORDER BY V DESC) FILTER (WHERE (V >= '4'))
> ----------------------------------------- ------------------------------------------------------------------------
> 1-2-3-4-5-6-7-8-9                         9-8-7-6-5-4
> rows: 1

drop table test;
> ok

create table test (id int auto_increment primary key, v int);
> ok

insert into test(v) values (7), (2), (8), (3), (7), (3), (9), (-1);
> update count: 8

select group_concat(v) from test;
> LISTAGG(V)
> ----------------
> 7,2,8,3,7,3,9,-1
> rows: 1

select group_concat(distinct v) from test;
> LISTAGG(DISTINCT V)
> -------------------
> -1,2,3,7,8,9
> rows: 1

select group_concat(distinct v order by v desc) from test;
> LISTAGG(DISTINCT V) WITHIN GROUP (ORDER BY V DESC)
> --------------------------------------------------
> 9,8,7,3,2,-1
> rows: 1

drop table test;
> ok

create table test(g varchar, v int) as values ('-', 1), ('-', 2), ('-', 3), ('|', 4), ('|', 5), ('|', 6), ('*', null);
> ok

select g, listagg(v, g) from test group by g;
> G LISTAGG(V, G)
> - -------------
> * null
> - 1-2-3
> | 4|5|6
> rows: 3

select g, listagg(v, g) over (partition by g) from test order by v;
> G LISTAGG(V, G) OVER (PARTITION BY G)
> - -----------------------------------
> * null
> - 1-2-3
> - 1-2-3
> - 1-2-3
> | 4|5|6
> | 4|5|6
> | 4|5|6
> rows (ordered): 7

select g, listagg(v, g on overflow error) within group (order by v) filter (where v <> 2) over (partition by g) from test order by v;
> G LISTAGG(V, G) WITHIN GROUP (ORDER BY V) FILTER (WHERE (V <> 2)) OVER (PARTITION BY G)
> - -------------------------------------------------------------------------------------
> * null
> - 1-3
> - 1-3
> - 1-3
> | 4|5|6
> | 4|5|6
> | 4|5|6
> rows (ordered): 7

select listagg(distinct v, '-') from test;
> LISTAGG(DISTINCT V, '-')
> ------------------------
> 1-2-3-4-5-6
> rows: 1

select g, group_concat(v separator v) from test group by g;
> exception INVALID_VALUE_2

drop table test;
> ok
