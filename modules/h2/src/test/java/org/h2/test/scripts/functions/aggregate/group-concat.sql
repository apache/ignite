-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v varchar);
> ok

insert into test values ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7'), ('8'), ('9');
> update count: 9

select group_concat(v order by v asc separator '-'),
    group_concat(v order by v desc separator '-') filter (where v >= '4')
    from test where v >= '2';
> GROUP_CONCAT(V ORDER BY V SEPARATOR '-') GROUP_CONCAT(V ORDER BY V DESC SEPARATOR '-') FILTER (WHERE (V >= '4'))
> ---------------------------------------- -----------------------------------------------------------------------
> 2-3-4-5-6-7-8-9                          9-8-7-6-5-4
> rows (ordered): 1

create index test_idx on test(v);

select group_concat(v order by v asc separator '-'),
    group_concat(v order by v desc separator '-') filter (where v >= '4')
    from test where v >= '2';
> GROUP_CONCAT(V ORDER BY V SEPARATOR '-') GROUP_CONCAT(V ORDER BY V DESC SEPARATOR '-') FILTER (WHERE (V >= '4'))
> ---------------------------------------- -----------------------------------------------------------------------
> 2-3-4-5-6-7-8-9                          9-8-7-6-5-4
> rows (ordered): 1

select group_concat(v order by v asc separator '-'),
    group_concat(v order by v desc separator '-') filter (where v >= '4')
    from test;
> GROUP_CONCAT(V ORDER BY V SEPARATOR '-') GROUP_CONCAT(V ORDER BY V DESC SEPARATOR '-') FILTER (WHERE (V >= '4'))
> ---------------------------------------- -----------------------------------------------------------------------
> 1-2-3-4-5-6-7-8-9                        9-8-7-6-5-4
> rows (ordered): 1


drop table test;
> ok
