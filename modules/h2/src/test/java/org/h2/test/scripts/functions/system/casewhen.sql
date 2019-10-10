-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select casewhen(null, '1', '2') xn, casewhen(1>0, 'n', 'y') xy, casewhen(0<1, 'a', 'b') xa from test;
> XN XY XA
> -- -- --
> 2  n  a
> rows: 1

select x, case when x=0 then 'zero' else 'not zero' end y from system_range(0, 2);
> X Y
> - --------
> 0 zero
> 1 not zero
> 2 not zero
> rows: 3

select x, case when x=0 then 'zero' end y from system_range(0, 1);
> X Y
> - ----
> 0 zero
> 1 null
> rows: 2

select x, case x when 0 then 'zero' else 'not zero' end y from system_range(0, 1);
> X Y
> - --------
> 0 zero
> 1 not zero
> rows: 2

select x, case x when 0 then 'zero' when 1 then 'one' end y from system_range(0, 2);
> X Y
> - ----
> 0 zero
> 1 one
> 2 null
> rows: 3
