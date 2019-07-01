-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select dayofmonth(date '2005-09-12');
>> 12

create table test(ts timestamp with time zone);
> ok

insert into test(ts) values ('2010-05-11 00:00:00+10:00'), ('2010-05-11 00:00:00-10:00');
> update count: 2

select dayofmonth(ts) d from test;
> D
> --
> 11
> 11
> rows: 2

drop table test;
> ok
