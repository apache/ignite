-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select dayofmonth(date '2005-09-12') from test;
>> 12

drop table test;
> ok

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
