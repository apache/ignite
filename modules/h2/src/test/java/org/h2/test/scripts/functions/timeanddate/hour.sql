-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select hour(time '23:10:59') from test;
>> 23

drop table test;
> ok

create table test(ts timestamp with time zone);
> ok

insert into test(ts) values ('2010-05-11 05:15:10+10:00'), ('2010-05-11 05:15:10-10:00');
> update count: 2

select hour(ts) h from test;
> H
> -
> 5
> 5
> rows: 2

drop table test;
> ok

select hour('2001-02-03 14:15:16');
>> 14
