-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select rand(1) e, random() f from test;
> E                  F
> ------------------ -------------------
> 0.7308781907032909 0.41008081149220166
> rows: 1

select rand() from test;
>> 0.20771484130971707
