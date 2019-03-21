-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

----- Issue#600 -----
create table test as (select char(x) as str from system_range(48,90));
> ok

select rownum() as rnum, str from test where str = 'A';
> RNUM STR
> ---- ---
> 1    A
> rows: 1

drop table test;
> ok
