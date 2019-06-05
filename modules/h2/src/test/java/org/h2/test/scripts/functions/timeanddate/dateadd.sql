-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select dateadd('month', 1, timestamp '2003-01-31 10:20:30.012345678') from test;
>> 2003-02-28 10:20:30.012345678

select dateadd('year', -1, timestamp '2000-02-29 10:20:30.012345678') from test;
>> 1999-02-28 10:20:30.012345678

drop table test;
> ok

create table test(d date, t time, ts timestamp);
> ok

insert into test values(date '2001-01-01', time '01:00:00', timestamp '2010-01-01 00:00:00');
> update count: 1

select ts + t from test;
>> 2010-01-01 01:00:00

select ts + t + t - t x from test;
>> 2010-01-01 01:00:00

select ts + t * 0.5 x from test;
>> 2010-01-01 00:30:00

select ts + 0.5 x from test;
>> 2010-01-01 12:00:00

select ts - 1.5 x from test;
>> 2009-12-30 12:00:00

select ts + 0.5 * t + t - t x from test;
>> 2010-01-01 00:30:00

select ts + t / 0.5 x from test;
>> 2010-01-01 02:00:00

select d + t, t + d - t x from test;
> T + D               X
> ------------------- -------------------
> 2001-01-01 01:00:00 2001-01-01 00:00:00
> rows: 1

select 1 + d + 1, d - 1, 2 + ts + 2, ts - 2 from test;
> DATEADD('DAY', 1, DATEADD('DAY', 1, D)) DATEADD('DAY', -1, D) DATEADD('DAY', 2, DATEADD('DAY', 2, TS)) DATEADD('DAY', -2, TS)
> --------------------------------------- --------------------- ---------------------------------------- ----------------------
> 2001-01-03                              2000-12-31            2010-01-05 00:00:00                      2009-12-30 00:00:00
> rows: 1

select 1 + d + t + 1 from test;
>> 2001-01-03 01:00:00

select ts - t - 2 from test;
>> 2009-12-29 23:00:00

drop table test;
> ok

call dateadd('MS', 1, TIMESTAMP '2001-02-03 04:05:06.789001');
>> 2001-02-03 04:05:06.790001

SELECT DATEADD('MICROSECOND', 1, TIME '10:00:01'), DATEADD('MCS', 1, TIMESTAMP '2010-10-20 10:00:01.1');
> TIME '10:00:01.000001' TIMESTAMP '2010-10-20 10:00:01.100001'
> ---------------------- --------------------------------------
> 10:00:01.000001        2010-10-20 10:00:01.100001
> rows: 1

SELECT DATEADD('NANOSECOND', 1, TIME '10:00:01'), DATEADD('NS', 1, TIMESTAMP '2010-10-20 10:00:01.1');
> TIME '10:00:01.000000001' TIMESTAMP '2010-10-20 10:00:01.100000001'
> ------------------------- -----------------------------------------
> 10:00:01.000000001        2010-10-20 10:00:01.100000001
> rows: 1

SELECT DATEADD('HOUR', 1, DATE '2010-01-20');
>> 2010-01-20 01:00:00

SELECT DATEADD('MINUTE', 30, TIME '12:30:55');
>> 13:00:55

SELECT DATEADD('DAY', 1, TIME '12:30:55');
> exception

SELECT DATEADD('QUARTER', 1, DATE '2010-11-16');
>> 2011-02-16

SELECT DATEADD('DAY', 10, TIMESTAMP WITH TIME ZONE '2000-01-05 15:00:30.123456789-10');
>> 2000-01-15 15:00:30.123456789-10

SELECT TIMESTAMPADD('DAY', 10, TIMESTAMP '2000-01-05 15:00:30.123456789');
>> 2000-01-15 15:00:30.123456789

SELECT TIMESTAMPADD('TIMEZONE_HOUR', 1, TIMESTAMP WITH TIME ZONE '2010-01-01 10:00:00+07:30');
>> 2010-01-01 10:00:00+08:30

SELECT TIMESTAMPADD('TIMEZONE_MINUTE', -45, TIMESTAMP WITH TIME ZONE '2010-01-01 10:00:00+07:30');
>> 2010-01-01 10:00:00+06:45

SELECT DATEADD(HOUR, 1, TIME '23:00:00');
>> 00:00:00
> rows: 1
