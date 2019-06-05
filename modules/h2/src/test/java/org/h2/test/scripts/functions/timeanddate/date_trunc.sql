-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

--
-- Test time unit in 'MICROSECONDS'
--
SELECT DATE_TRUNC('MICROSECONDS', time '00:00:00.000');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('microseconds', time '00:00:00.000');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('MICROSECONDS', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('microseconds', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('MICROSECONDS', time '15:14:13');
>> 1970-01-01 15:14:13

SELECT DATE_TRUNC('microseconds', time '15:14:13');
>> 1970-01-01 15:14:13

SELECT DATE_TRUNC('MICROSECONDS', time '15:14:13.123456789');
>> 1970-01-01 15:14:13.123456

SELECT DATE_TRUNC('microseconds', time '15:14:13.123456789');
>> 1970-01-01 15:14:13.123456

SELECT DATE_TRUNC('MICROSECONDS', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('microseconds', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MICROSECONDS', date '1970-01-01');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('microseconds', date '1970-01-01');
>> 1970-01-01 00:00:00

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13.123456789');
>> 2015-05-29 15:14:13.123456+00

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13.123456789');
>> 2015-05-29 15:14:13.123456+00

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13.123456789-06');
>> 2015-05-29 15:14:13.123456-06

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13.123456789-06');
>> 2015-05-29 15:14:13.123456-06

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('MICROSECONDS', timestamp with time zone '2015-05-29 15:14:13.123456789+10');
>> 2015-05-29 15:14:13.123456+10

select DATE_TRUNC('microseconds', timestamp with time zone '2015-05-29 15:14:13.123456789+10');
>> 2015-05-29 15:14:13.123456+10

SELECT DATE_TRUNC('microseconds', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('MICROSECONDS', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('microseconds', timestamp '2015-05-29 15:14:13.123456789');
>> 2015-05-29 15:14:13.123456

SELECT DATE_TRUNC('MICROSECONDS', timestamp '2015-05-29 15:14:13.123456789');
>> 2015-05-29 15:14:13.123456

SELECT DATE_TRUNC('microseconds', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('MICROSECONDS', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('microseconds', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MICROSECONDS', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('microseconds', '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('MICROSECONDS', '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('microseconds', '2015-05-29 15:14:13.123456789');
>> 2015-05-29 15:14:13.123456

SELECT DATE_TRUNC('MICROSECONDS', '2015-05-29 15:14:13.123456789');
>> 2015-05-29 15:14:13.123456

SELECT DATE_TRUNC('microseconds', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('MICROSECONDS', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('microseconds', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MICROSECONDS', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

--
-- Test time unit in 'MILLISECONDS'
--
SELECT DATE_TRUNC('MILLISECONDS', time '00:00:00.000');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('milliseconds', time '00:00:00.000');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('MILLISECONDS', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('milliseconds', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('MILLISECONDS', time '15:14:13');
>> 1970-01-01 15:14:13

SELECT DATE_TRUNC('milliseconds', time '15:14:13');
>> 1970-01-01 15:14:13

SELECT DATE_TRUNC('MILLISECONDS', time '15:14:13.123456');
>> 1970-01-01 15:14:13.123

SELECT DATE_TRUNC('milliseconds', time '15:14:13.123456');
>> 1970-01-01 15:14:13.123

SELECT DATE_TRUNC('MILLISECONDS', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('milliseconds', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MILLISECONDS', date '1970-01-01');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('milliseconds', date '1970-01-01');
>> 1970-01-01 00:00:00

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13.123+00

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13.123+00

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13.123456-06');
>> 2015-05-29 15:14:13.123-06

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13.123456-06');
>> 2015-05-29 15:14:13.123-06

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('MILLISECONDS', timestamp with time zone '2015-05-29 15:14:13.123456+10');
>> 2015-05-29 15:14:13.123+10

select DATE_TRUNC('milliseconds', timestamp with time zone '2015-05-29 15:14:13.123456+10');
>> 2015-05-29 15:14:13.123+10

SELECT DATE_TRUNC('milliseconds', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('MILLISECONDS', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('milliseconds', timestamp '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13.123

SELECT DATE_TRUNC('MILLISECONDS', timestamp '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13.123

SELECT DATE_TRUNC('milliseconds', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('MILLISECONDS', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('milliseconds', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MILLISECONDS', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('milliseconds', '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('MILLISECONDS', '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('milliseconds', '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13.123

SELECT DATE_TRUNC('MILLISECONDS', '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13.123

SELECT DATE_TRUNC('milliseconds', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('MILLISECONDS', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('milliseconds', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MILLISECONDS', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

--
-- Test time unit 'SECOND'
--
SELECT DATE_TRUNC('SECOND', time '00:00:00.000');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('second', time '00:00:00.000');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('SECOND', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('second', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('SECOND', time '15:14:13');
>> 1970-01-01 15:14:13

SELECT DATE_TRUNC('second', time '15:14:13');
>> 1970-01-01 15:14:13

SELECT DATE_TRUNC('SECOND', time '15:14:13.123456');
>> 1970-01-01 15:14:13

SELECT DATE_TRUNC('second', time '15:14:13.123456');
>> 1970-01-01 15:14:13

SELECT DATE_TRUNC('SECOND', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('second', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('SECOND', date '1970-01-01');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('second', date '1970-01-01');
>> 1970-01-01 00:00:00

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13+00

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13.123456-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13.123456-06');
>> 2015-05-29 15:14:13-06

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('SECOND', timestamp with time zone '2015-05-29 15:14:13.123456+10');
>> 2015-05-29 15:14:13+10

select DATE_TRUNC('second', timestamp with time zone '2015-05-29 15:14:13.123456+10');
>> 2015-05-29 15:14:13+10

SELECT DATE_TRUNC('second', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('SECOND', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('second', timestamp '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('SECOND', timestamp '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('second', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('SECOND', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('second', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('SECOND', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('second', '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('SECOND', '2015-05-29 15:14:13');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('second', '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('SECOND', '2015-05-29 15:14:13.123456');
>> 2015-05-29 15:14:13

SELECT DATE_TRUNC('second', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('SECOND', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('second', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('SECOND', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00


--
-- Test time unit 'MINUTE'
--
SELECT DATE_TRUNC('MINUTE', time '00:00:00');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('minute', time '00:00:00');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('MINUTE', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('minute', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('MINUTE', time '15:14:13');
>> 1970-01-01 15:14:00

SELECT DATE_TRUNC('minute', time '15:14:13');
>> 1970-01-01 15:14:00

SELECT DATE_TRUNC('MINUTE', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('minute', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MINUTE', date '1970-01-01');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('minute', date '1970-01-01');
>> 1970-01-01 00:00:00

select DATE_TRUNC('MINUTE', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:14:00+00

select DATE_TRUNC('minute', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:14:00+00

select DATE_TRUNC('MINUTE', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:00-06

select DATE_TRUNC('minute', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:14:00-06

select DATE_TRUNC('MINUTE', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:00+10

select DATE_TRUNC('minute', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:14:00+10

SELECT DATE_TRUNC('minute', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:00

SELECT DATE_TRUNC('MINUTE', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:14:00

SELECT DATE_TRUNC('minute', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('MINUTE', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('minute', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MINUTE', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('minute', '2015-05-29 15:14:13');
>> 2015-05-29 15:14:00

SELECT DATE_TRUNC('MINUTE', '2015-05-29 15:14:13');
>> 2015-05-29 15:14:00

SELECT DATE_TRUNC('minute', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('MINUTE', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('minute', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('MINUTE', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

--
-- Test time unit 'HOUR'
--
SELECT DATE_TRUNC('HOUR', time '00:00:00');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('hour', time '00:00:00');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('HOUR', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('hour', time '15:00:00');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('HOUR', time '15:14:13');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('hour', time '15:14:13');
>> 1970-01-01 15:00:00

SELECT DATE_TRUNC('HOUR', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('hour', date '2015-05-29');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('HOUR', date '1970-01-01');
>> 1970-01-01 00:00:00

SELECT DATE_TRUNC('hour', date '1970-01-01');
>> 1970-01-01 00:00:00

select DATE_TRUNC('HOUR', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00+00

select DATE_TRUNC('hour', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00+00

select DATE_TRUNC('HOUR', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:00:00-06

select DATE_TRUNC('hour', timestamp with time zone '2015-05-29 15:14:13-06');
>> 2015-05-29 15:00:00-06

select DATE_TRUNC('HOUR', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:00:00+10

select DATE_TRUNC('hour', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 15:00:00+10

SELECT DATE_TRUNC('hour', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('HOUR', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('hour', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('HOUR', timestamp '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('hour', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('HOUR', timestamp '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('hour', '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('HOUR', '2015-05-29 15:14:13');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('hour', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('HOUR', '2015-05-29 15:00:00');
>> 2015-05-29 15:00:00

SELECT DATE_TRUNC('hour', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

SELECT DATE_TRUNC('HOUR', '2015-05-29 00:00:00');
>> 2015-05-29 00:00:00

--
-- Test time unit 'DAY'
--
select DATE_TRUNC('day', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('DAY', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('day', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('DAY', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('day', date '2015-05-29');
>> 2015-05-29 00:00:00

select DATE_TRUNC('DAY', date '2015-05-29');
>> 2015-05-29 00:00:00

select DATE_TRUNC('day', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00

select DATE_TRUNC('DAY', timestamp '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00+00

select DATE_TRUNC('DAY', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00+00

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-29 00:00:00-06

select DATE_TRUNC('DAY', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-29 00:00:00-06

select DATE_TRUNC('day', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 00:00:00+10

select DATE_TRUNC('DAY', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-29 00:00:00+10

select DATE_TRUNC('day', '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00

select DATE_TRUNC('DAY', '2015-05-29 15:14:13');
>> 2015-05-29 00:00:00


--
-- Test time unit 'WEEK'
--
select DATE_TRUNC('week', time '00:00:00');
>> 1969-12-29 00:00:00

select DATE_TRUNC('WEEK', time '00:00:00');
>> 1969-12-29 00:00:00

select DATE_TRUNC('week', time '15:14:13');
>> 1969-12-29 00:00:00

select DATE_TRUNC('WEEK', time '15:14:13');
>> 1969-12-29 00:00:00

select DATE_TRUNC('week', date '2015-05-28');
>> 2015-05-25 00:00:00

select DATE_TRUNC('WEEK', date '2015-05-28');
>> 2015-05-25 00:00:00

select DATE_TRUNC('week', timestamp '2015-05-29 15:14:13');
>> 2015-05-25 00:00:00

select DATE_TRUNC('WEEK', timestamp '2015-05-29 15:14:13');
>> 2015-05-25 00:00:00

select DATE_TRUNC('week', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-25 00:00:00+00

select DATE_TRUNC('WEEK', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-25 00:00:00+00

select DATE_TRUNC('week', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-25 00:00:00-06

select DATE_TRUNC('WEEK', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-25 00:00:00-06

select DATE_TRUNC('week', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-25 00:00:00+10

select DATE_TRUNC('WEEK', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-25 00:00:00+10

select DATE_TRUNC('week', '2015-05-29 15:14:13');
>> 2015-05-25 00:00:00

select DATE_TRUNC('WEEK', '2015-05-29 15:14:13');
>> 2015-05-25 00:00:00

SELECT DATE_TRUNC('WEEK', '2018-03-14 00:00:00.000');
>> 2018-03-12 00:00:00

SELECT DATE_TRUNC('week', '2018-03-14 00:00:00.000');
>> 2018-03-12 00:00:00

--
-- Test time unit 'MONTH'
--
select DATE_TRUNC('month', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('MONTH', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('month', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('MONTH', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('month', date '2015-05-28');
>> 2015-05-01 00:00:00

select DATE_TRUNC('MONTH', date '2015-05-28');
>> 2015-05-01 00:00:00

select DATE_TRUNC('month', timestamp '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00

select DATE_TRUNC('MONTH', timestamp '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00

select DATE_TRUNC('month', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00+00

select DATE_TRUNC('MONTH', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00+00

select DATE_TRUNC('month', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-01 00:00:00-06

select DATE_TRUNC('MONTH', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-05-01 00:00:00-06

select DATE_TRUNC('month', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-01 00:00:00+10

select DATE_TRUNC('MONTH', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-05-01 00:00:00+10

select DATE_TRUNC('month', '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00

select DATE_TRUNC('MONTH', '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00

SELECT DATE_TRUNC('MONTH', '2018-03-14 00:00:00.000');
>> 2018-03-01 00:00:00

SELECT DATE_TRUNC('month', '2018-03-14 00:00:00.000');
>> 2018-03-01 00:00:00

SELECT DATE_TRUNC('month', '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00

SELECT DATE_TRUNC('MONTH', '2015-05-29 15:14:13');
>> 2015-05-01 00:00:00

SELECT DATE_TRUNC('month', '2015-05-01 15:14:13');
>> 2015-05-01 00:00:00

SELECT DATE_TRUNC('MONTH', '2015-05-01 15:14:13');
>> 2015-05-01 00:00:00

--
-- Test time unit 'QUARTER'
--
select DATE_TRUNC('quarter', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('QUARTER', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('quarter', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('QUARTER', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('quarter', date '2015-05-28');
>> 2015-04-01 00:00:00

select DATE_TRUNC('QUARTER', date '2015-05-28');
>> 2015-04-01 00:00:00

select DATE_TRUNC('quarter', timestamp '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00

select DATE_TRUNC('QUARTER', timestamp '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00

select DATE_TRUNC('quarter', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00+00

select DATE_TRUNC('QUARTER', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00+00

select DATE_TRUNC('quarter', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-04-01 00:00:00-06

select DATE_TRUNC('QUARTER', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-04-01 00:00:00-06

select DATE_TRUNC('quarter', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-04-01 00:00:00+10

select DATE_TRUNC('QUARTER', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-04-01 00:00:00+10

select DATE_TRUNC('quarter', '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00

select DATE_TRUNC('QUARTER', '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00

SELECT DATE_TRUNC('QUARTER', '2018-03-14 00:00:00.000');
>> 2018-01-01 00:00:00

SELECT DATE_TRUNC('quarter', '2018-03-14 00:00:00.000');
>> 2018-01-01 00:00:00

SELECT DATE_TRUNC('quarter', '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00

SELECT DATE_TRUNC('QUARTER', '2015-05-29 15:14:13');
>> 2015-04-01 00:00:00

SELECT DATE_TRUNC('quarter', '2015-05-01 15:14:13');
>> 2015-04-01 00:00:00

SELECT DATE_TRUNC('QUARTER', '2015-05-01 15:14:13');
>> 2015-04-01 00:00:00

SELECT DATE_TRUNC('quarter', '2015-07-29 15:14:13');
>> 2015-07-01 00:00:00

SELECT DATE_TRUNC('QUARTER', '2015-07-29 15:14:13');
>> 2015-07-01 00:00:00

SELECT DATE_TRUNC('quarter', '2015-09-29 15:14:13');
>> 2015-07-01 00:00:00

SELECT DATE_TRUNC('QUARTER', '2015-09-29 15:14:13');
>> 2015-07-01 00:00:00

SELECT DATE_TRUNC('quarter', '2015-10-29 15:14:13');
>> 2015-10-01 00:00:00

SELECT DATE_TRUNC('QUARTER', '2015-10-29 15:14:13');
>> 2015-10-01 00:00:00

SELECT DATE_TRUNC('quarter', '2015-12-29 15:14:13');
>> 2015-10-01 00:00:00

SELECT DATE_TRUNC('QUARTER', '2015-12-29 15:14:13');
>> 2015-10-01 00:00:00


--
-- Test time unit 'YEAR'
--
select DATE_TRUNC('year', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('YEAR', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('year', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('YEAR', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('year', date '2015-05-28');
>> 2015-01-01 00:00:00

select DATE_TRUNC('YEAR', date '2015-05-28');
>> 2015-01-01 00:00:00

select DATE_TRUNC('year', timestamp '2015-05-29 15:14:13');
>> 2015-01-01 00:00:00

select DATE_TRUNC('YEAR', timestamp '2015-05-29 15:14:13');
>> 2015-01-01 00:00:00

select DATE_TRUNC('year', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-01-01 00:00:00+00

select DATE_TRUNC('YEAR', timestamp with time zone '2015-05-29 15:14:13');
>> 2015-01-01 00:00:00+00

select DATE_TRUNC('year', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-01-01 00:00:00-06

select DATE_TRUNC('YEAR', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2015-01-01 00:00:00-06

select DATE_TRUNC('year', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-01-01 00:00:00+10

select DATE_TRUNC('YEAR', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2015-01-01 00:00:00+10

SELECT DATE_TRUNC('year', '2015-05-29 15:14:13');
>> 2015-01-01 00:00:00

SELECT DATE_TRUNC('YEAR', '2015-05-29 15:14:13');
>> 2015-01-01 00:00:00

--
-- Test time unit 'DECADE'
--
select DATE_TRUNC('decade', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('DECADE', time '00:00:00');
>> 1970-01-01 00:00:00

select DATE_TRUNC('decade', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('DECADE', time '15:14:13');
>> 1970-01-01 00:00:00

select DATE_TRUNC('decade', date '2015-05-28');
>> 2010-01-01 00:00:00

select DATE_TRUNC('DECADE', date '2015-05-28');
>> 2010-01-01 00:00:00

select DATE_TRUNC('decade', timestamp '2015-05-29 15:14:13');
>> 2010-01-01 00:00:00

select DATE_TRUNC('DECADE', timestamp '2015-05-29 15:14:13');
>> 2010-01-01 00:00:00

select DATE_TRUNC('decade', timestamp with time zone '2015-05-29 15:14:13');
>> 2010-01-01 00:00:00+00

select DATE_TRUNC('DECADE', timestamp with time zone '2015-05-29 15:14:13');
>> 2010-01-01 00:00:00+00

select DATE_TRUNC('decade', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2010-01-01 00:00:00-06

select DATE_TRUNC('DECADE', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2010-01-01 00:00:00-06

select DATE_TRUNC('decade', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2010-01-01 00:00:00+10

select DATE_TRUNC('DECADE', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2010-01-01 00:00:00+10

SELECT DATE_TRUNC('decade', '2015-05-29 15:14:13');
>> 2010-01-01 00:00:00

SELECT DATE_TRUNC('DECADE', '2015-05-29 15:14:13');
>> 2010-01-01 00:00:00

SELECT DATE_TRUNC('decade', '2010-05-29 15:14:13');
>> 2010-01-01 00:00:00

SELECT DATE_TRUNC('DECADE', '2010-05-29 15:14:13');
>> 2010-01-01 00:00:00

--
-- Test time unit 'CENTURY'
--
select DATE_TRUNC('century', time '00:00:00');
>> 1901-01-01 00:00:00

select DATE_TRUNC('CENTURY', time '00:00:00');
>> 1901-01-01 00:00:00

select DATE_TRUNC('century', time '15:14:13');
>> 1901-01-01 00:00:00

select DATE_TRUNC('CENTURY', time '15:14:13');
>> 1901-01-01 00:00:00

select DATE_TRUNC('century', date '2015-05-28');
>> 2001-01-01 00:00:00

select DATE_TRUNC('CENTURY', date '2015-05-28');
>> 2001-01-01 00:00:00

select DATE_TRUNC('century', timestamp '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

select DATE_TRUNC('CENTURY', timestamp '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

select DATE_TRUNC('century', timestamp with time zone '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00+00

select DATE_TRUNC('CENTURY', timestamp with time zone '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00+00

select DATE_TRUNC('century', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2001-01-01 00:00:00-06

select DATE_TRUNC('CENTURY', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2001-01-01 00:00:00-06

select DATE_TRUNC('century', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2001-01-01 00:00:00+10

select DATE_TRUNC('CENTURY', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2001-01-01 00:00:00+10

SELECT DATE_TRUNC('century', '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

SELECT DATE_TRUNC('CENTURY', '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

SELECT DATE_TRUNC('century', '2199-05-29 15:14:13');
>> 2101-01-01 00:00:00

SELECT DATE_TRUNC('CENTURY', '2199-05-29 15:14:13');
>> 2101-01-01 00:00:00

SELECT DATE_TRUNC('century', '2000-05-29 15:14:13');
>> 1901-01-01 00:00:00

SELECT DATE_TRUNC('CENTURY', '2000-05-29 15:14:13');
>> 1901-01-01 00:00:00

SELECT DATE_TRUNC('century', '2001-05-29 15:14:13');
>> 2001-01-01 00:00:00

SELECT DATE_TRUNC('CENTURY', '2001-05-29 15:14:13');
>> 2001-01-01 00:00:00

--
-- Test time unit 'MILLENNIUM'
--
select DATE_TRUNC('millennium', time '00:00:00');
>> 1001-01-01 00:00:00

select DATE_TRUNC('MILLENNIUM', time '00:00:00');
>> 1001-01-01 00:00:00

select DATE_TRUNC('millennium', time '15:14:13');
>> 1001-01-01 00:00:00

select DATE_TRUNC('MILLENNIUM', time '15:14:13');
>> 1001-01-01 00:00:00

select DATE_TRUNC('millennium', date '2015-05-28');
>> 2001-01-01 00:00:00

select DATE_TRUNC('MILLENNIUM', date '2015-05-28');
>> 2001-01-01 00:00:00

select DATE_TRUNC('millennium', timestamp '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

select DATE_TRUNC('MILLENNIUM', timestamp '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

select DATE_TRUNC('millennium', timestamp with time zone '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00+00

select DATE_TRUNC('MILLENNIUM', timestamp with time zone '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00+00

select DATE_TRUNC('millennium', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2001-01-01 00:00:00-06

select DATE_TRUNC('MILLENNIUM', timestamp with time zone '2015-05-29 05:14:13-06');
>> 2001-01-01 00:00:00-06

select DATE_TRUNC('millennium', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2001-01-01 00:00:00+10

select DATE_TRUNC('MILLENNIUM', timestamp with time zone '2015-05-29 15:14:13+10');
>> 2001-01-01 00:00:00+10

SELECT DATE_TRUNC('millennium', '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

SELECT DATE_TRUNC('MILLENNIUM', '2015-05-29 15:14:13');
>> 2001-01-01 00:00:00

SELECT DATE_TRUNC('millennium', '2001-05-29 15:14:13');
>> 2001-01-01 00:00:00

SELECT DATE_TRUNC('MILLENNIUM', '2001-05-29 15:14:13');
>> 2001-01-01 00:00:00

SELECT DATE_TRUNC('millennium', '2000-05-29 15:14:13');
>> 1001-01-01 00:00:00

SELECT DATE_TRUNC('MILLENNIUM', '2000-05-29 15:14:13');
>> 1001-01-01 00:00:00

--
-- Test unhandled time unit and bad date
--
SELECT DATE_TRUNC('---', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('', '2015-05-29 15:14:13');
> exception

SELECT DATE_TRUNC('', '');
> exception

SELECT DATE_TRUNC('YEAR', '');
> exception

