-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select datediff('yy', timestamp '2003-12-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') from test;
>> 1

select datediff('year', timestamp '2003-12-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') from test;
>> 1

select datediff('mm', timestamp '2003-11-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') from test;
>> 2

select datediff('month', timestamp '2003-11-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') from test;
>> 2

select datediff('dd', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-05 10:00:00.0') from test;
>> 4

select datediff('day', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-05 10:00:00.0') from test;
>> 4

select datediff('hh', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-02 10:00:00.0') from test;
>> 24

select datediff('hour', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-02 10:00:00.0') from test;
>> 24

select datediff('mi', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') from test;
>> -20

select datediff('minute', timestamp '2004-01-01 10:20:30.0', timestamp '2004-01-01 10:00:00.0') from test;
>> -20

select datediff('ss', timestamp '2004-01-01 10:00:00.5', timestamp '2004-01-01 10:00:01.0') from test;
>> 1

select datediff('second', timestamp '2004-01-01 10:00:00.5', timestamp '2004-01-01 10:00:01.0') from test;
>> 1

select datediff('ms', timestamp '2004-01-01 10:00:00.5', timestamp '2004-01-01 10:00:01.0') from test;
>> 500

select datediff('millisecond', timestamp '2004-01-01 10:00:00.5', timestamp '2004-01-01 10:00:01.0') from test;
>> 500

SELECT DATEDIFF('SECOND', '1900-01-01 00:00:00.001', '1900-01-01 00:00:00.002'), DATEDIFF('SECOND', '2000-01-01 00:00:00.001', '2000-01-01 00:00:00.002');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('SECOND', '1900-01-01 00:00:00.000', '1900-01-01 00:00:00.001'), DATEDIFF('SECOND', '2000-01-01 00:00:00.000', '2000-01-01 00:00:00.001');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('MINUTE', '1900-01-01 00:00:00.000', '1900-01-01 00:00:01.000'), DATEDIFF('MINUTE', '2000-01-01 00:00:00.000', '2000-01-01 00:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('MINUTE', '1900-01-01 00:00:01.000', '1900-01-01 00:00:02.000'), DATEDIFF('MINUTE', '2000-01-01 00:00:01.000', '2000-01-01 00:00:02.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('HOUR', '1900-01-01 00:00:00.000', '1900-01-01 00:00:01.000'), DATEDIFF('HOUR', '2000-01-01 00:00:00.000', '2000-01-01 00:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('HOUR', '1900-01-01 00:00:00.001', '1900-01-01 00:00:01.000'), DATEDIFF('HOUR', '2000-01-01 00:00:00.001', '2000-01-01 00:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('HOUR', '1900-01-01 01:00:00.000', '1900-01-01 01:00:01.000'), DATEDIFF('HOUR', '2000-01-01 01:00:00.000', '2000-01-01 01:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('HOUR', '1900-01-01 01:00:00.001', '1900-01-01 01:00:01.000'), DATEDIFF('HOUR', '2000-01-01 01:00:00.001', '2000-01-01 01:00:01.000');
> 0 0
> - -
> 0 0
> rows: 1

select datediff(day, '2015-12-09 23:59:00.0', '2016-01-16 23:59:00.0'), datediff(wk, '2015-12-09 23:59:00.0', '2016-01-16 23:59:00.0');
> 38 5
> -- -
> 38 5
> rows: 1

call datediff('MS', TIMESTAMP '2001-02-03 04:05:06.789001', TIMESTAMP '2001-02-03 04:05:06.789002');
> 0
> -
> 0
> rows: 1

call datediff('MS', TIMESTAMP '1900-01-01 00:00:01.000', TIMESTAMP '2008-01-01 00:00:00.000');
>> 3408134399000

SELECT DATEDIFF('MICROSECOND', '2006-01-01 00:00:00.0000000', '2006-01-01 00:00:00.123456789'),
    DATEDIFF('MCS', '2006-01-01 00:00:00.0000000', '2006-01-01 00:00:00.123456789'),
    DATEDIFF('MCS', '2006-01-01 00:00:00.0000000', '2006-01-02 00:00:00.123456789');
> 123456 123456 86400123456
> ------ ------ -----------
> 123456 123456 86400123456
> rows: 1

SELECT DATEDIFF('NANOSECOND', '2006-01-01 00:00:00.0000000', '2006-01-01 00:00:00.123456789'),
    DATEDIFF('NS', '2006-01-01 00:00:00.0000000', '2006-01-01 00:00:00.123456789'),
    DATEDIFF('NS', '2006-01-01 00:00:00.0000000', '2006-01-02 00:00:00.123456789');
> 123456789 123456789 86400123456789
> --------- --------- --------------
> 123456789 123456789 86400123456789
> rows: 1

SELECT DATEDIFF('WEEK', DATE '2018-02-02', DATE '2018-02-03'), DATEDIFF('ISO_WEEK', DATE '2018-02-02', DATE '2018-02-03');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('WEEK', DATE '2018-02-03', DATE '2018-02-04'), DATEDIFF('ISO_WEEK', DATE '2018-02-03', DATE '2018-02-04');
> 1 0
> - -
> 1 0
> rows: 1

SELECT DATEDIFF('WEEK', DATE '2018-02-04', DATE '2018-02-05'), DATEDIFF('ISO_WEEK', DATE '2018-02-04', DATE '2018-02-05');
> 0 1
> - -
> 0 1
> rows: 1

SELECT DATEDIFF('WEEK', DATE '2018-02-05', DATE '2018-02-06'), DATEDIFF('ISO_WEEK', DATE '2018-02-05', DATE '2018-02-06');
> 0 0
> - -
> 0 0
> rows: 1

SELECT DATEDIFF('WEEK', DATE '1969-12-27', DATE '1969-12-28'), DATEDIFF('ISO_WEEK', DATE '1969-12-27', DATE '1969-12-28');
> 1 0
> - -
> 1 0
> rows: 1

SELECT DATEDIFF('WEEK', DATE '1969-12-28', DATE '1969-12-29'), DATEDIFF('ISO_WEEK', DATE '1969-12-28', DATE '1969-12-29');
> 0 1
> - -
> 0 1
> rows: 1

SELECT DATEDIFF('QUARTER', DATE '2009-12-30', DATE '2009-12-31');
>> 0

SELECT DATEDIFF('QUARTER', DATE '2010-01-01', DATE '2009-12-31');
>> -1

SELECT DATEDIFF('QUARTER', DATE '2010-01-01', DATE '2010-01-02');
>> 0

SELECT DATEDIFF('QUARTER', DATE '2010-01-01', DATE '2010-03-31');
>> 0

SELECT DATEDIFF('QUARTER', DATE '-1000-01-01', DATE '2000-01-01');
>> 12000

SELECT DATEDIFF('TIMEZONE_HOUR', TIMESTAMP WITH TIME ZONE '2010-01-01 10:00:00+01',
    TIMESTAMP WITH TIME ZONE '2012-02-02 12:00:00+02');
>> 1

SELECT DATEDIFF('TIMEZONE_MINUTE', TIMESTAMP WITH TIME ZONE '2010-01-01 10:00:00+01:15',
    TIMESTAMP WITH TIME ZONE '2012-02-02 12:00:00+02');
>> 45

select datediff('HOUR', timestamp '2007-01-06 10:00:00Z', '2007-01-06 10:00:00Z');
>> 0

select datediff('HOUR', timestamp '1234-05-06 10:00:00+01:00', '1234-05-06 10:00:00+02:00');
>> -1

select datediff('HOUR', timestamp '1234-05-06 10:00:00+01:00', '1234-05-06 10:00:00-02:00');
>> 3

select timestampdiff(month, '2003-02-01','2003-05-01');
>> 3

select timestampdiff(YEAR,'2002-05-01','2001-01-01');
>> -1

select timestampdiff(YEAR,'2017-01-01','2016-12-31 23:59:59');
>> -1

select timestampdiff(YEAR,'2017-01-01','2017-12-31 23:59:59');
>> 0

select timestampdiff(MINUTE,'2003-02-01','2003-05-01 12:05:55');
>> 128885
