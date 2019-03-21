-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select week(date '2003-01-09') from test;
>> 2

drop table test;
> ok

-- ISO_WEEK

select iso_week('2006-12-31') w, iso_year('2007-12-31') y, iso_day_of_week('2007-12-31') w;
> W  Y    W
> -- ---- -
> 52 2008 1
> rows: 1
