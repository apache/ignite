-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- 01-Aug-03 + 3 months = 01-Nov-03
SELECT ADD_MONTHS('2003-08-01', 3);
>> 2003-11-01 00:00:00

-- 31-Jan-03 + 1 month = 28-Feb-2003
SELECT ADD_MONTHS('2003-01-31', 1);
>> 2003-02-28 00:00:00

-- 21-Aug-2003 - 3 months = 21-May-2003
SELECT ADD_MONTHS('2003-08-21', -3);
>> 2003-05-21 00:00:00

-- 21-Aug-2003 00:00:00.333 - 3 months = 21-May-2003 00:00:00.333
SELECT ADD_MONTHS('2003-08-21 00:00:00.333', -3);
>> 2003-05-21 00:00:00.333
