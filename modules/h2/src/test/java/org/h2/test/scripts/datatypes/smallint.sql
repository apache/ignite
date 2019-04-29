-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- Division

SELECT CAST(1 AS SMALLINT) / CAST(0 AS SMALLINT);
> exception DIVISION_BY_ZERO_1

SELECT CAST(-32768 AS SMALLINT) / CAST(1 AS SMALLINT);
>> -32768

SELECT CAST(-32768 AS SMALLINT) / CAST(-1 AS SMALLINT);
> exception NUMERIC_VALUE_OUT_OF_RANGE_1
