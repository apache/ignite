-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT LENGTH(CAST(RANDOM_UUID() AS VARCHAR));
>> 36

SELECT RANDOM_UUID() = RANDOM_UUID();
>> FALSE

SELECT NEWID();
> exception FUNCTION_NOT_FOUND_1

SET MODE MSSQLServer;
> ok

SELECT LENGTH(CAST(NEWID() AS VARCHAR));
>> 36

SET MODE Regular;
> ok
