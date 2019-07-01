-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select length(curdate()) c1, length(current_date()) c2, substring(curdate(), 5, 1) c3;
> C1 C2 C3
> -- -- --
> 10 10 -
> rows: 1

SELECT CURRENT_DATE IS OF (DATE);
>> TRUE

SELECT GETDATE();
> exception FUNCTION_NOT_FOUND_1

SET MODE MSSQLServer;
> ok

SELECT CURRENT_DATE = GETDATE();
>> TRUE

SET MODE Regular;
> ok
