-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select power(null, null) en, power(2, 3) e8, power(16, 0.5) e4;
> EN   E8  E4
> ---- --- ---
> null 8.0 4.0
> rows: 1

SELECT POWER(10, 2) IS OF (DOUBLE);
>> TRUE
