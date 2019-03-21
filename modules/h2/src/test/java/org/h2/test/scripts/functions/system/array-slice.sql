-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select array_slice(ARRAY[1, 2, 3, 4], 1, 1) = ARRAY[1];
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], 1, 3) = ARRAY[1, 2, 3];
>> TRUE

-- test invalid indexes
select array_slice(ARRAY[1, 2, 3, 4], 3, 1) is null;
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], 0, 3) is null;
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], 1, 5) is null;
>> TRUE

-- in PostgreSQL, indexes are corrected
SET MODE PostgreSQL;
> ok

select array_slice(ARRAY[1, 2, 3, 4], 3, 1) = ARRAY[];
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], 0, 3) = ARRAY[1, 2, 3];
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], 1, 5) = ARRAY[1, 2, 3, 4];
>> TRUE

SET MODE Regular;
> ok

-- null parameters
select array_slice(null, 1, 3) is null;
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], null, 3) is null;
>> TRUE

select array_slice(ARRAY[1, 2, 3, 4], 1, null) is null;
>> TRUE
