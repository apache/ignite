-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select array_cat(ARRAY[1, 2], ARRAY[3, 4]) = ARRAY[1, 2, 3, 4];
>> TRUE

select array_cat(ARRAY[1, 2], null) is null;
>> TRUE

select array_cat(null, ARRAY[1, 2]) is null;
>> TRUE

select array_append(ARRAY[1, 2], 3) = ARRAY[1, 2, 3];
>> TRUE

select array_append(ARRAY[1, 2], null) is null;
>> TRUE

select array_append(null, 3) is null;
>> TRUE
