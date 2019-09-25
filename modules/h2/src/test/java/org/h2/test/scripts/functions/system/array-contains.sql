-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select array_contains((4.0, 2.0, 2.0), 2.0);
>> TRUE

select array_contains((4.0, 2.0, 2.0), 5.0);
>> FALSE

select array_contains(('one', 'two'), 'one');
>> TRUE

select array_contains(('one', 'two'), 'xxx');
>> FALSE

select array_contains(('one', 'two'), null);
>> FALSE

select array_contains((null, 'two'), null);
>> TRUE

select array_contains(null, 'one');
>> FALSE

select array_contains(((1, 2), (3, 4)), (1, 2));
>> TRUE

select array_contains(((1, 2), (3, 4)), (5, 6));
>> FALSE
