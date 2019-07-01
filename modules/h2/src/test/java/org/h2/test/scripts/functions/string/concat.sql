-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');

select concat(null, null) en, concat(null, 'a') ea, concat('b', null) eb, concat('ab', 'c') abc from test;
> EN   EA EB ABC
> ---- -- -- ---
> null a  b  abc
> rows: 1

SELECT CONCAT('a', 'b', 'c', 'd');
>> abcd
