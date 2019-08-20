-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table person(firstname varchar, lastname varchar);
> ok

create index person_1 on person(firstname, lastname);
> ok

insert into person select convert(x,varchar) as firstname, (convert(x,varchar) || ' last') as lastname from system_range(1,100);
> update count: 100

-- Issue #643: verify that when using an index, we use the IN part of the query, if that part of the query
-- can directly use the index.
--
explain analyze SELECT * FROM person WHERE firstname IN ('FirstName1', 'FirstName2') AND lastname='LastName1';
> PLAN
> -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> SELECT PERSON.FIRSTNAME, PERSON.LASTNAME FROM PUBLIC.PERSON /* PUBLIC.PERSON_1: FIRSTNAME IN('FirstName1', 'FirstName2') AND LASTNAME = 'LastName1' */ /* scanCount: 1 */ WHERE (FIRSTNAME IN('FirstName1', 'FirstName2')) AND (LASTNAME = 'LastName1')
> rows: 1
