-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT (10, 20, 30)[1];
>> 10

SELECT (10, 20, 30)[3];
>> 30

SELECT (10, 20, 30)[0];
>> null

SELECT (10, 20, 30)[4];
>> null

SELECT ARRAY[];
>> []

SELECT ARRAY[10];
>> [10]

SELECT ARRAY[10, 20, 30];
>> [10, 20, 30]

SELECT ARRAY[1, NULL] IS NOT DISTINCT FROM ARRAY[1, NULL];
>> TRUE

SELECT ARRAY[1, NULL] IS DISTINCT FROM ARRAY[1, NULL];
>> FALSE

SELECT ARRAY[1, NULL] = ARRAY[1, NULL];
>> null

SELECT ARRAY[1, NULL] <> ARRAY[1, NULL];
>> null

SELECT ARRAY[NULL] = ARRAY[NULL, NULL];
>> FALSE

select ARRAY[1, NULL, 2] = ARRAY[1, NULL, 1];
>> FALSE

select ARRAY[1, NULL, 2] <> ARRAY[1, NULL, 1];
>> TRUE

SELECT ARRAY[1, NULL] > ARRAY[1, NULL];
>> null

SELECT ARRAY[1, 2] > ARRAY[1, NULL];
>> null

SELECT ARRAY[1, 2, NULL] > ARRAY[1, 1, NULL];
>> TRUE

SELECT ARRAY[1, 1, NULL] > ARRAY[1, 2, NULL];
>> FALSE

SELECT ARRAY[1, 2, NULL] < ARRAY[1, 1, NULL];
>> FALSE

SELECT ARRAY[1, 1, NULL] <= ARRAY[1, 1, NULL];
>> null

SELECT ARRAY[1, NULL] IN (ARRAY[1, NULL]);
>> null

CREATE TABLE TEST(A ARRAY);
> ok

INSERT INTO TEST VALUES (ARRAY[1, NULL]), (ARRAY[1, 2]);
> update count: 2

SELECT ARRAY[1, 2] IN (SELECT A FROM TEST);
>> TRUE

SELECT ROW (ARRAY[1, 2]) IN (SELECT A FROM TEST);
>> TRUE

SELECT ARRAY[1, NULL] IN (SELECT A FROM TEST);
>> null

SELECT ROW (ARRAY[1, NULL]) IN (SELECT A FROM TEST);
>> null

-- Compatibility with H2 1.4.197 and older
SELECT A FROM TEST WHERE A = (1, 2);
>> [1, 2]

-- Compatibility with H2 1.4.197 and older
INSERT INTO TEST VALUES ((1, 3));
> update count: 1

DROP TABLE TEST;
> ok
