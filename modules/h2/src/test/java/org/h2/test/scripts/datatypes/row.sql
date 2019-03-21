-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT ROW (10);
>> ROW (10)

SELECT (10, 20, 30);
>> ROW (10, 20, 30)

SELECT (1, NULL) IS NOT DISTINCT FROM (1, NULL);
>> TRUE

SELECT (1, NULL) IS DISTINCT FROM ROW (1, NULL);
>> FALSE

SELECT (1, NULL) = (1, NULL);
>> null

SELECT (1, NULL) <> (1, NULL);
>> null

SELECT ROW (NULL) = (NULL, NULL);
> exception COLUMN_COUNT_DOES_NOT_MATCH

select (1, NULL, 2) = (1, NULL, 1);
>> FALSE

select (1, NULL, 2) <> (1, NULL, 1);
>> TRUE

SELECT (1, NULL) > (1, NULL);
>> null

SELECT (1, 2) > (1, NULL);
>> null

SELECT (1, 2, NULL) > (1, 1, NULL);
>> TRUE

SELECT (1, 1, NULL) > (1, 2, NULL);
>> FALSE

SELECT (1, 2, NULL) < (1, 1, NULL);
>> FALSE

SELECT (1, 1, NULL) <= (1, 1, NULL);
>> null

SELECT (1, 2) IN (SELECT 1, 2);
>> TRUE

SELECT (1, 2) IN (SELECT * FROM VALUES (1, 2), (1, NULL));
>> TRUE

SELECT (1, 2) IN (SELECT * FROM VALUES (1, 1), (1, NULL));
>> null

SELECT (1, 2) IN (SELECT * FROM VALUES (1, 1), (1, 3));
>> FALSE

SELECT (1, NULL) IN (SELECT 1, NULL);
>> null

SELECT (1, ARRAY[1]) IN (SELECT 1, ARRAY[1]);
>> TRUE

SELECT (1, ARRAY[1]) IN (SELECT 1, ARRAY[2]);
>> FALSE

SELECT (1, ARRAY[NULL]) IN (SELECT 1, ARRAY[NULL]);
>> null

-- The next tests should be at the of this file

SET MAX_MEMORY_ROWS = 2;
> ok

SELECT (X, X) FROM SYSTEM_RANGE(1, 100000) ORDER BY -X FETCH FIRST ROW ONLY;
>> ROW (100000, 100000)
