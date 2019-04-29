-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select array_contains(ARRAY[4.0, 2.0, 2.0], 2.0);
>> TRUE

select array_contains(ARRAY[4.0, 2.0, 2.0], 5.0);
>> FALSE

select array_contains(ARRAY['one', 'two'], 'one');
>> TRUE

select array_contains(ARRAY['one', 'two'], 'xxx');
>> FALSE

select array_contains(ARRAY['one', 'two'], null);
>> FALSE

select array_contains(ARRAY[null, 'two'], null);
>> TRUE

select array_contains(null, 'one');
>> null

select array_contains(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[1, 2]);
>> TRUE

select array_contains(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[5, 6]);
>> FALSE

CREATE TABLE TEST (ID INT PRIMARY KEY AUTO_INCREMENT, A ARRAY);
> ok

INSERT INTO TEST (A) VALUES (ARRAY[1L, 2L]), (ARRAY[3L, 4L]);
> update count: 2

SELECT ID, ARRAY_CONTAINS(A, 1L), ARRAY_CONTAINS(A, 2L), ARRAY_CONTAINS(A, 3L), ARRAY_CONTAINS(A, 4L) FROM TEST;
> ID ARRAY_CONTAINS(A, 1) ARRAY_CONTAINS(A, 2) ARRAY_CONTAINS(A, 3) ARRAY_CONTAINS(A, 4)
> -- -------------------- -------------------- -------------------- --------------------
> 1  TRUE                 TRUE                 FALSE                FALSE
> 2  FALSE                FALSE                TRUE                 TRUE
> rows: 2

SELECT * FROM (
    SELECT ID, ARRAY_CONTAINS(A, 1L), ARRAY_CONTAINS(A, 2L), ARRAY_CONTAINS(A, 3L), ARRAY_CONTAINS(A, 4L) FROM TEST
);
> ID ARRAY_CONTAINS(A, 1) ARRAY_CONTAINS(A, 2) ARRAY_CONTAINS(A, 3) ARRAY_CONTAINS(A, 4)
> -- -------------------- -------------------- -------------------- --------------------
> 1  TRUE                 TRUE                 FALSE                FALSE
> 2  FALSE                FALSE                TRUE                 TRUE
> rows: 2

DROP TABLE TEST;
> ok
