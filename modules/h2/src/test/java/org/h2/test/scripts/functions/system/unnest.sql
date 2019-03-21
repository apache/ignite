-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT * FROM UNNEST();
> exception INVALID_PARAMETER_COUNT_2

SELECT * FROM UNNEST(ARRAY[]);
> C1
> --
> rows: 0

SELECT * FROM UNNEST(ARRAY[1, 2, 3]);
> C1
> --
> 1
> 2
> 3
> rows: 3

SELECT * FROM UNNEST(ARRAY[1], ARRAY[2, 3, 4], ARRAY[5, 6]);
> C1   C2 C3
> ---- -- ----
> 1    2  5
> null 3  6
> null 4  null
> rows: 3

SELECT * FROM UNNEST(ARRAY[1], ARRAY[2, 3, 4], ARRAY[5, 6]) WITH ORDINALITY;
> C1   C2 C3   NORD
> ---- -- ---- ----
> 1    2  5    1
> null 3  6    2
> null 4  null 3
> rows: 3

EXPLAIN SELECT * FROM UNNEST(ARRAY[1]);
>> SELECT "UNNEST"."C1" FROM UNNEST(ARRAY [1]) /* function */

EXPLAIN SELECT * FROM UNNEST(ARRAY[1]) WITH ORDINALITY;
>> SELECT "UNNEST"."C1", "UNNEST"."NORD" FROM UNNEST(ARRAY [1]) WITH ORDINALITY /* function */

SELECT 1 IN(UNNEST(ARRAY[1, 2, 3]));
>> TRUE

SELECT 4 IN(UNNEST(ARRAY[1, 2, 3]));
>> FALSE

SELECT X, X IN(UNNEST(ARRAY[2, 4])) FROM SYSTEM_RANGE(1, 5);
> X X IN(2, 4)
> - ----------
> 1 FALSE
> 2 TRUE
> 3 FALSE
> 4 TRUE
> 5 FALSE
> rows: 5

SELECT X, X IN(UNNEST(?)) FROM SYSTEM_RANGE(1, 5);
{
2
> X X = ANY(?1)
> - -----------
> 1 FALSE
> 2 TRUE
> 3 FALSE
> 4 FALSE
> 5 FALSE
> rows: 5
};
> update count: 0

CREATE TABLE TEST(A INT, B ARRAY);
> ok

INSERT INTO TEST VALUES (2, ARRAY[2, 4]), (3, ARRAY[2, 5]);
> update count: 2

SELECT A, B, A IN(UNNEST(B)) FROM TEST;
> A B      A IN(UNNEST(B))
> - ------ ---------------
> 2 [2, 4] TRUE
> 3 [2, 5] FALSE
> rows: 2

DROP TABLE TEST;
> ok
