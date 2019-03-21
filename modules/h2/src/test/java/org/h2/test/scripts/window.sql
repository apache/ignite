-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID INT, R INT, CATEGORY INT);
> ok

INSERT INTO TEST VALUES
    (1, 4, 1),
    (2, 3, 1),
    (3, 2, 2),
    (4, 1, 2);
> update count: 4

SELECT *, ROW_NUMBER() OVER W FROM TEST;
> exception WINDOW_NOT_FOUND_1

SELECT * FROM TEST WINDOW W AS W1, W1 AS ();
> exception SYNTAX_ERROR_2

SELECT *, ROW_NUMBER() OVER W1, ROW_NUMBER() OVER W2 FROM TEST
    WINDOW W1 AS (W2 ORDER BY ID), W2 AS (PARTITION BY CATEGORY ORDER BY ID DESC);
> ID R CATEGORY ROW_NUMBER() OVER (PARTITION BY CATEGORY ORDER BY ID) ROW_NUMBER() OVER (PARTITION BY CATEGORY ORDER BY ID DESC)
> -- - -------- ----------------------------------------------------- ----------------------------------------------------------
> 1  4 1        1                                                     2
> 2  3 1        2                                                     1
> 3  2 2        1                                                     2
> 4  1 2        2                                                     1
> rows: 4

SELECT *, LAST_VALUE(ID) OVER W FROM TEST
    WINDOW W AS (PARTITION BY CATEGORY ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW);
> ID R CATEGORY LAST_VALUE(ID) OVER (PARTITION BY CATEGORY ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW)
> -- - -------- -------------------------------------------------------------------------------------------------------------------------------------
> 1  4 1        2
> 2  3 1        1
> 3  2 2        4
> 4  1 2        3
> rows: 4

DROP TABLE TEST;
> ok

SELECT MAX(MAX(X) OVER ()) OVER () FROM VALUES (1);
> exception INVALID_USE_OF_AGGREGATE_FUNCTION_1

SELECT MAX(MAX(X) OVER ()) FROM VALUES (1);
> exception INVALID_USE_OF_AGGREGATE_FUNCTION_1

SELECT MAX(MAX(X)) FROM VALUES (1);
> exception INVALID_USE_OF_AGGREGATE_FUNCTION_1

CREATE TABLE TEST(ID INT, CATEGORY INT);
> ok

INSERT INTO TEST VALUES
    (1, 1),
    (2, 1),
    (4, 2),
    (8, 2),
    (16, 3),
    (32, 3);
> update count: 6

SELECT ROW_NUMBER() OVER (ORDER  /**/ BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING SUM(ID) = 12;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> rows: 1

SELECT ROW_NUMBER() OVER (ORDER  /**/ BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING CATEGORY = 2;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> rows: 1

SELECT ROW_NUMBER() OVER (ORDER BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING CATEGORY > 1;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> 2                                     48
> rows: 2

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, CATEGORY BOOLEAN);
> ok

INSERT INTO TEST VALUES
    (1, FALSE),
    (2, FALSE),
    (4, TRUE),
    (8, TRUE),
    (16, FALSE),
    (32, FALSE);
> update count: 6

SELECT ROW_NUMBER() OVER (ORDER BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING SUM(ID) = 12;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> rows: 1

SELECT ROW_NUMBER() OVER (ORDER BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING CATEGORY;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> rows: 1

SELECT SUM(ID) OVER (ORDER BY ID ROWS NULL PRECEDING) P FROM TEST;
> exception INVALID_PRECEDING_OR_FOLLOWING_1

SELECT SUM(ID) OVER (ORDER BY ID RANGE NULL PRECEDING) P FROM TEST;
> exception INVALID_PRECEDING_OR_FOLLOWING_1

SELECT ARRAY_AGG(ID) OVER (ORDER BY V NULLS FIRST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) A,
    ID, V FROM VALUES (1, 1), (2, NULL), (3, 2) T(ID, V) ORDER BY V NULLS FIRST;
> A         ID V
> --------- -- ----
> [3, 1, 2] 2  null
> [3, 1]    1  1
> [3, 1]    3  2
> rows (ordered): 3

SELECT ARRAY_AGG(ID) OVER (ORDER BY V NULLS LAST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) A,
    ID, V FROM VALUES (1, 1), (2, NULL), (3, 2) T(ID, V) ORDER BY V NULLS LAST;
> A         ID V
> --------- -- ----
> [2, 3, 1] 1  1
> [2, 3, 1] 3  2
> [2]       2  null
> rows (ordered): 3

SELECT ARRAY_AGG(ID) OVER (ORDER BY V NULLS FIRST RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) A,
    ID, V FROM VALUES (1, 1), (2, NULL), (3, 2) T(ID, V) ORDER BY V NULLS FIRST;
> A         ID V
> --------- -- ----
> [3, 1, 2] 2  null
> [3]       1  1
> null      3  2
> rows (ordered): 3

SELECT ARRAY_AGG(ID) OVER (ORDER BY V NULLS LAST RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) A,
    ID, V FROM VALUES (1, 1), (2, NULL), (3, 2) T(ID, V) ORDER BY V NULLS LAST;
> A      ID V
> ------ -- ----
> [2, 3] 1  1
> [2]    3  2
> [2]    2  null
> rows (ordered): 3

SELECT ARRAY_AGG(ID) OVER (ORDER BY V NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) A,
    ID, V FROM VALUES (1, 1), (2, NULL), (3, 2) T(ID, V) ORDER BY V NULLS FIRST;
> A         ID V
> --------- -- ----
> [2]       2  null
> [2, 1, 3] 1  1
> [2, 1, 3] 3  2
> rows (ordered): 3

SELECT ARRAY_AGG(ID) OVER (ORDER BY V NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) A,
    ID, V FROM VALUES (1, 1), (2, NULL), (3, 2) T(ID, V) ORDER BY V NULLS LAST;
> A         ID V
> --------- -- ----
> [1, 3]    1  1
> [1, 3]    3  2
> [1, 3, 2] 2  null
> rows (ordered): 3

SELECT ARRAY_AGG(ID) OVER (ORDER BY V NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) A,
    ID, V FROM VALUES (1, 1), (2, NULL), (3, 2) T(ID, V) ORDER BY V NULLS FIRST;
> A      ID V
> ------ -- ----
> [2]    2  null
> [2]    1  1
> [2, 1] 3  2
> rows (ordered): 3

SELECT ARRAY_AGG(ID) OVER (ORDER BY V NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) A,
    ID, V FROM VALUES (1, 1), (2, NULL), (3, 2) T(ID, V) ORDER BY V NULLS LAST;
> A         ID V
> --------- -- ----
> null      1  1
> [1]       3  2
> [1, 3, 2] 2  null
> rows (ordered): 3

SELECT SUM(V) OVER (ORDER BY V RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM VALUES (TRUE) T(V);
> exception INVALID_VALUE_2

SELECT
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN 10000000000 PRECEDING AND CURRENT ROW) P,
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN 10000000001 PRECEDING AND 10000000000 PRECEDING) P2,
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN CURRENT ROW AND 2147483647 FOLLOWING) F,
    SUM(ID) OVER (ORDER BY ID RANGE BETWEEN 2147483647 FOLLOWING AND 2147483648 FOLLOWING) F2,
    ID FROM TEST ORDER BY ID;
> P  P2   F  F2   ID
> -- ---- -- ---- --
> 1  null 63 null 1
> 3  null 62 null 2
> 7  null 60 null 4
> 15 null 56 null 8
> 31 null 48 null 16
> 63 null 32 null 32
> rows (ordered): 6

DROP TABLE TEST;
> ok

SELECT
    ARRAY_AGG(T) OVER (ORDER BY T RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) C,
    ARRAY_AGG(T) OVER (ORDER BY T RANGE BETWEEN INTERVAL 2 HOUR PRECEDING AND INTERVAL 1 HOUR PRECEDING) P,
    T FROM VALUES (TIME '00:00:00'), (TIME '01:30:00') TEST(T) ORDER BY T;
> C                    P          T
> -------------------- ---------- --------
> [00:00:00]           null       00:00:00
> [00:00:00, 01:30:00] [00:00:00] 01:30:00
> rows (ordered): 2

SELECT SUM(A) OVER (ORDER BY A, B RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) S FROM VALUES (1, 2) T(A, B);
>> 1

SELECT SUM(A) OVER (ORDER BY A, B RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) S FROM VALUES (1, 2) T(A, B);
> exception SYNTAX_ERROR_2

SELECT SUM(A) OVER (ORDER BY A, B RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) S FROM VALUES (1, 2) T(A, B);
> exception SYNTAX_ERROR_2
