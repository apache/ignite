-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT FIRST_VALUE(1) OVER (PARTITION BY ID);
> exception COLUMN_NOT_FOUND_1

SELECT FIRST_VALUE(1) OVER (ORDER BY ID);
> exception COLUMN_NOT_FOUND_1

CREATE TABLE TEST (ID INT PRIMARY KEY, CATEGORY INT, VALUE INT);
> ok

INSERT INTO TEST VALUES
    (1, 1, NULL),
    (2, 1, 12),
    (3, 1, NULL),
    (4, 1, 13),
    (5, 1, NULL),
    (6, 1, 13),
    (7, 2, 21),
    (8, 2, 22),
    (9, 3, 31),
    (10, 3, 32),
    (11, 3, 33),
    (12, 4, 41),
    (13, 4, NULL);
> update count: 13

SELECT *,
    FIRST_VALUE(VALUE) OVER (ORDER BY ID) FIRST,
    FIRST_VALUE(VALUE) RESPECT NULLS OVER (ORDER BY ID) FIRST_N,
    FIRST_VALUE(VALUE) IGNORE NULLS OVER (ORDER BY ID) FIRST_NN,
    LAST_VALUE(VALUE) OVER (ORDER BY ID) LAST,
    LAST_VALUE(VALUE) RESPECT NULLS OVER (ORDER BY ID) LAST_N,
    LAST_VALUE(VALUE) IGNORE NULLS OVER (ORDER BY ID) LAST_NN
    FROM TEST FETCH FIRST 6 ROWS ONLY;
> ID CATEGORY VALUE FIRST FIRST_N FIRST_NN LAST LAST_N LAST_NN
> -- -------- ----- ----- ------- -------- ---- ------ -------
> 1  1        null  null  null    null     null null   null
> 2  1        12    null  null    12       12   12     12
> 3  1        null  null  null    12       null null   12
> 4  1        13    null  null    12       13   13     13
> 5  1        null  null  null    12       null null   13
> 6  1        13    null  null    12       13   13     13
> rows: 6

SELECT *,
    FIRST_VALUE(VALUE) OVER (ORDER BY ID) FIRST,
    FIRST_VALUE(VALUE) RESPECT NULLS OVER (ORDER BY ID) FIRST_N,
    FIRST_VALUE(VALUE) IGNORE NULLS OVER (ORDER BY ID) FIRST_NN,
    LAST_VALUE(VALUE) OVER (ORDER BY ID) LAST,
    LAST_VALUE(VALUE) RESPECT NULLS OVER (ORDER BY ID) LAST_N,
    LAST_VALUE(VALUE) IGNORE NULLS OVER (ORDER BY ID) LAST_NN
    FROM TEST WHERE ID > 1 FETCH FIRST 3 ROWS ONLY;
> ID CATEGORY VALUE FIRST FIRST_N FIRST_NN LAST LAST_N LAST_NN
> -- -------- ----- ----- ------- -------- ---- ------ -------
> 2  1        12    12    12      12       12   12     12
> 3  1        null  12    12      12       null null   12
> 4  1        13    12    12      12       13   13     13
> rows: 3

SELECT *,
    NTH_VALUE(VALUE, 2) OVER (ORDER BY ID) NTH,
    NTH_VALUE(VALUE, 2) FROM FIRST OVER (ORDER BY ID) NTH_FF,
    NTH_VALUE(VALUE, 2) FROM LAST OVER (ORDER BY ID) NTH_FL,
    NTH_VALUE(VALUE, 2) RESPECT NULLS OVER (ORDER BY ID) NTH_N,
    NTH_VALUE(VALUE, 2) FROM FIRST RESPECT NULLS OVER (ORDER BY ID) NTH_FF_N,
    NTH_VALUE(VALUE, 2) FROM LAST RESPECT NULLS OVER (ORDER BY ID) NTH_FL_N,
    NTH_VALUE(VALUE, 2) IGNORE NULLS OVER (ORDER BY ID) NTH_NN,
    NTH_VALUE(VALUE, 2) FROM FIRST IGNORE NULLS OVER (ORDER BY ID) NTH_FF_NN,
    NTH_VALUE(VALUE, 2) FROM LAST IGNORE NULLS OVER (ORDER BY ID) NTH_FL_NN
    FROM TEST FETCH FIRST 6 ROWS ONLY;
> ID CATEGORY VALUE NTH  NTH_FF NTH_FL NTH_N NTH_FF_N NTH_FL_N NTH_NN NTH_FF_NN NTH_FL_NN
> -- -------- ----- ---- ------ ------ ----- -------- -------- ------ --------- ---------
> 1  1        null  null null   null   null  null     null     null   null      null
> 2  1        12    12   12     null   12    12       null     null   null      null
> 3  1        null  12   12     12     12    12       12       null   null      null
> 4  1        13    12   12     null   12    12       null     13     13        12
> 5  1        null  12   12     13     12    12       13       13     13        12
> 6  1        13    12   12     null   12    12       null     13     13        13
> rows: 6

SELECT *,
    NTH_VALUE(VALUE, 2) OVER(ORDER BY ID) F,
    NTH_VALUE(VALUE, 2) OVER(ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) F_U_C,
    NTH_VALUE(VALUE, 2) OVER(ORDER BY ID RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) F_C_U,
    NTH_VALUE(VALUE, 2) OVER(ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) F_U_U,
    NTH_VALUE(VALUE, 2) FROM LAST OVER(ORDER BY ID) L,
    NTH_VALUE(VALUE, 2) FROM LAST OVER(ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) L_U_C,
    NTH_VALUE(VALUE, 2) FROM LAST OVER(ORDER BY ID RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) L_C_U,
    NTH_VALUE(VALUE, 2) FROM LAST OVER(ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) L_U_U
    FROM TEST ORDER BY ID;
> ID CATEGORY VALUE F    F_U_C F_C_U F_U_U L    L_U_C L_C_U L_U_U
> -- -------- ----- ---- ----- ----- ----- ---- ----- ----- -----
> 1  1        null  null null  12    12    null null  41    41
> 2  1        12    12   12    null  12    null null  41    41
> 3  1        null  12   12    13    12    12   12    41    41
> 4  1        13    12   12    null  12    null null  41    41
> 5  1        null  12   12    13    12    13   13    41    41
> 6  1        13    12   12    21    12    null null  41    41
> 7  2        21    12   12    22    12    13   13    41    41
> 8  2        22    12   12    31    12    21   21    41    41
> 9  3        31    12   12    32    12    22   22    41    41
> 10 3        32    12   12    33    12    31   31    41    41
> 11 3        33    12   12    41    12    32   32    41    41
> 12 4        41    12   12    null  12    33   33    41    41
> 13 4        null  12   12    null  12    41   41    null  41
> rows (ordered): 13

SELECT NTH_VALUE(VALUE, 0) OVER (ORDER BY ID) FROM TEST;
> exception INVALID_VALUE_2

SELECT *,
    FIRST_VALUE(VALUE) OVER (PARTITION BY CATEGORY ORDER BY ID) FIRST,
    LAST_VALUE(VALUE) OVER (PARTITION BY CATEGORY ORDER BY ID) LAST,
    NTH_VALUE(VALUE, 2) OVER (PARTITION BY CATEGORY ORDER BY ID) NTH
    FROM TEST ORDER BY ID;
> ID CATEGORY VALUE FIRST LAST NTH
> -- -------- ----- ----- ---- ----
> 1  1        null  null  null null
> 2  1        12    null  12   12
> 3  1        null  null  null 12
> 4  1        13    null  13   12
> 5  1        null  null  null 12
> 6  1        13    null  13   12
> 7  2        21    21    21   null
> 8  2        22    21    22   22
> 9  3        31    31    31   null
> 10 3        32    31    32   32
> 11 3        33    31    33   32
> 12 4        41    41    41   null
> 13 4        null  41    null null
> rows (ordered): 13

SELECT ID, CATEGORY,
    NTH_VALUE(CATEGORY, 2) OVER (ORDER BY CATEGORY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) C,
    NTH_VALUE(CATEGORY, 2) OVER (ORDER BY CATEGORY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)
    FROM TEST FETCH FIRST 3 ROWS ONLY;
> ID CATEGORY C    NTH_VALUE(CATEGORY, 2) OVER (ORDER BY CATEGORY ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW)
> -- -------- ---- --------------------------------------------------------------------------------------------
> 1  1        null null
> 2  1        1    null
> 3  1        1    1
> rows: 3

SELECT ID, CATEGORY,
    NTH_VALUE(CATEGORY, 2) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) C2,
    NTH_VALUE(CATEGORY, 3) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) C3,
    NTH_VALUE(CATEGORY, 2) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW)
    FROM TEST OFFSET 10 ROWS;
> ID CATEGORY C2 C3   NTH_VALUE(CATEGORY, 2) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN CURRENT_ROW AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW)
> -- -------- -- ---- -------------------------------------------------------------------------------------------------------------------------------
> 11 3        4  3    4
> 12 4        4  null null
> 13 4        4  null null
> rows: 3

SELECT ID, CATEGORY,
    NTH_VALUE(CATEGORY, 2) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) C
    FROM TEST OFFSET 10 ROWS;
> ID CATEGORY C
> -- -------- -
> 11 3        4
> 12 4        3
> 13 4        3
> rows: 3

SELECT ID, CATEGORY,
    NTH_VALUE(CATEGORY, 1) OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) F1,
    NTH_VALUE(CATEGORY, 2) OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) F2,
    NTH_VALUE(CATEGORY, 5) OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) F5,
    NTH_VALUE(CATEGORY, 5) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) L5,
    NTH_VALUE(CATEGORY, 2) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) L2,
    NTH_VALUE(CATEGORY, 1) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) L1
    FROM TEST ORDER BY ID;
> ID CATEGORY F1 F2 F5 L5 L2 L1
> -- -------- -- -- -- -- -- --
> 1  1        2  2  3  3  4  4
> 2  1        2  2  3  3  4  4
> 3  1        2  2  3  3  4  4
> 4  1        2  2  3  3  4  4
> 5  1        2  2  3  3  4  4
> 6  1        2  2  3  3  4  4
> 7  2        1  1  1  3  4  4
> 8  2        1  1  1  3  4  4
> 9  3        1  1  1  1  4  4
> 10 3        1  1  1  1  4  4
> 11 3        1  1  1  1  4  4
> 12 4        1  1  1  2  3  3
> 13 4        1  1  1  2  3  3
> rows (ordered): 13

SELECT ID, CATEGORY,
    NTH_VALUE(CATEGORY, 1) OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) F1,
    NTH_VALUE(CATEGORY, 2) OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) F2,
    NTH_VALUE(CATEGORY, 5) OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) F5,
    NTH_VALUE(CATEGORY, 5) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) L5,
    NTH_VALUE(CATEGORY, 2) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) L2,
    NTH_VALUE(CATEGORY, 1) FROM LAST OVER (ORDER BY CATEGORY RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) L1
    FROM TEST ORDER BY ID;
> ID CATEGORY F1 F2 F5 L5 L2 L1
> -- -------- -- -- -- -- -- --
> 1  1        1  2  3  3  4  4
> 2  1        1  2  3  3  4  4
> 3  1        1  2  3  3  4  4
> 4  1        1  2  3  3  4  4
> 5  1        1  2  3  3  4  4
> 6  1        1  2  3  3  4  4
> 7  2        1  1  1  3  4  4
> 8  2        1  1  1  3  4  4
> 9  3        1  1  1  2  4  4
> 10 3        1  1  1  2  4  4
> 11 3        1  1  1  2  4  4
> 12 4        1  1  1  2  3  4
> 13 4        1  1  1  2  3  4
> rows (ordered): 13

SELECT ID, CATEGORY,
    FIRST_VALUE(ID) OVER (ORDER BY ID ROWS BETWEEN CATEGORY FOLLOWING AND UNBOUNDED FOLLOWING) F,
    LAST_VALUE(ID) OVER (ORDER BY ID ROWS BETWEEN CURRENT ROW AND CATEGORY FOLLOWING) L,
    NTH_VALUE(ID, 2) OVER (ORDER BY ID ROWS BETWEEN CATEGORY FOLLOWING AND UNBOUNDED FOLLOWING) N
    FROM TEST ORDER BY ID;
> ID CATEGORY F    L  N
> -- -------- ---- -- ----
> 1  1        2    2  3
> 2  1        3    3  4
> 3  1        4    4  5
> 4  1        5    5  6
> 5  1        6    6  7
> 6  1        7    7  8
> 7  2        9    9  10
> 8  2        10   10 11
> 9  3        12   12 13
> 10 3        13   13 null
> 11 3        null 13 null
> 12 4        null 13 null
> 13 4        null 13 null
> rows (ordered): 13

DROP TABLE TEST;
> ok

SELECT I, X, LAST_VALUE(I) OVER (ORDER BY X) L FROM VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3) V(I, X);
> I X L
> - - -
> 1 1 2
> 2 1 2
> 3 2 4
> 4 2 4
> 5 3 5
> rows: 5

SELECT A, MAX(B) M, FIRST_VALUE(A) OVER (ORDER BY A ROWS BETWEEN MAX(B) - 1 FOLLOWING AND UNBOUNDED FOLLOWING) F
    FROM VALUES (1, 1), (1, 1), (2, 1), (2, 2), (3, 1) V(A, B)
    GROUP BY A;
> A M F
> - - -
> 1 1 1
> 2 2 3
> 3 1 3
> rows: 3
