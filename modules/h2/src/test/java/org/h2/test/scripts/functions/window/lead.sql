-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST (ID INT PRIMARY KEY, VALUE INT);
> ok

INSERT INTO TEST VALUES
    (1, NULL),
    (2, 12),
    (3, NULL),
    (4, 13),
    (5, NULL),
    (6, 21),
    (7, 22),
    (8, 33),
    (9, NULL);
> update count: 9

SELECT *,
    LEAD(VALUE) OVER (ORDER BY ID) LD,
    LEAD(VALUE) RESPECT NULLS OVER (ORDER BY ID) LD_N,
    LEAD(VALUE) IGNORE NULLS OVER (ORDER BY ID) LD_NN,
    LAG(VALUE) OVER (ORDER BY ID) LG,
    LAG(VALUE) RESPECT NULLS OVER (ORDER BY ID) LG_N,
    LAG(VALUE) IGNORE NULLS OVER (ORDER BY ID) LG_NN
    FROM TEST;
> ID VALUE LD   LD_N LD_NN LG   LG_N LG_NN
> -- ----- ---- ---- ----- ---- ---- -----
> 1  null  12   12   12    null null null
> 2  12    null null 13    null null null
> 3  null  13   13   13    12   12   12
> 4  13    null null 21    null null 12
> 5  null  21   21   21    13   13   13
> 6  21    22   22   22    null null 13
> 7  22    33   33   33    21   21   21
> 8  33    null null null  22   22   22
> 9  null  null null null  33   33   33
> rows: 9

SELECT *,
    LEAD(VALUE, 1) OVER (ORDER BY ID) LD,
    LEAD(VALUE, 1) RESPECT NULLS OVER (ORDER BY ID) LD_N,
    LEAD(VALUE, 1) IGNORE NULLS OVER (ORDER BY ID) LD_NN,
    LAG(VALUE, 1) OVER (ORDER BY ID) LG,
    LAG(VALUE, 1) RESPECT NULLS OVER (ORDER BY ID) LG_N,
    LAG(VALUE, 1) IGNORE NULLS OVER (ORDER BY ID) LG_NN
    FROM TEST;
> ID VALUE LD   LD_N LD_NN LG   LG_N LG_NN
> -- ----- ---- ---- ----- ---- ---- -----
> 1  null  12   12   12    null null null
> 2  12    null null 13    null null null
> 3  null  13   13   13    12   12   12
> 4  13    null null 21    null null 12
> 5  null  21   21   21    13   13   13
> 6  21    22   22   22    null null 13
> 7  22    33   33   33    21   21   21
> 8  33    null null null  22   22   22
> 9  null  null null null  33   33   33
> rows: 9

SELECT *,
    LEAD(VALUE, 0) OVER (ORDER BY ID) LD,
    LEAD(VALUE, 0) RESPECT NULLS OVER (ORDER BY ID) LD_N,
    LEAD(VALUE, 0) IGNORE NULLS OVER (ORDER BY ID) LD_NN,
    LAG(VALUE, 0) OVER (ORDER BY ID) LG,
    LAG(VALUE, 0) RESPECT NULLS OVER (ORDER BY ID) LG_N,
    LAG(VALUE, 0) IGNORE NULLS OVER (ORDER BY ID) LG_NN
    FROM TEST;
> ID VALUE LD   LD_N LD_NN LG   LG_N LG_NN
> -- ----- ---- ---- ----- ---- ---- -----
> 1  null  null null null  null null null
> 2  12    12   12   12    12   12   12
> 3  null  null null null  null null null
> 4  13    13   13   13    13   13   13
> 5  null  null null null  null null null
> 6  21    21   21   21    21   21   21
> 7  22    22   22   22    22   22   22
> 8  33    33   33   33    33   33   33
> 9  null  null null null  null null null
> rows: 9

SELECT *,
    LEAD(VALUE, 2) OVER (ORDER BY ID) LD,
    LEAD(VALUE, 2) RESPECT NULLS OVER (ORDER BY ID) LD_N,
    LEAD(VALUE, 2) IGNORE NULLS OVER (ORDER BY ID) LD_NN,
    LAG(VALUE, 2) OVER (ORDER BY ID) LG,
    LAG(VALUE, 2) RESPECT NULLS OVER (ORDER BY ID) LG_N,
    LAG(VALUE, 2) IGNORE NULLS OVER (ORDER BY ID) LG_NN
    FROM TEST;
> ID VALUE LD   LD_N LD_NN LG   LG_N LG_NN
> -- ----- ---- ---- ----- ---- ---- -----
> 1  null  null null 13    null null null
> 2  12    13   13   21    null null null
> 3  null  null null 21    null null null
> 4  13    21   21   22    12   12   null
> 5  null  22   22   22    null null 12
> 6  21    33   33   33    13   13   12
> 7  22    null null null  null null 13
> 8  33    null null null  21   21   21
> 9  null  null null null  22   22   22
> rows: 9

SELECT *,
    LEAD(VALUE, 2, 1111.0) OVER (ORDER BY ID) LD,
    LEAD(VALUE, 2, 1111.0) RESPECT NULLS OVER (ORDER BY ID) LD_N,
    LEAD(VALUE, 2, 1111.0) IGNORE NULLS OVER (ORDER BY ID) LD_NN,
    LAG(VALUE, 2, 1111.0) OVER (ORDER BY ID) LG,
    LAG(VALUE, 2, 1111.0) RESPECT NULLS OVER (ORDER BY ID) LG_N,
    LAG(VALUE, 2, 1111.0) IGNORE NULLS OVER (ORDER BY ID) LG_NN
    FROM TEST;
> ID VALUE LD   LD_N LD_NN LG   LG_N LG_NN
> -- ----- ---- ---- ----- ---- ---- -----
> 1  null  null null 13    1111 1111 1111
> 2  12    13   13   21    1111 1111 1111
> 3  null  null null 21    null null 1111
> 4  13    21   21   22    12   12   1111
> 5  null  22   22   22    null null 12
> 6  21    33   33   33    13   13   12
> 7  22    null null 1111  null null 13
> 8  33    1111 1111 1111  21   21   21
> 9  null  1111 1111 1111  22   22   22
> rows: 9

SELECT LEAD(VALUE, -1) OVER (ORDER BY ID) FROM TEST;
> exception INVALID_VALUE_2

SELECT LAG(VALUE, -1) OVER (ORDER BY ID) FROM TEST;
> exception INVALID_VALUE_2

SELECT LEAD(VALUE) OVER () FROM TEST;
> exception SYNTAX_ERROR_2

SELECT LAG(VALUE) OVER () FROM TEST;
> exception SYNTAX_ERROR_2

SELECT LEAD(VALUE) OVER (ORDER BY ID RANGE CURRENT ROW) FROM TEST;
> exception SYNTAX_ERROR_1

SELECT LAG(VALUE) OVER (ORDER BY ID RANGE CURRENT ROW) FROM TEST;
> exception SYNTAX_ERROR_1

DROP TABLE TEST;
> ok

SELECT C, SUM(I) S, LEAD(SUM(I)) OVER (ORDER BY SUM(I)) L FROM
    VALUES (1, 1), (2, 1), (4, 2), (8, 2) T(I, C) GROUP BY C;
> C S  L
> - -- ----
> 1 3  12
> 2 12 null
> rows: 2
