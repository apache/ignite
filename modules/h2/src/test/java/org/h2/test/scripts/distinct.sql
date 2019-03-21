-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID BIGINT, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES (1, 'a'), (2, 'B'), (3, 'c'), (1, 'a');
> update count: 4

CREATE TABLE TEST2(ID2 BIGINT);
> ok

INSERT INTO TEST2 VALUES (1), (2);
> update count: 2

SELECT DISTINCT NAME FROM TEST ORDER BY NAME;
> NAME
> ----
> B
> a
> c
> rows (ordered): 3

SELECT DISTINCT NAME FROM TEST ORDER BY LOWER(NAME);
> NAME
> ----
> a
> B
> c
> rows (ordered): 3

SELECT DISTINCT ID FROM TEST ORDER BY ID;
> ID
> --
> 1
> 2
> 3
> rows (ordered): 3

SELECT DISTINCT ID FROM TEST ORDER BY -ID - 1;
> ID
> --
> 3
> 2
> 1
> rows (ordered): 3

SELECT DISTINCT ID FROM TEST ORDER BY (-ID + 10) > 0 AND NOT (ID = 0), ID;
> ID
> --
> 1
> 2
> 3
> rows (ordered): 3

SELECT DISTINCT NAME, ID + 1 FROM TEST ORDER BY UPPER(NAME) || (ID + 1);
> NAME ID + 1
> ---- ------
> a    2
> B    3
> c    4
> rows (ordered): 3

SELECT DISTINCT ID FROM TEST ORDER BY NAME;
> exception ORDER_BY_NOT_IN_RESULT

SELECT DISTINCT ID FROM TEST ORDER BY UPPER(NAME);
> exception ORDER_BY_NOT_IN_RESULT

SELECT DISTINCT ID FROM TEST ORDER BY CURRENT_TIMESTAMP;
> exception ORDER_BY_NOT_IN_RESULT

SET MODE MySQL;
> ok

SELECT DISTINCT ID FROM TEST ORDER BY NAME;
> ID
> --
> 2
> 1
> 3
> rows (ordered): 3

SELECT DISTINCT ID FROM TEST ORDER BY LOWER(NAME);
> ID
> --
> 1
> 2
> 3
> rows (ordered): 3

SELECT DISTINCT ID FROM TEST JOIN TEST2 ON ID = ID2 ORDER BY LOWER(NAME);
> ID
> --
> 1
> 2
> rows (ordered): 2

SET MODE Regular;
> ok

DROP TABLE TEST;
> ok

DROP TABLE TEST2;
> ok

CREATE TABLE TEST(C1 INT, C2 INT, C3 INT, C4 INT, C5 INT);
> ok

INSERT INTO TEST VALUES(1, 2, 3, 4, 5), (1, 2, 3, 6, 7), (2, 1, 4, 8, 9), (3, 4, 5, 1, 1);
> update count: 4

SELECT DISTINCT ON(C1, C2) C1, C2, C3, C4, C5 FROM TEST;
> C1 C2 C3 C4 C5
> -- -- -- -- --
> 1  2  3  4  5
> 2  1  4  8  9
> 3  4  5  1  1
> rows: 3

SELECT DISTINCT ON(C1 + C2) C1, C2, C3, C4, C5 FROM TEST;
> C1 C2 C3 C4 C5
> -- -- -- -- --
> 1  2  3  4  5
> 3  4  5  1  1
> rows: 2

SELECT DISTINCT ON(C1 + C2, C3) C1, C2, C3, C4, C5 FROM TEST;
> C1 C2 C3 C4 C5
> -- -- -- -- --
> 1  2  3  4  5
> 2  1  4  8  9
> 3  4  5  1  1
> rows: 3

SELECT DISTINCT ON(C1) C2 FROM TEST ORDER BY C1;
> C2
> --
> 2
> 1
> 4
> rows (ordered): 3

SELECT DISTINCT ON(C1) C1, C4, C5 FROM TEST ORDER BY C1, C5;
> C1 C4 C5
> -- -- --
> 1  4  5
> 2  8  9
> 3  1  1
> rows (ordered): 3

SELECT DISTINCT ON(C1) C1, C4, C5 FROM TEST ORDER BY C1, C5 DESC;
> C1 C4 C5
> -- -- --
> 1  6  7
> 2  8  9
> 3  1  1
> rows (ordered): 3

SELECT T1.C1, T2.C5 FROM TEST T1 JOIN (
    SELECT DISTINCT ON(C1) C1, C4, C5 FROM TEST ORDER BY C1, C5
) T2 ON T1.C4 = T2.C4 ORDER BY T1.C1;
> C1 C5
> -- --
> 1  5
> 2  9
> 3  1
> rows (ordered): 3

SELECT T1.C1, T2.C5 FROM TEST T1 JOIN (
    SELECT DISTINCT ON(C1) C1, C4, C5 FROM TEST ORDER BY C1, C5 DESC
) T2 ON T1.C4 = T2.C4 ORDER BY T1.C1;
> C1 C5
> -- --
> 1  7
> 2  9
> 3  1
> rows (ordered): 3

EXPLAIN SELECT DISTINCT ON(C1) C2 FROM TEST ORDER BY C1;
>> SELECT DISTINCT ON("C1") "C2" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ ORDER BY ="C1"

SELECT DISTINCT ON(C1) C2 FROM TEST ORDER BY C3;
> exception ORDER_BY_NOT_IN_RESULT

DROP TABLE TEST;
> ok
