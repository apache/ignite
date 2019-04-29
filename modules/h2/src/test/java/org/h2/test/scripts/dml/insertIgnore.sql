-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SET MODE MySQL;
> ok

CREATE TABLE TEST(ID BIGINT PRIMARY KEY, VALUE INT NOT NULL);
> ok

INSERT INTO TEST VALUES (1, 10), (2, 20), (3, 30), (4, 40);
> update count: 4

INSERT INTO TEST VALUES (3, 31), (5, 51);
> exception DUPLICATE_KEY_1

SELECT * FROM TEST ORDER BY ID;
> ID VALUE
> -- -----
> 1  10
> 2  20
> 3  30
> 4  40
> rows (ordered): 4

INSERT IGNORE INTO TEST VALUES (3, 32), (5, 52);
> update count: 1

INSERT IGNORE INTO TEST VALUES (4, 43);
> ok

SELECT * FROM TEST ORDER BY ID;
> ID VALUE
> -- -----
> 1  10
> 2  20
> 3  30
> 4  40
> 5  52
> rows (ordered): 5

CREATE TABLE TESTREF(ID BIGINT PRIMARY KEY, VALUE INT NOT NULL);
> ok

INSERT INTO TESTREF VALUES (1, 11), (2, 21), (6, 61), (7, 71);
> update count: 4

INSERT INTO TEST (ID, VALUE) SELECT ID, VALUE FROM TESTREF;
> exception DUPLICATE_KEY_1

SELECT * FROM TEST ORDER BY ID;
> ID VALUE
> -- -----
> 1  10
> 2  20
> 3  30
> 4  40
> 5  52
> rows (ordered): 5

INSERT IGNORE INTO TEST (ID, VALUE) SELECT ID, VALUE FROM TESTREF;
> update count: 2

INSERT IGNORE INTO TEST (ID, VALUE) SELECT ID, VALUE FROM TESTREF;
> ok

SELECT * FROM TEST ORDER BY ID;
> ID VALUE
> -- -----
> 1  10
> 2  20
> 3  30
> 4  40
> 5  52
> 6  61
> 7  71
> rows (ordered): 7

INSERT INTO TESTREF VALUES (8, 81), (9, 91);
> update count: 2

INSERT INTO TEST (ID, VALUE) SELECT ID, VALUE FROM TESTREF ON DUPLICATE KEY UPDATE VALUE=83;
> update count: 10

SELECT * FROM TEST ORDER BY ID;
> ID VALUE
> -- -----
> 1  83
> 2  83
> 3  30
> 4  40
> 5  52
> 6  83
> 7  83
> 8  81
> 9  91
> rows (ordered): 9
