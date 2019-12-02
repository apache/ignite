-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE SEQUENCE SEQ;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, V INT DEFAULT NEXT VALUE FOR SEQ ON UPDATE 1000 * NEXT VALUE FOR SEQ);
> ok

INSERT INTO TEST(ID) VALUES (1), (2);
> update count: 2

SELECT * FROM TEST ORDER BY ID;
> ID V
> -- -
> 1  1
> 2  2
> rows (ordered): 2

UPDATE TEST SET ID = 3 WHERE ID = 2;
> update count: 1

SELECT * FROM TEST ORDER BY ID;
> ID V
> -- ----
> 1  1
> 3  3000
> rows (ordered): 2


UPDATE TEST SET V = 3 WHERE ID = 3;
> update count: 1

SELECT * FROM TEST ORDER BY ID;
> ID V
> -- -
> 1  1
> 3  3
> rows (ordered): 2

ALTER TABLE TEST ADD V2 TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
> ok

UPDATE TEST SET V = 4 WHERE ID = 3;
> update count: 1

SELECT ID, V, LENGTH(V2) > 18 AS L FROM TEST ORDER BY ID;
> ID V L
> -- - ----
> 1  1 null
> 3  4 TRUE
> rows (ordered): 2

UPDATE TEST SET V = 1 WHERE V = 1;
> update count: 1

SELECT ID, V, LENGTH(V2) > 18 AS L FROM TEST ORDER BY ID;
> ID V L
> -- - ----
> 1  1 null
> 3  4 TRUE
> rows (ordered): 2

MERGE INTO TEST(ID, V) KEY(ID) VALUES (1, 1);
> update count: 1

SELECT ID, V, LENGTH(V2) > 18 AS L FROM TEST ORDER BY ID;
> ID V L
> -- - ----
> 1  1 null
> 3  4 TRUE
> rows (ordered): 2

MERGE INTO TEST(ID, V) KEY(ID) VALUES (1, 2);
> update count: 1

SELECT ID, V, LENGTH(V2) > 18 AS L FROM TEST ORDER BY ID;
> ID V L
> -- - ----
> 1  2 TRUE
> 3  4 TRUE
> rows (ordered): 2

ALTER TABLE TEST ALTER COLUMN V SET ON UPDATE NULL;
> ok

SELECT COLUMN_NAME, COLUMN_DEFAULT, COLUMN_ON_UPDATE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY COLUMN_NAME;
> COLUMN_NAME COLUMN_DEFAULT              COLUMN_ON_UPDATE
> ----------- --------------------------- -------------------
> ID          null                        null
> V           (NEXT VALUE FOR PUBLIC.SEQ) NULL
> V2          null                        CURRENT_TIMESTAMP()
> rows (ordered): 3

ALTER TABLE TEST ALTER COLUMN V DROP ON UPDATE;
> ok

SELECT COLUMN_NAME, COLUMN_DEFAULT, COLUMN_ON_UPDATE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST' ORDER BY COLUMN_NAME;
> COLUMN_NAME COLUMN_DEFAULT              COLUMN_ON_UPDATE
> ----------- --------------------------- -------------------
> ID          null                        null
> V           (NEXT VALUE FOR PUBLIC.SEQ) null
> V2          null                        CURRENT_TIMESTAMP()
> rows (ordered): 3

DROP TABLE TEST;
> ok

DROP SEQUENCE SEQ;
> ok
