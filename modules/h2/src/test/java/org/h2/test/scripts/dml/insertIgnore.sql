-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
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
> exception

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
