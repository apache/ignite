-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID INT PRIMARY KEY, N NUMERIC);
> ok

INSERT INTO TEST VALUES(1, 1), (2, 2), (3, NULL), (4, 5);
> update count: 4

SELECT ID, N, RATIO_TO_REPORT(N) OVER() R2R FROM TEST;
> ID N    R2R
> -- ---- -----
> 1  1    0.125
> 2  2    0.25
> 3  null null
> 4  5    0.625
> rows: 4

INSERT INTO TEST VALUES (5, -8);
> update count: 1

SELECT ID, N, RATIO_TO_REPORT(N) OVER() R2R FROM TEST;
> ID N    R2R
> -- ---- ----
> 1  1    null
> 2  2    null
> 3  null null
> 4  5    null
> 5  -8   null
> rows: 5

SELECT RATIO_TO_REPORT(N) OVER (ORDER BY N) FROM TEST;
> exception SYNTAX_ERROR_1

DROP TABLE TEST;
> ok
