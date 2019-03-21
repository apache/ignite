-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INT, B INT);
> ok

INSERT INTO TEST VALUES (1, 1), (1, 3), (2, 1), (2, 5), (3, 4);
> update count: 5

SELECT A, EVERY(B < 5), BOOL_AND(B > 1), EVERY(B >= 1) FILTER (WHERE A = 1) FROM TEST GROUP BY A;
> A EVERY(B < 5) EVERY(B > 1) EVERY(B >= 1) FILTER (WHERE (A = 1))
> - ------------ ------------ ------------------------------------
> 1 TRUE         FALSE        TRUE
> 2 FALSE        FALSE        null
> 3 TRUE         TRUE         null
> rows: 3

DROP TABLE TEST;
> ok
