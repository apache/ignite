-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT * FROM (VALUES(1, 2));
> C1 C2
> -- --
> 1  2
> rows: 1

SELECT * FROM (VALUES(1, 2)) AS T;
> C1 C2
> -- --
> 1  2
> rows: 1

SELECT * FROM (VALUES(1, 2)) AS T(A, B);
> A B
> - -
> 1 2
> rows: 1

SELECT A AS A1, B AS B1 FROM (VALUES(1, 2)) AS T(A, B);
> A1 B1
> -- --
> 1  2
> rows: 1

SELECT A AS A1, B AS B1 FROM (VALUES(1, 2)) AS T(A, B) WHERE A <> B;
> A1 B1
> -- --
> 1  2
> rows: 1

SELECT A AS A1, B AS B1 FROM (VALUES(1, 2)) AS T(A, B) WHERE A1 <> B1;
> exception

SELECT * FROM (VALUES(1, 2)) AS T(A);
> exception

SELECT * FROM (VALUES(1, 2)) AS T(A, a);
> exception

SELECT * FROM (VALUES(1, 2)) AS T(A, B, C);
> exception

SELECT V AS V1, A AS A1, B AS B1 FROM (VALUES (1)) T1(V) INNER JOIN (VALUES(1, 2)) T2(A, B) ON V = A;
> V1 A1 B1
> -- -- --
> 1  1  2
> rows: 1

CREATE TABLE TEST(I INT, J INT);
> ok

CREATE INDEX TEST_I_IDX ON TEST(I);
> ok

INSERT INTO TEST VALUES (1, 2);
> update count: 1

SELECT * FROM (TEST) AS T(A, B);
> A B
> - -
> 1 2
> rows: 1

SELECT * FROM TEST AS T(A, B);
> A B
> - -
> 1 2
> rows: 1

SELECT * FROM TEST AS T(A, B) USE INDEX (TEST_I_IDX);
> A B
> - -
> 1 2
> rows: 1

DROP TABLE TEST;
> ok
