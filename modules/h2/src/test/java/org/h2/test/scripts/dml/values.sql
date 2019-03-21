-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

VALUES (1, 2);
> C1 C2
> -- --
> 1  2
> rows: 1

VALUES ROW (1, 2);
> C1 C2
> -- --
> 1  2
> rows: 1

VALUES 1, 2;
> C1
> --
> 1
> 2
> rows: 2

VALUES 4, 3, 1, 2 ORDER BY 1 FETCH FIRST 75 PERCENT ROWS ONLY;
> C1
> --
> 1
> 2
> 3
> rows (ordered): 3

SELECT * FROM (VALUES (1::BIGINT, 2)) T (A, B) WHERE (A, B) IN (VALUES(1, 2));
> A B
> - -
> 1 2
> rows: 1

SELECT * FROM (VALUES (1000000000000, 2)) T (A, B) WHERE (A, B) IN (VALUES(1, 2));
> A B
> - -
> rows: 0

SELECT * FROM (VALUES (1, 2)) T (A, B) WHERE (A, B) IN (VALUES(1::BIGINT, 2));
> A B
> - -
> 1 2
> rows: 1

SELECT * FROM (VALUES (1, 2)) T (A, B) WHERE (A, B) IN (VALUES(1000000000000, 2));
> A B
> - -
> rows: 0
