-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INT, B INT);
> ok

INSERT INTO TEST VALUES (1, 1), (1, 3), (2, 1), (2, 5), (3, 4);
> update count: 5

SELECT A, ANY(B < 2), SOME(B > 3), BOOL_OR(B = 1), ANY(B = 1) FILTER (WHERE A = 1) FROM TEST GROUP BY A;
> A ANY(B < 2) ANY(B > 3) ANY(B = 1) ANY(B = 1) FILTER (WHERE (A = 1))
> - ---------- ---------- ---------- ---------------------------------
> 1 TRUE       FALSE      TRUE       TRUE
> 2 TRUE       TRUE       TRUE       null
> 3 FALSE      TRUE       FALSE      null
> rows: 3

DROP TABLE TEST;
> ok

SELECT TRUE = (ANY((SELECT TRUE)));
> TRUE = (ANY((SELECT TRUE FROM SYSTEM_RANGE(1, 1) /* PUBLIC.RANGE_INDEX */ /* scanCount: 2 */)))
> -----------------------------------------------------------------------------------------------
> TRUE
> rows: 1

SELECT TRUE = (ANY((SELECT FALSE)));
> TRUE = (ANY((SELECT FALSE FROM SYSTEM_RANGE(1, 1) /* PUBLIC.RANGE_INDEX */ /* scanCount: 2 */)))
> ------------------------------------------------------------------------------------------------
> FALSE
> rows: 1
