-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID INT);
> ok

INSERT INTO TEST VALUES (1), (2), (3);
> update count: 3

DELETE FROM TEST WHERE EXISTS (SELECT X FROM SYSTEM_RANGE(1, 3) WHERE X = ID) AND ROWNUM() = 1;
> update count: 1

SELECT ID FROM TEST;
> ID
> --
> 2
> 3
> rows: 2

DROP TABLE TEST;
> ok
