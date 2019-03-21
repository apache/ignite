-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INT, B INT);
> ok

INSERT INTO TEST VALUES ROW (1, 2), (3, 4), ROW (5, 6);
> update count: 3

INSERT INTO TEST(a) VALUES 7;
> update count: 1

INSERT INTO TEST(a) VALUES 8, 9;
> update count: 2

TABLE TEST;
> A B
> - ----
> 1 2
> 3 4
> 5 6
> 7 null
> 8 null
> 9 null
> rows: 6

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT);
> ok

-- TODO Do we need _ROWID_ support here?
INSERT INTO TEST(_ROWID_, ID) VALUES (2, 3);
> update count: 1

SELECT _ROWID_, ID FROM TEST;
> _ROWID_ ID
> ------- --
> 2       3
> rows: 1

DROP TABLE TEST;
> ok
