-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(C1 VARCHAR_IGNORECASE);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME          COLUMN_TYPE
> ----------- --------- ------------------ ------------------
> C1          12        VARCHAR_IGNORECASE VARCHAR_IGNORECASE
> rows (ordered): 1

DROP TABLE TEST;
> ok
