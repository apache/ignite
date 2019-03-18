-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INT CONSTRAINT PK_1 PRIMARY KEY);
> ok

SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS;
> CONSTRAINT_NAME CONSTRAINT_TYPE
> --------------- ---------------
> PK_1            PRIMARY KEY
> rows: 1

DROP TABLE TEST;
> ok
