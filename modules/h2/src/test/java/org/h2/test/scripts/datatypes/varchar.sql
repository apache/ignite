-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(C1 VARCHAR, C2 CHARACTER VARYING, C3 VARCHAR2, C4 NVARCHAR, C5 NVARCHAR2, C6 VARCHAR_CASESENSITIVE,
    C7 LONGVARCHAR, C8 TID);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE
> ----------- --------- --------- ---------------------
> C1          12        VARCHAR   VARCHAR
> C2          12        VARCHAR   CHARACTER VARYING
> C3          12        VARCHAR   VARCHAR2
> C4          12        VARCHAR   NVARCHAR
> C5          12        VARCHAR   NVARCHAR2
> C6          12        VARCHAR   VARCHAR_CASESENSITIVE
> C7          12        VARCHAR   LONGVARCHAR
> C8          12        VARCHAR   TID
> rows (ordered): 8

DROP TABLE TEST;
> ok
