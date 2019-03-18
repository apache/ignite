-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE MEMORY TABLE TEST(D1 DOUBLE, D2 DOUBLE PRECISION, D3 FLOAT, D4 FLOAT(25), D5 FLOAT(53));
> ok

ALTER TABLE TEST ADD COLUMN D6 FLOAT(54);
> exception

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE
> ----------- --------- --------- ----------------
> D1          8         DOUBLE    DOUBLE
> D2          8         DOUBLE    DOUBLE PRECISION
> D3          8         DOUBLE    FLOAT
> D4          8         DOUBLE    FLOAT(25)
> D5          8         DOUBLE    FLOAT(53)
> rows (ordered): 5

SCRIPT NODATA NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> --------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE PUBLIC.TEST( D1 DOUBLE, D2 DOUBLE PRECISION, D3 FLOAT, D4 FLOAT(25), D5 FLOAT(53) );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> rows: 3

DROP TABLE TEST;
> ok
