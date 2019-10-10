-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(T1 TIMESTAMP, T2 TIMESTAMP WITHOUT TIME ZONE);
> ok

INSERT INTO TEST(T1, T2) VALUES (TIMESTAMP '2010-01-01 10:00:00', TIMESTAMP WITHOUT TIME ZONE '2010-01-01 10:00:00');
> update count: 1

SELECT T1, T2, T1 = T2 FROM TEST;
> T1                  T2                  T1 = T2
> ------------------- ------------------- -------
> 2010-01-01 10:00:00 2010-01-01 10:00:00 TRUE
> rows: 1

ALTER TABLE TEST ADD (T3 TIMESTAMP(0), T4 TIMESTAMP(9) WITHOUT TIME ZONE);

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE                    NUMERIC_SCALE
> ----------- --------- --------- ------------------------------ -------------
> T1          93        TIMESTAMP TIMESTAMP                      6
> T2          93        TIMESTAMP TIMESTAMP WITHOUT TIME ZONE    6
> T3          93        TIMESTAMP TIMESTAMP(0)                   0
> T4          93        TIMESTAMP TIMESTAMP(9) WITHOUT TIME ZONE 9
> rows (ordered): 4

ALTER TABLE TEST ADD T5 TIMESTAMP(10);
> exception

DROP TABLE TEST;
> ok

-- Check that TIMESTAMP is allowed as a column name
CREATE TABLE TEST(TIMESTAMP TIMESTAMP(0));
> ok

INSERT INTO TEST VALUES (TIMESTAMP '1999-12-31 08:00:00');
> update count: 1

SELECT TIMESTAMP FROM TEST;
>> 1999-12-31 08:00:00

DROP TABLE TEST;
> ok

CREATE TABLE TEST(T TIMESTAMP, T0 TIMESTAMP(0), T1 TIMESTAMP(1), T2 TIMESTAMP(2), T3 TIMESTAMP(3), T4 TIMESTAMP(4),
    T5 TIMESTAMP(5), T6 TIMESTAMP(6), T7 TIMESTAMP(7), T8 TIMESTAMP(8), T9 TIMESTAMP(9));
> ok

INSERT INTO TEST VALUES ('2000-01-01 08:00:00.123456789', '2000-01-01 08:00:00.123456789',
    '2000-01-01 08:00:00.123456789', '2000-01-01 08:00:00.123456789', '2000-01-01 08:00:00.123456789',
    '2000-01-01 08:00:00.123456789', '2000-01-01 08:00:00.123456789', '2000-01-01 08:00:00.123456789',
    '2000-01-01 08:00:00.123456789', '2000-01-01 08:00:00.123456789', '2000-01-01 08:00:00.123456789');
> update count: 1

SELECT T, T0, T1, T2 FROM TEST;
> T                          T0                  T1                    T2
> -------------------------- ------------------- --------------------- ----------------------
> 2000-01-01 08:00:00.123457 2000-01-01 08:00:00 2000-01-01 08:00:00.1 2000-01-01 08:00:00.12
> rows: 1

SELECT T3, T4, T5, T6 FROM TEST;
> T3                      T4                       T5                        T6
> ----------------------- ------------------------ ------------------------- --------------------------
> 2000-01-01 08:00:00.123 2000-01-01 08:00:00.1235 2000-01-01 08:00:00.12346 2000-01-01 08:00:00.123457
> rows: 1

SELECT T7, T8, T9 FROM TEST;
> T7                          T8                           T9
> --------------------------- ---------------------------- -----------------------------
> 2000-01-01 08:00:00.1234568 2000-01-01 08:00:00.12345679 2000-01-01 08:00:00.123456789
> rows: 1

DELETE FROM TEST;
> update count: 1

INSERT INTO TEST(T0) VALUES ('2000-01-01 23:59:59.999999999');
> update count: 1

SELECT T0 FROM TEST;
>> 2000-01-02 00:00:00

DROP TABLE TEST;
> ok
