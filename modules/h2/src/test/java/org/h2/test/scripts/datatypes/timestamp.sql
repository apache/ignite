-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
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

ALTER TABLE TEST ADD (T3 TIMESTAMP(0), T4 TIMESTAMP(9) WITHOUT TIME ZONE,
    DT1 DATETIME, DT2 DATETIME(0), DT3 DATETIME(9),
    DT2_1 DATETIME2, DT2_2 DATETIME2(0), DT2_3 DATETIME2(7),
    SDT1 SMALLDATETIME);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE, NUMERIC_SCALE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE                    NUMERIC_SCALE DATETIME_PRECISION
> ----------- --------- --------- ------------------------------ ------------- ------------------
> T1          93        TIMESTAMP TIMESTAMP                      6             6
> T2          93        TIMESTAMP TIMESTAMP WITHOUT TIME ZONE    6             6
> T3          93        TIMESTAMP TIMESTAMP(0)                   0             0
> T4          93        TIMESTAMP TIMESTAMP(9) WITHOUT TIME ZONE 9             9
> DT1         93        TIMESTAMP DATETIME                       6             6
> DT2         93        TIMESTAMP DATETIME(0)                    0             0
> DT3         93        TIMESTAMP DATETIME(9)                    9             9
> DT2_1       93        TIMESTAMP DATETIME2                      6             6
> DT2_2       93        TIMESTAMP DATETIME2(0)                   0             0
> DT2_3       93        TIMESTAMP DATETIME2(7)                   7             7
> SDT1        93        TIMESTAMP SMALLDATETIME                  0             0
> rows (ordered): 11

ALTER TABLE TEST ADD T5 TIMESTAMP(10);
> exception INVALID_VALUE_SCALE_PRECISION

ALTER TABLE TEST ADD DT4 DATETIME(10);
> exception INVALID_VALUE_SCALE_PRECISION

ALTER TABLE TEST ADD DT2_4 DATETIME2(10);
> exception INVALID_VALUE_SCALE_PRECISION

ALTER TABLE TEST ADD STD2 SMALLDATETIME(1);
> exception SYNTAX_ERROR_1

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

create table test(id int, d timestamp);
> ok

insert into test values(1, '2006-01-01 12:00:00.000');
> update count: 1

insert into test values(1, '1999-12-01 23:59:00.000');
> update count: 1

select * from test where d= '1999-12-01 23:59:00.000';
> ID D
> -- -------------------
> 1  1999-12-01 23:59:00
> rows: 1

select * from test where d= timestamp '2006-01-01 12:00:00.000';
> ID D
> -- -------------------
> 1  2006-01-01 12:00:00
> rows: 1

drop table test;
> ok

SELECT TIMESTAMP '2000-01-02 11:22:33';
>> 2000-01-02 11:22:33

SELECT TIMESTAMP '2000-01-02T11:22:33';
>> 2000-01-02 11:22:33

SELECT TIMESTAMP '20000102 11:22:33';
>> 2000-01-02 11:22:33

SELECT TIMESTAMP '20000102T11:22:33';
>> 2000-01-02 11:22:33

SELECT TIMESTAMP '2000-01-02 112233';
>> 2000-01-02 11:22:33

SELECT TIMESTAMP '2000-01-02T112233';
>> 2000-01-02 11:22:33

SELECT TIMESTAMP '20000102 112233';
>> 2000-01-02 11:22:33

SELECT TIMESTAMP '20000102T112233';
>> 2000-01-02 11:22:33
