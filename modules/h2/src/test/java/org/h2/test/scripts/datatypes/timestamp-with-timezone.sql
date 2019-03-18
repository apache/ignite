-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE tab_with_timezone(x TIMESTAMP WITH TIME ZONE);
> ok

INSERT INTO tab_with_timezone(x) VALUES ('2017-01-01');
> update count: 1

SELECT "Query".* FROM (select * from tab_with_timezone where x > '2016-01-01') AS "Query";
> X
> ----------------------
> 2017-01-01 00:00:00+00

DELETE FROM tab_with_timezone;
> update count: 1

INSERT INTO tab_with_timezone VALUES ('2018-03-25 01:59:00 Europe/Berlin'), ('2018-03-25 03:00:00 Europe/Berlin');
> update count: 2

SELECT * FROM tab_with_timezone ORDER BY X;
> X
> ----------------------
> 2018-03-25 01:59:00+01
> 2018-03-25 03:00:00+02
> rows (ordered): 2

SELECT TIMESTAMP WITH TIME ZONE '2000-01-10 00:00:00 -02' AS A,
    TIMESTAMP WITH TIME ZONE '2000-01-10 00:00:00.000000000 +02:00' AS B,
    TIMESTAMP WITH TIME ZONE '2000-01-10 00:00:00.000000000+02:00' AS C,
    TIMESTAMP WITH TIME ZONE '2000-01-10T00:00:00.000000000+09:00[Asia/Tokyo]' AS D;
> A                      B                      C                      D
> ---------------------- ---------------------- ---------------------- ----------------------
> 2000-01-10 00:00:00-02 2000-01-10 00:00:00+02 2000-01-10 00:00:00+02 2000-01-10 00:00:00+09
> rows: 1

CREATE TABLE TEST(T1 TIMESTAMP WITH TIME ZONE, T2 TIMESTAMP(0) WITH TIME ZONE, T3 TIMESTAMP(9) WITH TIME ZONE);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME                COLUMN_TYPE                 NUMERIC_SCALE
> ----------- --------- ------------------------ --------------------------- -------------
> T1          2014      TIMESTAMP WITH TIME ZONE TIMESTAMP WITH TIME ZONE    6
> T2          2014      TIMESTAMP WITH TIME ZONE TIMESTAMP(0) WITH TIME ZONE 0
> T3          2014      TIMESTAMP WITH TIME ZONE TIMESTAMP(9) WITH TIME ZONE 9
> rows (ordered): 3

ALTER TABLE TEST ADD T4 TIMESTAMP (10) WITH TIME ZONE;
> exception

DROP TABLE TEST;
> ok

CREATE TABLE TEST(T TIMESTAMP WITH TIME ZONE, T0 TIMESTAMP(0) WITH TIME ZONE, T1 TIMESTAMP(1) WITH TIME ZONE,
    T2 TIMESTAMP(2) WITH TIME ZONE, T3 TIMESTAMP(3) WITH TIME ZONE, T4 TIMESTAMP(4) WITH TIME ZONE,
    T5 TIMESTAMP(5) WITH TIME ZONE, T6 TIMESTAMP(6) WITH TIME ZONE, T7 TIMESTAMP(7) WITH TIME ZONE,
    T8 TIMESTAMP(8) WITH TIME ZONE, T9 TIMESTAMP(9) WITH TIME ZONE);
> ok

INSERT INTO TEST VALUES ('2000-01-01 08:00:00.123456789Z', '2000-01-01 08:00:00.123456789Z',
    '2000-01-01 08:00:00.123456789Z', '2000-01-01 08:00:00.123456789Z', '2000-01-01 08:00:00.123456789Z',
    '2000-01-01 08:00:00.123456789Z', '2000-01-01 08:00:00.123456789Z', '2000-01-01 08:00:00.123456789Z',
    '2000-01-01 08:00:00.123456789Z', '2000-01-01 08:00:00.123456789Z', '2000-01-01 08:00:00.123456789Z');
> update count: 1

SELECT T, T0, T1, T2 FROM TEST;
> T                             T0                     T1                       T2
> ----------------------------- ---------------------- ------------------------ -------------------------
> 2000-01-01 08:00:00.123457+00 2000-01-01 08:00:00+00 2000-01-01 08:00:00.1+00 2000-01-01 08:00:00.12+00
> rows: 1

SELECT T3, T4, T5, T6 FROM TEST;
> T3                         T4                          T5                           T6
> -------------------------- --------------------------- ---------------------------- -----------------------------
> 2000-01-01 08:00:00.123+00 2000-01-01 08:00:00.1235+00 2000-01-01 08:00:00.12346+00 2000-01-01 08:00:00.123457+00
> rows: 1

SELECT T7, T8, T9 FROM TEST;
> T7                             T8                              T9
> ------------------------------ ------------------------------- --------------------------------
> 2000-01-01 08:00:00.1234568+00 2000-01-01 08:00:00.12345679+00 2000-01-01 08:00:00.123456789+00
> rows: 1

DELETE FROM TEST;
> update count: 1

INSERT INTO TEST(T0) VALUES ('2000-01-01 23:59:59.999999999Z');
> update count: 1

SELECT T0 FROM TEST;
>> 2000-01-02 00:00:00+00

DROP TABLE TEST;
> ok
