-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(T1 TIME, T2 TIME WITHOUT TIME ZONE);
> ok

INSERT INTO TEST(T1, T2) VALUES (TIME '10:00:00', TIME WITHOUT TIME ZONE '10:00:00');
> update count: 1

SELECT T1, T2, T1 = T2 FROM TEST;
> T1       T2       T1 = T2
> -------- -------- -------
> 10:00:00 10:00:00 TRUE
> rows: 1

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE
> ----------- --------- --------- ----------------------
> T1          92        TIME      TIME
> T2          92        TIME      TIME WITHOUT TIME ZONE
> rows (ordered): 2

ALTER TABLE TEST ADD (T3 TIME(0), T4 TIME(9) WITHOUT TIME ZONE);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE               NUMERIC_SCALE
> ----------- --------- --------- ------------------------- -------------
> T1          92        TIME      TIME                      0
> T2          92        TIME      TIME WITHOUT TIME ZONE    0
> T3          92        TIME      TIME(0)                   0
> T4          92        TIME      TIME(9) WITHOUT TIME ZONE 9
> rows (ordered): 4

ALTER TABLE TEST ADD T5 TIME(10);
> exception

DROP TABLE TEST;
> ok

-- Check that TIME is allowed as a column name
CREATE TABLE TEST(TIME TIME);
> ok

INSERT INTO TEST VALUES (TIME '08:00:00');
> update count: 1

SELECT TIME FROM TEST;
> TIME
> --------
> 08:00:00
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(T TIME, T0 TIME(0), T1 TIME(1), T2 TIME(2), T3 TIME(3), T4 TIME(4), T5 TIME(5), T6 TIME(6),
    T7 TIME(7), T8 TIME(8), T9 TIME(9));
> ok

INSERT INTO TEST VALUES ('08:00:00.123456789', '08:00:00.123456789', '08:00:00.123456789', '08:00:00.123456789',
    '08:00:00.123456789', '08:00:00.123456789', '08:00:00.123456789', '08:00:00.123456789', '08:00:00.123456789',
    '08:00:00.123456789', '08:00:00.123456789');
> update count: 1

SELECT * FROM TEST;
> T        T0       T1         T2          T3           T4            T5             T6              T7               T8                T9
> -------- -------- ---------- ----------- ------------ ------------- -------------- --------------- ---------------- ----------------- ------------------
> 08:00:00 08:00:00 08:00:00.1 08:00:00.12 08:00:00.123 08:00:00.1235 08:00:00.12346 08:00:00.123457 08:00:00.1234568 08:00:00.12345679 08:00:00.123456789
> rows: 1

DELETE FROM TEST;
> update count: 1

INSERT INTO TEST(T0) VALUES ('23:59:59.999999999');
> update count: 1

SELECT T0 FROM TEST;
>> 23:59:59.999999999

DROP TABLE TEST;
> ok
