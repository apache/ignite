-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(D1 DATE);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE, NUMERIC_SCALE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE NUMERIC_SCALE DATETIME_PRECISION
> ----------- --------- --------- ----------- ------------- ------------------
> D1          91        DATE      DATE        0             0
> rows (ordered): 1

DROP TABLE TEST;
> ok

SELECT DATE '2000-01-02';
>> 2000-01-02

SELECT DATE '20000102';
>> 2000-01-02

SELECT DATE '-1000102';
>> -100-01-02

SELECT DATE '3001231';
>> 0300-12-31

-- PostgreSQL returns 2020-12-31
SELECT DATE '201231';
> exception INVALID_DATETIME_CONSTANT_2
