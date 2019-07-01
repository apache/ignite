-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select floor(null) vn, floor(1) v1, floor(1.1) v2, floor(-1.1) v3, floor(1.9) v4, floor(-1.9) v5;
> VN   V1 V2 V3 V4 V5
> ---- -- -- -- -- --
> null 1  1  -2 1  -2
> rows: 1

SELECT FLOOR(1.5), FLOOR(-1.5), FLOOR(1.5) IS OF (NUMERIC);
> 1 -2 TRUE
> - -- ----
> 1 -2 TRUE
> rows: 1

SELECT FLOOR(1.5::DOUBLE), FLOOR(-1.5::DOUBLE), FLOOR(1.5::DOUBLE) IS OF (DOUBLE);
> 1.0 -2.0 TRUE
> --- ---- ----
> 1.0 -2.0 TRUE
> rows: 1

SELECT FLOOR(1.5::REAL), FLOOR(-1.5::REAL), FLOOR(1.5::REAL) IS OF (REAL);
> 1.0 -2.0 TRUE
> --- ---- ----
> 1.0 -2.0 TRUE
> rows: 1
