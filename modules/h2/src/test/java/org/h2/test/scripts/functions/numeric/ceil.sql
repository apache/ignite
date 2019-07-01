-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select ceil(null) vn, ceil(1) v1, ceiling(1.1) v2, ceil(-1.1) v3, ceiling(1.9) v4, ceiling(-1.9) v5;
> VN   V1 V2 V3 V4 V5
> ---- -- -- -- -- --
> null 1  2  -1 2  -1
> rows: 1

SELECT CEIL(1.5), CEIL(-1.5), CEIL(1.5) IS OF (NUMERIC);
> 2 -1 TRUE
> - -- ----
> 2 -1 TRUE
> rows: 1

SELECT CEIL(1.5::DOUBLE), CEIL(-1.5::DOUBLE), CEIL(1.5::DOUBLE) IS OF (DOUBLE);
> 2.0 -1.0 TRUE
> --- ---- ----
> 2.0 -1.0 TRUE
> rows: 1

SELECT CEIL(1.5::REAL), CEIL(-1.5::REAL), CEIL(1.5::REAL) IS OF (REAL);
> 2.0 -1.0 TRUE
> --- ---- ----
> 2.0 -1.0 TRUE
> rows: 1
