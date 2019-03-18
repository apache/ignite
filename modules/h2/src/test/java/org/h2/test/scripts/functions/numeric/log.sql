-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select log(null) vn, log(1) v1, ln(1.1) v2, log(-1.1) v3, log(1.9) v4, log(-1.9) v5 from test;
> VN   V1  V2                  V3  V4                 V5
> ---- --- ------------------- --- ------------------ ---
> null 0.0 0.09531017980432493 NaN 0.6418538861723947 NaN
> rows: 1

select log10(null) vn, log10(0) v1, log10(10) v2, log10(0.0001) v3, log10(1000000) v4, log10(1) v5 from test;
> VN   V1        V2  V3   V4  V5
> ---- --------- --- ---- --- ---
> null -Infinity 1.0 -4.0 6.0 0.0
> rows: 1

select log(null) vn, log(1) v1, log(1.1) v2, log(-1.1) v3, log(1.9) v4, log(-1.9) v5 from test;
> VN   V1  V2                  V3  V4                 V5
> ---- --- ------------------- --- ------------------ ---
> null 0.0 0.09531017980432493 NaN 0.6418538861723947 NaN
> rows: 1






