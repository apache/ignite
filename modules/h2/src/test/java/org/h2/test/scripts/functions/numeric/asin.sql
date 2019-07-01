-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select asin(null) vn, asin(-1) r1;
> VN   R1
> ---- -------------------
> null -1.5707963267948966
> rows: 1
