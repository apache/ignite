-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select atan2(null, null) vn, atan2(10, 1) r1;
> VN   R1
> ---- ------------------
> null 1.4711276743037347
> rows: 1
