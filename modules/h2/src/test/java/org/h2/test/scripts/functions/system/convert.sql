-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select convert(null, varchar(255)) xn, convert(' 10', int) x10, convert(' 20 ', int) x20;
> XN   X10 X20
> ---- --- ---
> null 10  20
> rows: 1
