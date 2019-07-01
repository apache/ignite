-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select position(null, null) en, position(null, 'abc') en1, position('World', 'Hello World') e7, position('hi', 'abchihihi') e1;
> EN   EN1  E7 E1
> ---- ---- -- --
> null null 7  4
> rows: 1
