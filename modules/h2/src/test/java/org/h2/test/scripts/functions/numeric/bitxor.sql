-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select bitxor(null, 1) vn, bitxor(1, null) vn1, bitxor(null, null) vn2, bitxor(3, 6) e5 from test;
> VN   VN1  VN2  E5
> ---- ---- ---- --
> null null null 5
> rows: 1




