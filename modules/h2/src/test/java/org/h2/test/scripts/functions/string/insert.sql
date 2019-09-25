-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select insert(null, null, null, null) en, insert('Rund', 1, 0, 'o') e_round, insert(null, 1, 1, 'a') ea from test;
> EN   E_ROUND EA
> ---- ------- --
> null Rund    a
> rows: 1

select insert('World', 2, 4, 'e') welt, insert('Hello', 2, 1, 'a') hallo from test;
> WELT HALLO
> ---- -----
> We   Hallo
> rows: 1

