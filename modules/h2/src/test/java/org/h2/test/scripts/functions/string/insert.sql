-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select insert(null, null, null, null) en, insert('Rund', 1, 0, 'o') e_round, insert(null, 1, 1, 'a') ea;
> EN   E_ROUND EA
> ---- ------- --
> null Rund    a
> rows: 1

select insert('World', 2, 4, 'e') welt, insert('Hello', 2, 1, 'a') hallo;
> WELT HALLO
> ---- -----
> We   Hallo
> rows: 1
