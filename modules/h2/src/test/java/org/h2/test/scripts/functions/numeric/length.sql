-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select bit_length(null) en, bit_length('') e0, bit_length('ab') e32 from test;
> EN   E0 E32
> ---- -- ---
> null 0  32
> rows: 1

select length(null) en, length('') e0, length('ab') e2 from test;
> EN   E0 E2
> ---- -- --
> null 0  2
> rows: 1

select char_length(null) en, char_length('') e0, char_length('ab') e2 from test;
> EN   E0 E2
> ---- -- --
> null 0  2
> rows: 1

select character_length(null) en, character_length('') e0, character_length('ab') e2 from test;
> EN   E0 E2
> ---- -- --
> null 0  2
> rows: 1

select octet_length(null) en, octet_length('') e0, octet_length('ab') e4 from test;
> EN   E0 E4
> ---- -- --
> null 0  4
> rows: 1





