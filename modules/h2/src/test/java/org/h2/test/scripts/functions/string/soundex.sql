-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');

select soundex(null) en, soundex('tom') et from test;
> EN   ET
> ---- ----
> null t500
> rows: 1

select
soundex('Washington') W252, soundex('Lee') L000,
soundex('Gutierrez') G362, soundex('Pfister') P236,
soundex('Jackson') J250, soundex('Tymczak') T522,
soundex('VanDeusen') V532, soundex('Ashcraft') A261 from test;
> W252 L000 G362 P236 J250 T522 V532 A261
> ---- ---- ---- ---- ---- ---- ---- ----
> W252 L000 G362 P236 J250 T522 V532 A261
> rows: 1

