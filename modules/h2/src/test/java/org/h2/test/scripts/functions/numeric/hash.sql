-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

call hash('SHA256', 'Hello', 0);
> exception INVALID_VALUE_2

call hash('SHA256', 'Hello');
>> 185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969

call hash('SHA256', 'Hello', 1);
>> 185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969

call hash('SHA256', stringtoutf8('Hello'), 1);
>> 185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969

CALL HASH('SHA256', 'Password', 1000);
>> c644a176ce920bde361ac336089b06cc2f1514dfa95ba5aabfe33f9a22d577f0

CALL HASH('SHA256', STRINGTOUTF8('Password'), 1000);
>> c644a176ce920bde361ac336089b06cc2f1514dfa95ba5aabfe33f9a22d577f0

call hash('unknown', 'Hello', 1);
> exception INVALID_VALUE_2
