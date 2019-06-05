-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1


select cast(null as varchar(255)) xn, cast(' 10' as int) x10, cast(' 20 ' as int) x20 from test;
> XN   X10 X20
> ---- --- ---
> null 10  20
> rows: 1

select cast(128 as binary);
>> 00000080

select cast(65535 as binary);
>> 0000ffff

select cast(cast('ff' as binary) as tinyint) x;
>> -1

select cast(cast('7f' as binary) as tinyint) x;
>> 127

select cast(cast('ff' as binary) as smallint) x;
>> 255

select cast(cast('ff' as binary) as int) x;
>> 255

select cast(cast('ffff' as binary) as long) x;
>> 65535

select cast(cast(65535 as long) as binary);
>> 000000000000ffff

select cast(cast(-1 as tinyint) as binary);
>> ff

select cast(cast(-1 as smallint) as binary);
>> ffff

select cast(cast(-1 as int) as binary);
>> ffffffff

select cast(cast(-1 as long) as binary);
>> ffffffffffffffff

select cast(cast(1 as tinyint) as binary);
>> 01

select cast(cast(1 as smallint) as binary);
>> 0001

select cast(cast(1 as int) as binary);
>> 00000001

select cast(cast(1 as long) as binary);
>> 0000000000000001

select cast(X'ff' as tinyint);
>> -1

select cast(X'ffff' as smallint);
>> -1

select cast(X'ffffffff' as int);
>> -1

select cast(X'ffffffffffffffff' as long);
>> -1

select cast(' 011 ' as int);
>> 11

select cast(cast(0.1 as real) as decimal);
>> 0.1

select cast(cast(95605327.73 as float) as decimal);
>> 95605327.73

select cast(cast('01020304-0506-0708-090a-0b0c0d0e0f00' as uuid) as binary);
>> 0102030405060708090a0b0c0d0e0f00

call cast('null' as uuid);
> exception

select cast('12345678123456781234567812345678' as uuid);
>> 12345678-1234-5678-1234-567812345678

select cast('000102030405060708090a0b0c0d0e0f' as uuid);
>> 00010203-0405-0607-0809-0a0b0c0d0e0f

select -cast(0 as double);
>> 0.0
