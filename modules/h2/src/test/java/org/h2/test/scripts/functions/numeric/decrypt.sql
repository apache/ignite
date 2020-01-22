-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

call utf8tostring(decrypt('AES', '00000000000000000000000000000000', 'dbd42d55d4b923c4b03eba0396fac98e'));
>> Hello World Test

call utf8tostring(decrypt('AES', hash('sha256', stringtoutf8('Hello'), 1000), encrypt('AES', hash('sha256', stringtoutf8('Hello'), 1000), stringtoutf8('Hello World Test'))));
>> Hello World Test
