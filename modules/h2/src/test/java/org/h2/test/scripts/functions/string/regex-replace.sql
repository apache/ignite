-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

call regexp_replace('x', 'x', '\');
> exception

CALL REGEXP_REPLACE('abckaboooom', 'o+', 'o');
>> abckabom

select regexp_replace('Sylvain', 'S..', 'TOTO', 'mni');
>> TOTOvain

set mode oracle;

select regexp_replace('first last', '(\w+) (\w+)', '\2 \1');
>> last first

select regexp_replace('first last', '(\w+) (\w+)', '\\2 \1');
>> \2 first

select regexp_replace('first last', '(\w+) (\w+)', '\$2 \1');
>> $2 first

select regexp_replace('first last', '(\w+) (\w+)', '$2 $1');
>> $2 $1

set mode regular;

select regexp_replace('first last', '(\w+) (\w+)', '\2 \1');
>> 2 1

select regexp_replace('first last', '(\w+) (\w+)', '$2 $1');
>> last first
