-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CALL XMLTEXT('test');
>> test

CALL XMLTEXT('<test>');
>> &lt;test&gt;

SELECT XMLTEXT('hello' || chr(10) || 'world');
>> hello world

CALL XMLTEXT('hello' || chr(10) || 'world', true);
>> hello&#xa;world
