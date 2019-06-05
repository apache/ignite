-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CALL PARSEDATETIME('3. Februar 2001', 'd. MMMM yyyy', 'de');
>> 2001-02-03 00:00:00

CALL PARSEDATETIME('02/03/2001 04:05:06', 'MM/dd/yyyy HH:mm:ss');
>> 2001-02-03 04:05:06
