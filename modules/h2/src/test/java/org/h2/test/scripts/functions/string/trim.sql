-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID INT PRIMARY KEY, A VARCHAR, B VARCHAR, C VARCHAR) AS VALUES (1, '__A__', '    B    ', 'xAx');
> ok

SELECT TRIM(BOTH '_' FROM A), '|' || TRIM(LEADING FROM B) || '|', TRIM(TRAILING 'x' FROM C) FROM TEST;
> TRIM('_' FROM A) ('|' || TRIM(LEADING B)) || '|' TRIM(TRAILING 'x' FROM C)
> ---------------- ------------------------------- -------------------------
> A                |B |                            xA
> rows: 1

SELECT LENGTH(TRIM(B)), LENGTH(TRIM(FROM B)) FROM TEST;
> LENGTH(TRIM(B)) LENGTH(TRIM(B))
> --------------- ---------------
> 1               1
> rows: 1

SELECT TRIM(BOTH B) FROM TEST;
> exception SYNTAX_ERROR_2

DROP TABLE TEST;
> ok

select TRIM(' ' FROM '  abc   ') from dual;
> 'abc'
> -----
> abc
> rows: 1
