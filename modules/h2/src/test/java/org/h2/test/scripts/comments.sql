-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CALL 1 /* comment */ ;;
> 1
> -
> 1
> rows: 1

CALL 1 /* comment */ ;
> 1
> -
> 1
> rows: 1

call /* remark * / * /* ** // end */ 1;
> 1
> -
> 1
> rows: 1

--- remarks/comments/syntax ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(
ID INT PRIMARY KEY, -- this is the primary key, type {integer}
NAME VARCHAR(255) -- this is a string
);
> ok

INSERT INTO TEST VALUES(
1 /* ID */,
'Hello' // NAME
);
> update count: 1

SELECT * FROM TEST;
> ID NAME
> -- -----
> 1  Hello
> rows: 1

DROP_ TABLE_ TEST_T;
> exception SYNTAX_ERROR_2

DROP TABLE TEST /*;
> exception SYNTAX_ERROR_1

DROP TABLE TEST;
> ok
