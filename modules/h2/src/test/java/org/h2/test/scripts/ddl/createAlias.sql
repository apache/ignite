-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create alias "SYSDATE" for "java.lang.Integer.parseInt(java.lang.String)";
> exception FUNCTION_ALIAS_ALREADY_EXISTS_1

create alias "MIN" for "java.lang.Integer.parseInt(java.lang.String)";
> exception FUNCTION_ALIAS_ALREADY_EXISTS_1

create alias "CAST" for "java.lang.Integer.parseInt(java.lang.String)";
> exception FUNCTION_ALIAS_ALREADY_EXISTS_1

@reconnect off

--- function alias ---------------------------------------------------------------------------------------------
CREATE ALIAS MY_SQRT FOR "java.lang.Math.sqrt";
> ok

SELECT MY_SQRT(2.0) MS, SQRT(2.0);
> MS                 1.4142135623730951
> ------------------ ------------------
> 1.4142135623730951 1.4142135623730951
> rows: 1

SELECT MY_SQRT(SUM(X)), SUM(X), MY_SQRT(55) FROM SYSTEM_RANGE(1, 10);
> PUBLIC.MY_SQRT(SUM(X)) SUM(X) PUBLIC.MY_SQRT(55)
> ---------------------- ------ ------------------
> 7.416198487095663      55     7.416198487095663
> rows: 1

SELECT MY_SQRT(-1.0) MS, SQRT(NULL) S;
> MS  S
> --- ----
> NaN null
> rows: 1

SCRIPT NOPASSWORDS NOSETTINGS;
> SCRIPT
> ----------------------------------------------------------------
> CREATE FORCE ALIAS "PUBLIC"."MY_SQRT" FOR "java.lang.Math.sqrt";
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> rows: 2

SELECT ALIAS_NAME, JAVA_CLASS, JAVA_METHOD, DATA_TYPE, COLUMN_COUNT, RETURNS_RESULT, REMARKS FROM INFORMATION_SCHEMA.FUNCTION_ALIASES;
> ALIAS_NAME JAVA_CLASS     JAVA_METHOD DATA_TYPE COLUMN_COUNT RETURNS_RESULT REMARKS
> ---------- -------------- ----------- --------- ------------ -------------- -------
> MY_SQRT    java.lang.Math sqrt        8         1            2
> rows: 1

DROP ALIAS MY_SQRT;
> ok

CREATE SCHEMA TEST_SCHEMA;
> ok

CREATE ALIAS TRUNC FOR "java.lang.Math.floor(double)";
> exception FUNCTION_ALIAS_ALREADY_EXISTS_1

CREATE ALIAS PUBLIC.TRUNC FOR "java.lang.Math.floor(double)";
> exception FUNCTION_ALIAS_ALREADY_EXISTS_1

CREATE ALIAS TEST_SCHEMA.TRUNC FOR "java.lang.Math.round(double)";
> exception FUNCTION_ALIAS_ALREADY_EXISTS_1

SET BUILTIN_ALIAS_OVERRIDE=1;
> ok

CREATE ALIAS TRUNC FOR "java.lang.Math.floor(double)";
> ok

SELECT TRUNC(1.5);
>> 1.0

SELECT TRUNC(-1.5);
>> -2.0

DROP ALIAS TRUNC;
> ok

CREATE ALIAS PUBLIC.TRUNC FOR "java.lang.Math.floor(double)";
> ok

CREATE ALIAS TEST_SCHEMA.TRUNC FOR "java.lang.Math.round(double)";
> ok

SELECT PUBLIC.TRUNC(1.5);
>> 1.0

SELECT PUBLIC.TRUNC(-1.5);
>> -2.0

SELECT TEST_SCHEMA.TRUNC(1.5);
>> 2

SELECT TEST_SCHEMA.TRUNC(-1.5);
>> -1

DROP ALIAS PUBLIC.TRUNC;
> ok

DROP ALIAS TEST_SCHEMA.TRUNC;
> ok

SET BUILTIN_ALIAS_OVERRIDE=0;
> ok

DROP SCHEMA TEST_SCHEMA RESTRICT;
> ok

-- test for issue #1531
CREATE TABLE TEST (ID BIGINT, VAL VARCHAR2(10)) AS SELECT x,'val'||x FROM SYSTEM_RANGE(1,2);
> ok

CREATE ALIAS FTBL AS $$ ResultSet t(Connection c) throws SQLException {return c.prepareStatement("SELECT ID, VAL FROM TEST").executeQuery();} $$;
> ok

CREATE OR REPLACE VIEW V_TEST (ID, VAL) AS (SELECT * FROM FTBL());
> ok

SELECT * FROM V_TEST;
> ID VAL
> -- ----
> 1  val1
> 2  val2
> rows: 2
