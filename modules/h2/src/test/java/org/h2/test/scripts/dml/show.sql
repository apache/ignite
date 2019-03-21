-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

------------------------------
-- PostgreSQL compatibility --
------------------------------

SHOW CLIENT_ENCODING;
> CLIENT_ENCODING
> ---------------
> UNICODE
> rows: 1

SHOW DEFAULT_TRANSACTION_ISOLATION;
> DEFAULT_TRANSACTION_ISOLATION
> -----------------------------
> read committed
> rows: 1

SHOW TRANSACTION ISOLATION LEVEL;
> TRANSACTION_ISOLATION
> ---------------------
> read committed
> rows: 1

SHOW DATESTYLE;
> DATESTYLE
> ---------
> ISO
> rows: 1

SHOW SERVER_VERSION;
> SERVER_VERSION
> --------------
> 8.2.23
> rows: 1

SHOW SERVER_ENCODING;
> SERVER_ENCODING
> ---------------
> UTF8
> rows: 1

-------------------------
-- MySQL compatibility --
-------------------------

CREATE TABLE TEST_P(ID_P INT PRIMARY KEY, U_P VARCHAR(255) UNIQUE, N_P INT DEFAULT 1);
> ok

CREATE SCHEMA SCH;
> ok

CREATE TABLE SCH.TEST_S(ID_S INT PRIMARY KEY, U_S VARCHAR(255) UNIQUE, N_S INT DEFAULT 1);
> ok

SHOW TABLES;
> TABLE_NAME TABLE_SCHEMA
> ---------- ------------
> TEST_P     PUBLIC
> rows (ordered): 1

SHOW TABLES FROM PUBLIC;
> TABLE_NAME TABLE_SCHEMA
> ---------- ------------
> TEST_P     PUBLIC
> rows (ordered): 1

SHOW TABLES FROM SCH;
> TABLE_NAME TABLE_SCHEMA
> ---------- ------------
> TEST_S     SCH
> rows (ordered): 1

SHOW COLUMNS FROM TEST_P;
> FIELD TYPE         NULL KEY DEFAULT
> ----- ------------ ---- --- -------
> ID_P  INTEGER(10)  NO   PRI NULL
> U_P   VARCHAR(255) YES  UNI NULL
> N_P   INTEGER(10)  YES      1
> rows (ordered): 3

SHOW COLUMNS FROM TEST_S FROM SCH;
> FIELD TYPE         NULL KEY DEFAULT
> ----- ------------ ---- --- -------
> ID_S  INTEGER(10)  NO   PRI NULL
> U_S   VARCHAR(255) YES  UNI NULL
> N_S   INTEGER(10)  YES      1
> rows (ordered): 3

SHOW DATABASES;
> SCHEMA_NAME
> ------------------
> INFORMATION_SCHEMA
> PUBLIC
> SCH
> rows: 3

SHOW SCHEMAS;
> SCHEMA_NAME
> ------------------
> INFORMATION_SCHEMA
> PUBLIC
> SCH
> rows: 3

DROP TABLE TEST_P;
> ok

DROP SCHEMA SCH CASCADE;
> ok
