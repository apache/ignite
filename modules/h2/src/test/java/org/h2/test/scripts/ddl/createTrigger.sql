-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE COUNT(X INT);
> ok

CREATE FORCE TRIGGER T_COUNT BEFORE INSERT ON COUNT CALL "com.Unknown";
> ok

INSERT INTO COUNT VALUES(NULL);
> exception ERROR_CREATING_TRIGGER_OBJECT_3

DROP TRIGGER T_COUNT;
> ok

CREATE TABLE ITEMS(ID INT CHECK ID < SELECT MAX(ID) FROM COUNT);
> ok

insert into items values(DEFAULT);
> update count: 1

DROP TABLE COUNT;
> exception CANNOT_DROP_2

insert into items values(DEFAULT);
> update count: 1

drop table items, count;
> ok

-- ---------------------------------------------------------------------------
-- PostgreSQL syntax tests
-- ---------------------------------------------------------------------------

set mode postgresql;
> ok

CREATE TABLE COUNT(X INT);
> ok

INSERT INTO COUNT VALUES(1);
> update count: 1

CREATE FORCE TRIGGER T_COUNT BEFORE INSERT OR UPDATE ON COUNT CALL "com.Unknown";
> ok

INSERT INTO COUNT VALUES(NULL);
> exception ERROR_CREATING_TRIGGER_OBJECT_3

UPDATE COUNT SET X=2 WHERE X=1;
> exception ERROR_CREATING_TRIGGER_OBJECT_3
