-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table FOO(id integer primary key);
> ok

create table BAR(fooId integer);
> ok

alter table bar add foreign key (fooId) references foo (id);
> ok

truncate table bar;
> ok

truncate table foo;
> exception CANNOT_TRUNCATE_1

drop table bar, foo;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');
> update count: 2

TRUNCATE TABLE TEST;
> ok

SELECT * FROM TEST;
> ID NAME
> -- ----
> rows: 0

DROP TABLE TEST;
> ok

CREATE TABLE PARENT(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

CREATE TABLE CHILD(PARENTID INT, FOREIGN KEY(PARENTID) REFERENCES PARENT(ID), NAME VARCHAR);
> ok

TRUNCATE TABLE CHILD;
> ok

TRUNCATE TABLE PARENT;
> exception CANNOT_TRUNCATE_1

DROP TABLE CHILD;
> ok

DROP TABLE PARENT;
> ok

CREATE SEQUENCE SEQ2;
> ok

CREATE SEQUENCE SEQ3;
> ok

CREATE TABLE TEST(
    ID1 BIGINT AUTO_INCREMENT NOT NULL,
    ID2 BIGINT NOT NULL DEFAULT NEXT VALUE FOR SEQ2 NULL_TO_DEFAULT SEQUENCE SEQ2,
    ID3 BIGINT NOT NULL DEFAULT NEXT VALUE FOR SEQ3 NULL_TO_DEFAULT,
    VALUE INT NOT NULL);
> ok

INSERT INTO TEST(VALUE) VALUES (1), (2);
> update count: 2

SELECT * FROM TEST ORDER BY VALUE;
> ID1 ID2 ID3 VALUE
> --- --- --- -----
> 1   1   1   1
> 2   2   2   2
> rows (ordered): 2

TRUNCATE TABLE TEST;
> ok

INSERT INTO TEST(VALUE) VALUES (1), (2);
> update count: 2

SELECT * FROM TEST ORDER BY VALUE;
> ID1 ID2 ID3 VALUE
> --- --- --- -----
> 3   3   3   1
> 4   4   4   2
> rows (ordered): 2

TRUNCATE TABLE TEST CONTINUE IDENTITY;
> ok

INSERT INTO TEST(VALUE) VALUES (1), (2);
> update count: 2

SELECT * FROM TEST ORDER BY VALUE;
> ID1 ID2 ID3 VALUE
> --- --- --- -----
> 5   5   5   1
> 6   6   6   2
> rows (ordered): 2

TRUNCATE TABLE TEST RESTART IDENTITY;
> ok

INSERT INTO TEST(VALUE) VALUES (1), (2);
> update count: 2

SELECT * FROM TEST ORDER BY VALUE;
> ID1 ID2 ID3 VALUE
> --- --- --- -----
> 1   1   7   1
> 2   2   8   2
> rows (ordered): 2

DROP TABLE TEST;
> ok

DROP SEQUENCE SEQ3;
> ok
