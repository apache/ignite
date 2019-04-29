-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

----------------
--- ENUM support
----------------

--- ENUM basic operations

create table card (rank int, suit enum('hearts', 'clubs', 'spades'));
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts'), (4, NULL);
> update count: 3

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

select * from card;
> RANK SUIT
> ---- ------
> 0    clubs
> 3    hearts
> 4    null
> rows: 3

@reconnect

select suit from card where rank = 0;
>> clubs

alter table card alter column suit enum('a', 'b', 'c', 'd');
> exception ENUM_VALUE_NOT_PERMITTED

alter table card alter column suit enum('''none''', 'hearts', 'clubs', 'spades', 'diamonds');
> ok

select * from card order by suit;
> RANK SUIT
> ---- ------
> 4    null
> 3    hearts
> 0    clubs
> rows (ordered): 3

insert into card (rank, suit) values (8, 'diamonds'), (10, 'clubs'), (7, 'hearts');
> update count: 3

select suit, count(rank) from card group by suit order by suit, count(rank);
> SUIT     COUNT(RANK)
> -------- -----------
> null     1
> hearts   2
> clubs    2
> diamonds 1
> rows (ordered): 4

select rank from card where suit = 'diamonds';
>> 8

select column_type from information_schema.columns where COLUMN_NAME = 'SUIT';
>> ENUM('''none''', 'hearts', 'clubs', 'spades', 'diamonds')

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', 'long_enum_value_of_128_chars_00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');
> ok

insert into card (rank, suit) values (11, 'long_enum_value_of_128_chars_00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');
> update count: 1

--- ENUM integer-based operations

select rank from card where suit = 1;
> RANK
> ----
> 0
> 10
> rows: 2

insert into card (rank, suit) values(5, 2);
> update count: 1

select * from card where rank = 5;
> RANK SUIT
> ---- ------
> 5    spades
> rows: 1

--- ENUM edge cases

insert into card (rank, suit) values(6, ' ');
> exception ENUM_VALUE_NOT_PERMITTED

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', 'clubs');
> exception ENUM_DUPLICATE

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', '');
> exception ENUM_EMPTY

drop table card;
> ok

--- ENUM as custom user data type

create type CARD_SUIT as enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

create table card (rank int, suit CARD_SUIT);
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts');
> update count: 2

select * from card;
> RANK SUIT
> ---- ------
> 0    clubs
> 3    hearts
> rows: 2

drop table card;
> ok

drop type CARD_SUIT;
> ok

--- ENUM in primary key with another column
create type CARD_SUIT as enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

create table card (rank int, suit CARD_SUIT, primary key(rank, suit));
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts'), (1, 'clubs');
> update count: 3

insert into card (rank, suit) values (0, 'clubs');
> exception DUPLICATE_KEY_1

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1
> rows: 2

drop table card;
> ok

drop type CARD_SUIT;
> ok

--- ENUM with index
create type CARD_SUIT as enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

create table card (rank int, suit CARD_SUIT, primary key(rank, suit));
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts'), (1, 'clubs');
> update count: 3

create index idx_card_suite on card(`suit`);
> ok

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1
> rows: 2

select rank from card where suit in ('clubs');
> RANK
> ----
> 0
> 1
> rows: 2

insert into card values (2, 'diamonds');
> update count: 1

select rank from card where suit in ('clubs', 'hearts');
> RANK
> ----
> 0
> 1
> 3
> rows: 3

select rank from card where suit in ('clubs', 'hearts') or suit = 'diamonds';
> RANK
> ----
> 0
> 1
> 2
> 3
> rows: 4

drop table card;
> ok

drop type CARD_SUIT;
> ok

CREATE TABLE TEST(ID INT, E1 ENUM('A', 'B') DEFAULT 'A', E2 ENUM('C', 'D') DEFAULT 'C' ON UPDATE 'D');
> ok

INSERT INTO TEST(ID) VALUES (1);
> update count: 1

SELECT * FROM TEST;
> ID E1 E2
> -- -- --
> 1  A  C
> rows: 1

UPDATE TEST SET E1 = 'B';
> update count: 1

SELECT * FROM TEST;
> ID E1 E2
> -- -- --
> 1  B  D
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(E ENUM('A', 'B'));
> ok

INSERT INTO TEST VALUES ('B');
> update count: 1

CREATE VIEW V AS SELECT * FROM TEST;
> ok

SELECT * FROM V;
>> B

CREATE VIEW V1 AS SELECT E + 2 AS E FROM TEST;
> ok

SELECT * FROM V1;
>> 3

CREATE VIEW V2 AS SELECT E + E AS E FROM TEST;
> ok

SELECT * FROM V2;
>> 2

CREATE VIEW V3 AS SELECT -E AS E FROM TEST;
> ok

SELECT * FROM V3;
>> -1

SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'E' ORDER BY TABLE_NAME;
> TABLE_CATALOG TABLE_SCHEMA TABLE_NAME COLUMN_NAME ORDINAL_POSITION DOMAIN_CATALOG DOMAIN_SCHEMA DOMAIN_NAME COLUMN_DEFAULT IS_NULLABLE DATA_TYPE CHARACTER_MAXIMUM_LENGTH CHARACTER_OCTET_LENGTH NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE DATETIME_PRECISION INTERVAL_TYPE INTERVAL_PRECISION CHARACTER_SET_NAME COLLATION_NAME TYPE_NAME NULLABLE IS_COMPUTED SELECTIVITY CHECK_CONSTRAINT SEQUENCE_NAME REMARKS SOURCE_DATA_TYPE COLUMN_TYPE    COLUMN_ON_UPDATE IS_VISIBLE
> ------------- ------------ ---------- ----------- ---------------- -------------- ------------- ----------- -------------- ----------- --------- ------------------------ ---------------------- ----------------- ----------------------- ------------- ------------------ ------------- ------------------ ------------------ -------------- --------- -------- ----------- ----------- ---------------- ------------- ------- ---------------- -------------- ---------------- ----------
> SCRIPT        PUBLIC       TEST       E           1                null           null          null        null           YES         1111      1                        1                      1                 10                      0             null               null          null               Unicode            OFF            ENUM      1        FALSE       50                           null                  null             ENUM('A', 'B') null             TRUE
> SCRIPT        PUBLIC       V          E           1                null           null          null        null           YES         1111      1                        1                      1                 10                      0             null               null          null               Unicode            OFF            ENUM      1        FALSE       50                           null                  null             ENUM('A', 'B') null             TRUE
> SCRIPT        PUBLIC       V1         E           1                null           null          null        null           YES         4         10                       10                     10                10                      0             null               null          null               Unicode            OFF            INTEGER   1        FALSE       50                           null                  null             INTEGER        null             TRUE
> SCRIPT        PUBLIC       V2         E           1                null           null          null        null           YES         4         10                       10                     10                10                      0             null               null          null               Unicode            OFF            INTEGER   1        FALSE       50                           null                  null             INTEGER        null             TRUE
> SCRIPT        PUBLIC       V3         E           1                null           null          null        null           YES         4         10                       10                     10                10                      0             null               null          null               Unicode            OFF            INTEGER   1        FALSE       50                           null                  null             INTEGER        null             TRUE
> rows (ordered): 5

DROP VIEW V;
> ok

DROP VIEW V1;
> ok

DROP VIEW V2;
> ok

DROP VIEW V3;
> ok

DROP TABLE TEST;
> ok

SELECT CAST (2 AS ENUM('a', 'b', 'c', 'd'));
>> c

CREATE TABLE TEST(E ENUM('a', 'b'));
> ok

EXPLAIN SELECT * FROM TEST WHERE E = 'a';
>> SELECT "TEST"."E" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ WHERE "E" = 'a'

INSERT INTO TEST VALUES ('a');
> update count: 1

(SELECT * FROM TEST A) UNION ALL (SELECT * FROM TEST A);
> E
> -
> a
> a
> rows: 2

(SELECT * FROM TEST A) MINUS (SELECT * FROM TEST A);
> E
> -
> rows: 0

DROP TABLE TEST;
> ok
