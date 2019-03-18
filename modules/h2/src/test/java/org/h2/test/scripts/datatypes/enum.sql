-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
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

select * from card order by suit;
> RANK SUIT
> ---- ------
> 4    null
> 3    hearts
> 0    clubs

insert into card (rank, suit) values (8, 'diamonds'), (10, 'clubs'), (7, 'hearts');
> update count: 3

select suit, count(rank) from card group by suit order by suit, count(rank);
> SUIT     COUNT(RANK)
> -------- -----------
> null     1
> hearts   2
> clubs    2
> diamonds 1

select rank from card where suit = 'diamonds';
> RANK
> ----
> 8

select column_type from information_schema.columns where COLUMN_NAME = 'SUIT';
> COLUMN_TYPE
> ------------------------------------------
> ENUM('hearts','clubs','spades','diamonds')
> rows: 1

--- ENUM integer-based operations

select rank from card where suit = 1;
> RANK
> ----
> 0
> 10

insert into card (rank, suit) values(5, 2);
> update count: 1

select * from card where rank = 5;
> RANK SUIT
> ---- ------
> 5    spades

--- ENUM edge cases

insert into card (rank, suit) values(6, ' ');
> exception

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', 'clubs');
> exception

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', '');
> exception

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
> exception

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1

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

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1

select rank from card where suit in ('clubs');
> RANK
> ----
> 0
> 1

drop table card;
> ok

drop type CARD_SUIT;
> ok

