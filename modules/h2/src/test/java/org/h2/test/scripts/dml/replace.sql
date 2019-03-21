-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TABLE_WORD (
    WORD_ID int(11) NOT NULL AUTO_INCREMENT,
    WORD varchar(128) NOT NULL,
    PRIMARY KEY (WORD_ID)
);
> ok

REPLACE INTO TABLE_WORD(WORD) VALUES ('aaaaaaaaaa');
> update count: 1

REPLACE INTO TABLE_WORD(WORD) VALUES ('bbbbbbbbbb');
> update count: 1

REPLACE INTO TABLE_WORD(WORD_ID, WORD) VALUES (3, 'cccccccccc');
> update count: 1

SELECT WORD FROM TABLE_WORD where WORD_ID = 1;
>> aaaaaaaaaa

REPLACE INTO TABLE_WORD(WORD_ID, WORD) VALUES (1, 'REPLACED');
> update count: 2

SELECT WORD FROM TABLE_WORD where WORD_ID = 1;
>> REPLACED

REPLACE INTO TABLE_WORD(WORD) SELECT 'dddddddddd';
> update count: 1

SELECT WORD FROM TABLE_WORD where WORD_ID = 4;
>> dddddddddd

REPLACE INTO TABLE_WORD(WORD_ID, WORD) SELECT 1, 'REPLACED2';
> update count: 2

SELECT WORD FROM TABLE_WORD where WORD_ID = 1;
>> REPLACED2

DROP TABLE TABLE_WORD;
> ok
