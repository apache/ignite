-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

INSERT INTO TEST VALUES(2, STRINGDECODE('abcsond\344rzeich\344 ') || char(22222) || STRINGDECODE(' \366\344\374\326\304\334\351\350\340\361!'));
> update count: 1

script nopasswords nosettings;
> SCRIPT
> -------------------------------------------------------------------------------------------------------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE PUBLIC.TEST ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);
> CREATE MEMORY TABLE PUBLIC.TEST( ID INT NOT NULL, NAME VARCHAR(255) );
> CREATE USER IF NOT EXISTS SA PASSWORD '' ADMIN;
> INSERT INTO PUBLIC.TEST(ID, NAME) VALUES (2, STRINGDECODE('abcsond\u00e4rzeich\u00e4 \u56ce \u00f6\u00e4\u00fc\u00d6\u00c4\u00dc\u00e9\u00e8\u00e0\u00f1!'));
> rows: 5
