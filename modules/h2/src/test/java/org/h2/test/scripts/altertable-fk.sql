-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- Check that constraints are properly renamed when we rename a column.

CREATE TABLE user_group (ID decimal PRIMARY KEY NOT NULL);
> ok

CREATE TABLE login_message (ID decimal PRIMARY KEY NOT NULL, user_group_id decimal);
> ok

ALTER TABLE login_message ADD CONSTRAINT FK_LOGIN_MESSAGE
FOREIGN KEY (user_group_id)
REFERENCES user_group(id) ON DELETE CASCADE;
> ok

ALTER TABLE login_message ALTER COLUMN user_group_id RENAME TO user_group_id2;
> ok

INSERT INTO user_group (ID) VALUES (1);
> update count: 1

DELETE FROM user_group;
> update count: 1
