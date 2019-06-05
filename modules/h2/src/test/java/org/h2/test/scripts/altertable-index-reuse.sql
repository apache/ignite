-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE "domains" ("id" bigint NOT NULL auto_increment PRIMARY KEY);
> ok

CREATE TABLE "users" ("id" bigint NOT NULL auto_increment PRIMARY KEY,"username" varchar_ignorecase(255),"domain" bigint,"desc" varchar_ignorecase(255));
> ok

-- adds constraint on (domain,username) and generates unique index domainusername_key_INDEX_xxx
ALTER TABLE "users" ADD CONSTRAINT "domainusername_key" UNIQUE ("domain","username");
> ok

-- adds foreign key on domain - if domainusername_key didn't exist it would create unique index on domain, but it reuses the existing index
ALTER TABLE "users"  ADD CONSTRAINT "udomain_fkey" FOREIGN KEY ("domain") REFERENCES "domains"("id") ON DELETE RESTRICT;
> ok

-- now we drop the domainusername_key, but domainusername_key_INDEX_xxx is used by udomain_fkey and was not being dropped
-- this was an issue, because it's a unique index and still enforcing constraint on (domain,username)
ALTER TABLE "users" DROP CONSTRAINT "domainusername_key";
> ok

insert into "domains" ("id") VALUES (1);
> update count: 1

insert into "users" ("username","domain","desc") VALUES ('test',1,'first user');
> update count: 1

-- should work,because we dropped domainusername_key, but failed: Unique index or primary key violation
INSERT INTO "users" ("username","domain","desc") VALUES ('test',1,'second user');
> update count: 1
