create table if not exists Accounts (`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, val BIGINT);
create table if not exists Tellers (`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, val BIGINT);
create table if not exists Branches (`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, val BIGINT);
create table if not exists History (`id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, aid BIGINT, tid BIGINT, bid BIGINT, delta BIGINT);
create index v8_2 using btree on Accounts(`id`);