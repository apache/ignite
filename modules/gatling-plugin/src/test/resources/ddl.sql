CREATE TABLE City (id int primary key, name varchar, region varchar)
ALTER TABLE IF EXISTS City ADD IF NOT EXISTS sharding_id BIGINT
ALTER TABLE IF EXISTS City ADD IF NOT EXISTS population INT
