CREATE TABLE city (id INT PRIMARY KEY, name VARCHAR, region VARCHAR) WITH "atomicity=transactional,cache_name=s.city,value_type=s.city"
ALTER TABLE IF EXISTS city ADD IF NOT EXISTS sharding_id BIGINT
ALTER TABLE IF EXISTS city ADD IF NOT EXISTS population INT
