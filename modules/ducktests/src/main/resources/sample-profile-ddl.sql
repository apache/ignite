CREATE TABLE transaction (id INT, region VARCHAR, PRIMARY KEY(id)) WITH "atomicity=transactional,cache_name=transaction,value_type=transaction"
ALTER TABLE IF EXISTS transaction ADD IF NOT EXISTS amount INT
CREATE INDEX IF NOT EXISTS transaction_search_by_region  ON transaction (region DESC)
