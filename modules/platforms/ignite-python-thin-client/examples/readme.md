# Examples

This directory contains the following example files:

- `async_key_value` - asynchronous key-value operations,
- `async_sql` - asynchronous SQL operations,
- `binary_basics.py` − basic operations with Complex objects,
- `create_binary.py` − create SQL row with key-value operation,
- `expiry_policy.py` - the expiration policy for caches for synchronous and asynchronous operations is demonstrated,
- `failover.py` − fail-over connection to Ignite cluster,
- `get_and_put.py` − basic key-value operations,
- `get_and_put_complex.py` − key-value operations with different value  and key types,
- `migrate_binary.py` − work with Complex object schemas,
- `read_binary.py` − creates caches and fills them with data through SQL queries, demonstrates working with binary objects,
- `scans.py` − cache scan operation,
- `sql.py` − use Ignite SQL,
- `type_hints.py` − type hints.

For the explanation of the examples please refer to the
[Examples of usage](https://apache-ignite-binary-protocol-client.readthedocs.io/en/latest/examples.html)
section of the `pyignite` documentation.

You can start Apache Ignite locally for running examples using `docker` and `docker-compose`
```bash
cd ./examples
docker-compose up
```
