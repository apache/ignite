..  Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

..      http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

.. _cache_props:

================
Cache Properties
================

The :mod:`~pyignite.datatypes.prop_codes` module contains a list of ordinal
values, that represent various cache settings.

Please refer to the `Apache Ignite Data Grid`_ documentation on cache
synchronization, rebalance, affinity and other cache configuration-related
matters.

+---------------------------------------+----------+----------+-------------------------------------------------------+
| Property                              | Ordinal  | Property | Description                                           |
| name                                  | value    | type     |                                                       |
+=======================================+==========+==========+=======================================================+
| Read/write cache properties, used to configure cache via :py:meth:`~pyignite.client.Client.create_cache` or         |
| :py:meth:`~pyignite.client.Client.get_or_create_cache`                                                              |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_NAME                             |        0 | str      | Cache name. This is the only *required* property.     |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_CACHE_MODE                       |        1 | int      | Cache mode: LOCAL=0, REPLICATED=1, PARTITIONED=2      |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_CACHE_ATOMICITY_MODE             |        2 | int      | Cache atomicity mode: TRANSACTIONAL=0, ATOMIC=1       |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_BACKUPS_NUMBER                   |        3 | int      | Number of backups                                     |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_WRITE_SYNCHRONIZATION_MODE       |        4 | int      | Write synchronization mode: FULL_SYNC=0,              |
|                                       |          |          | FULL_ASYNC=1, PRIMARY_SYNC=2                          |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_COPY_ON_READ                     |        5 | bool     | Copy-on-read                                          |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_READ_FROM_BACKUP                 |        6 | bool     | Read from backup                                      |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_DATA_REGION_NAME                 |      100 | str      | Data region name                                      |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_IS_ONHEAP_CACHE_ENABLED          |      101 | bool     | Is OnHeap cache enabled?                              |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_QUERY_ENTITIES                   |      200 | list     | A list of query entities (see `Query entity`_)        |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_QUERY_PARALLELISM                |      201 | int      | Query parallelism                                     |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_QUERY_DETAIL_METRIC_SIZE         |      202 | int      | Query detail metric size                              |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_SQL_SCHEMA                       |      203 | str      | SQL schema                                            |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_SQL_INDEX_INLINE_MAX_SIZE        |      204 | int      | SQL index inline maximum size                         |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_SQL_ESCAPE_ALL                   |      205 | bool     | Turns on SQL escapes                                  |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_MAX_QUERY_ITERATORS              |      206 | int      | Maximum number of query iterators                     |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_REBALANCE_MODE                   |      300 | int      | Rebalance mode: SYNC=0, ASYNC=1, NONE=2               |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_REBALANCE_DELAY                  |      301 | int      | Rebalance delay (ms)                                  |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_REBALANCE_TIMEOUT                |      302 | int      | Rebalance timeout (ms)                                |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_REBALANCE_BATCH_SIZE             |      303 | int      | Rebalance batch size                                  |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_REBALANCE_BATCHES_PREFETCH_COUNT |      304 | int      | Rebalance batches prefetch count                      |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_REBALANCE_ORDER                  |      305 | int      | Rebalance order                                       |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_REBALANCE_THROTTLE               |      306 | int      | Rebalance throttle (ms)                               |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_GROUP_NAME                       |      400 | str      | Group name                                            |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_CACHE_KEY_CONFIGURATION          |      401 | list     | Cache key configuration (see `Cache key`_)            |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_DEFAULT_LOCK_TIMEOUT             |      402 | int      | Default lock timeout (ms)                             |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_MAX_CONCURRENT_ASYNC_OPERATIONS  |      403 | int      | Maximum number of concurrent asynchronous operations  |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_PARTITION_LOSS_POLICY            |      404 | int      | Partition loss policy: READ_ONLY_SAFE=0,              |
|                                       |          |          | READ_ONLY_ALL=1, READ_WRITE_SAFE=2, READ_WRITE_ALL=3, |
|                                       |          |          | IGNORE=4                                              |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_EAGER_TTL                        |      405 | bool     | Eager TTL                                             |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_STATISTICS_ENABLED               |      406 | bool     | Statistics enabled                                    |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| Read-only cache properties. Can not be set, but only retrieved via :py:meth:`~pyignite.cache.Cache.settings`        |
+---------------------------------------+----------+----------+-------------------------------------------------------+
| PROP_INVALIDATE                       |       -1 | bool     | Invalidate                                            |
+---------------------------------------+----------+----------+-------------------------------------------------------+

Query entity
------------

A dict with all ot the following keys:

- `table_name`: SQL table name,
- `key_field_name`: name of the key field,
- `key_type_name`: name of the key type (Java type or complex object),
- `value_field_name`: name of the value field,
- `value_type_name`: name of the value type,
- `field_name_aliases`: a list of 0 or more dicts of aliases
  (see `Field name alias`_),
- `query_fields`: a list of 0 or more query field names (see `Query field`_),
- `query_indexes`: a list of 0 or more query indexes (see `Query index`_).

Field name alias
================

- `field_name`: field name,
- `alias`: alias (str).

Query field
===========

- `name`: field name,
- `type_name`: name of Java type or complex object,
- `is_key_field`: (optional) boolean value, `False` by default,
- `is_notnull_constraint_field`: boolean value,
- `default_value`: (optional) anything that can be converted to `type_name`
  type. `None` (:py:class:`~pyignite.datatypes.null_object.Null`) by default,
- `precision` − (optional) decimal precision: total number of digits
  in decimal value. Defaults to -1 (use cluster default). Ignored for
  non-decimal SQL types (other than `java.math.BigDecimal`),
- `scale` − (optional) decimal precision: number of digits after the decimal
  point. Defaults to -1 (use cluster default). Ignored for non-decimal SQL
  types.

Query index
===========

- `index_name`: index name,
- `index_type`: index type code as an integer value in unsigned byte range,
- `inline_size`: integer value,
- `fields`: a list of 0 or more indexed fields (see `Fields`_).

Fields
======

- `name`: field name,
- `is_descending`: (optional) boolean value, `False` by default.

Cache key
---------

A dict of the following format:

- `type_name`: name of the complex object,
- `affinity_key_field_name`: name of the affinity key field.

.. _Apache Ignite Data Grid: https://apacheignite.readme.io/docs/data-grid
