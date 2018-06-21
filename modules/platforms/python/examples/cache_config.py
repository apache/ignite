# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyignite.connection import Connection
from pyignite.api import (
    cache_create, cache_create_with_config, cache_destroy,
    cache_get_configuration, hashcode,
)
from pyignite.datatypes.prop_codes import *

conn = Connection()
conn.connect('127.0.0.1', 10800)

cache_name = 'my cache'
hash_code = hashcode(cache_name)

cache_create(conn, cache_name)

result = cache_get_configuration(conn, hash_code)
print(result.value)
# OrderedDict([
#     ('length', 122),
#     ('backups_number', 1),
#     ('cache_mode', 0),
#     ('copy_on_read', True),
#     ('data_region_name', None),
#     ('eager_ttl', True),
#     ('statistics_enabled', False),
#     ('group_name', None),
#     ('invalidate', 0),
#     ('default_lock_timeout', 2147483648000),
#     ('max_query_iterators', 1024),
#     ('name', 'my cache'),
#     ('is_onheap_cache_enabled', False),
#     ('partition_loss_policy', 4),
#     ('query_detail_metric_size', 0),
#     ('query_parallelism', 1),
#     ('read_from_backup', True),
#     ('rebalance_batch_size', 524288),
#     ('rebalance_batches_prefetch_count', 2),
#     ('rebalance_delay', 0),
#     ('rebalance_mode', 1),
#     ('rebalance_order', 0),
#     ('rebalance_throttle', 0),
#     ('rebalance_timeout', 10000),
#     ('sql_escape_all', False),
#     ('sql_index_inline_max_size', -1),
#     ('sql_schema', None),
#     ('write_synchronization_mode', 2),
#     ('cache_key_configuration', []),
#     ('query_entity', [])
# ])

cache_name = 'my_special_cache'

cache_create_with_config(conn, {
        PROP_NAME: cache_name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'name': '123_key',
                'type_name': 'blah',
                'is_key_field': False,
                'is_notnull_constraint_field': False,
            }
        ],
    })

cache_destroy(conn, hash_code)
conn.close()
