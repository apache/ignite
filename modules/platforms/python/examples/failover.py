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

from pyignite.api import (
    hashcode, cache_get, cache_put, cache_get_or_create_with_config,
)
from pyignite.connection import Connection
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import *


MAX_ERRORS = 20
CACHE_NAME = 'failover_test'
nodes = [
    ('127.0.0.1', 10800),
    ('127.0.0.1', 10801),
    ('127.0.0.1', 10802),
]

node_idx = err_count = 0
conn = Connection(timeout=3.0)

conn.connect(*nodes[node_idx])

while True:
    try:
        # reconnect
        conn.connect(*nodes[node_idx])
        print('Connected to node {}'.format(node_idx))

        # proceed with initializing or modifying data
        while True:
            cache_get_or_create_with_config(conn, {
                PROP_NAME: CACHE_NAME,
                PROP_CACHE_MODE: CacheMode.REPLICATED,
            })
            result = cache_get(conn, hashcode(CACHE_NAME), 'test_value')
            cache_put(
                conn,
                hashcode(CACHE_NAME),
                'test_value',
                result.value + 1 if result.value else 1
            )
    except Exception as e:
        # count errors
        err_count += 1
        if err_count > MAX_ERRORS:
            print('Too many disconnects! Exiting.')
            break
        # switch to another node
        node_idx = node_idx + 1
        if node_idx >= len(nodes):
            node_idx = 0
        print(
            '“{}” just happened; switching to node {}.'.format(e, node_idx)
        )

# Connected to node 0
# “Socket connection broken.” just happened; switching to node 1.
# Connected to node 1
# “Socket connection broken.” just happened; switching to node 2.
# “[Errno 111] Connection refused” just happened; switching to node 0.
# Connected to node 0
