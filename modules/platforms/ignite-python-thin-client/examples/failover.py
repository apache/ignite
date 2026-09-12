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

from pyignite import Client
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_CACHE_MODE, PROP_BACKUPS_NUMBER
from pyignite.exceptions import SocketError


nodes = [
    ('127.0.0.1', 10800),
    ('127.0.0.1', 10801),
    ('127.0.0.1', 10802),
]


client = Client(timeout=4.0)
with client.connect(nodes):
    print('Connected')

    my_cache = client.get_or_create_cache({
        PROP_NAME: 'my_cache',
        PROP_CACHE_MODE: CacheMode.PARTITIONED,
        PROP_BACKUPS_NUMBER: 2,
    })
    my_cache.put('test_key', 0)
    test_value = 0

    # abstract main loop
    while True:
        try:
            # do the work
            test_value = my_cache.get('test_key') or 0
            my_cache.put('test_key', test_value + 1)
        except (OSError, SocketError) as e:
            # recover from error (repeat last command, check data
            # consistency or just continue − depends on the task)
            print(f'Error: {e}')
            print(f'Last value: {test_value}')
            print('Reconnecting')

# Connected
# Error: Connection broken.
# Last value: 2650
# Reconnecting
# Error: Connection broken.
# Last value: 10204
# Reconnecting
# Error: Connection broken.
# Last value: 18932
# Reconnecting
# Traceback (most recent call last):
#   ...
# pyignite.exceptions.ReconnectError: Can not reconnect: out of nodes.
