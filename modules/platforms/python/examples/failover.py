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
from pyignite.exceptions import SocketError


nodes = [
    ('127.0.0.1', 10800),
    ('127.0.0.1', 10801),
    ('127.0.0.1', 10802),
]

conn = Connection(timeout=4.0)
conn.connect(nodes)
print('Connected to {}'.format(conn))

while True:
    try:
        my_cache = conn.get_or_create_cache('my_cache')
        test_value = my_cache.get('test_key')
        my_cache.put('test_key', test_value + 1 if test_value else 1)
    except (OSError, SocketError) as e:
        print('Error: {}'.format(e))
        conn.reconnect()
        print('Reconnected to {}'.format(conn))
