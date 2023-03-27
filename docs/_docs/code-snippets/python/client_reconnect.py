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

#tag::example-block[]
from pyignite import Client
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import *
from pyignite.exceptions import SocketError

nodes = [
    ('127.0.0.1', 10800),
    ('217.29.2.1', 10800),
    ('200.10.33.1', 10800),
]

client = Client(timeout=40.0)
client.connect(nodes)
print('Connected to {}'.format(client))

my_cache = client.get_or_create_cache({
    PROP_NAME: 'my_cache',
    PROP_CACHE_MODE: CacheMode.REPLICATED,
})
my_cache.put('test_key', 0)

# Abstract main loop
while True:
    try:
        # Do the work
        test_value = my_cache.get('test_key')
        my_cache.put('test_key', test_value + 1)
    except (OSError, SocketError) as e:
        # Recover from error (repeat last command, check data
        # consistency or just continue − depends on the task)
        print('Error: {}'.format(e))
        print('Last value: {}'.format(my_cache.get('test_key')))
        print('Reconnected to {}'.format(client))
#end::example-block[]
