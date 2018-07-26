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
    cache_create, cache_destroy, cache_put, cache_remove_keys,
)
from pyignite.connection import Connection
from pyignite.utils import hashcode

conn = Connection()
conn.connect('127.0.0.1', 10800)

cache_name = 'my cache'
hash_code = hashcode(cache_name)

cache_create(conn, cache_name)

from pyignite.datatypes import CharObject, ShortObject


cache_put(conn, hash_code, 'my key', 42)
# value ‘42’ takes 9 bytes of memory as a LongObject

cache_put(conn, hash_code, 'my key', 42, value_hint=ShortObject)
# value ‘42’ takes only 3 bytes as a ShortObject

cache_put(conn, hash_code, 'a', 1)
# ‘a’ is a key of type String

cache_put(conn, hash_code, 'a', 2, key_hint=CharObject)
# another key ‘a’ of type CharObject was created

# now let us delete both keys at once
cache_remove_keys(conn, hash_code, [
    'a',                # a default type key
    ('a', CharObject),  # a key of type CharObject
])

cache_destroy(conn, hash_code)
conn.close()
