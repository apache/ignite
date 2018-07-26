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
    cache_create, cache_destroy, cache_get, cache_put, cache_get_names
)
from pyignite.connection import Connection

conn = Connection()
conn.connect('127.0.0.1', 10800)

cache_create(conn, 'my cache')

result = cache_put(conn, 'my cache', 'my key', 42)
print(result.message)  # “Success”

result = cache_get(conn, 'my cache', 'my key')
print(result.value)  # “42”

result = cache_get(conn, 'my cache', 'non-existent key')
print(result.value)  # None

result = cache_get_names(conn, 'my cache')
print(result.value)  # ['my key']

cache_destroy(conn, 'my cache')
conn.close()
