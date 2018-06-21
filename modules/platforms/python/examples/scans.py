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
    scan, cache_create, scan_cursor_get_page, resource_close, cache_put_all,
    hashcode,
)

conn = Connection()
conn.connect('127.0.0.1', 10800)

cache_name = 'my cache'
hash_code = hashcode(cache_name)

cache_create(conn, cache_name)

page_size = 10

cache_put_all(conn, hash_code, {
        'key_{}'.format(v): v for v in range(page_size * 2)
    })

# {
#     'key_0': 0,
#     'key_1': 1,
#     'key_2': 2,
#     ...
#     'key_18': 18,
#     'key_19': 19,
# }

result = scan(conn, hash_code, page_size)
print(dict(result.value))

cursor = result.value['cursor']
result = scan_cursor_get_page(conn, cursor)
print(result.value)

resource_close(conn, cursor)

result = scan_cursor_get_page(conn, cursor)
print(result.message)
