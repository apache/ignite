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
    scan, cache_create, scan_cursor_get_page, resource_close, cache_put_all,
)
from pyignite.connection import Connection

conn = Connection()
conn.connect('127.0.0.1', 10800)

cache_create(conn, 'my cache')

page_size = 10

cache_put_all(conn, 'my cache', {
        'key_{}'.format(v): v for v in range(page_size * 2)
    })

# {
#     'key_0': 0,
#     'key_1': 1,
#     'key_2': 2,
#     ... 20 elements in total...
#     'key_18': 18,
#     'key_19': 19
# }

result = scan(conn, 'my cache', page_size)
print(dict(result.value))
# {
#     'cursor': 1,
#     'data': {
#         'key_4': 4,
#         'key_2': 2,
#         'key_8': 8,
#         ... 10 elements on page...
#         'key_0': 0,
#         'key_7': 7
#     },
#     'more': True
# }

cursor = result.value['cursor']
result = scan_cursor_get_page(conn, cursor)
print(result.value)
# {
#     'data': {
#         'key_15': 15,
#         'key_17': 17,
#         'key_11': 11,
#         ... another 10 elements...
#         'key_19': 19,
#         'key_16': 16
#     },
#     'more': False
# }

result = scan_cursor_get_page(conn, cursor)
print(result.message)
# Failed to find resource with id: 1

resource_close(conn, cursor)
