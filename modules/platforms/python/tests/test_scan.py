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
    scan, scan_cursor_get_page, resource_close, cache_put_all,
)


def test_scan(client, cache):

    page_size = 10

    result = cache_put_all(client, cache, {
        'key_{}'.format(v): v for v in range(page_size * 2)
    })
    assert result.status == 0

    result = scan(client, cache, page_size)
    assert result.status == 0
    assert len(result.value['data']) == page_size
    assert result.value['more'] is True

    cursor = result.value['cursor']

    result = scan_cursor_get_page(client, cursor)
    assert result.status == 0
    assert len(result.value['data']) == page_size
    assert result.value['more'] is False

    result = scan_cursor_get_page(client, cursor)
    assert result.status != 0


def test_close_resource(client, cache):

    page_size = 10

    result = cache_put_all(client, cache, {
        'key_{}'.format(v): v for v in range(page_size * 2)
    })
    assert result.status == 0

    result = scan(client, cache, page_size)
    assert result.status == 0
    assert len(result.value['data']) == page_size
    assert result.value['more'] is True

    cursor = result.value['cursor']

    result = resource_close(client, cursor)
    assert result.status == 0

    result = scan_cursor_get_page(client, cursor)
    assert result.status != 0
