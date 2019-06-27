#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from pygridgain.api import (
    scan, scan_cursor_get_page, resource_close, cache_put_all,
)


def test_scan(client, cache):

    conn = client.random_node
    page_size = 10

    result = cache_put_all(conn, cache, {
        'key_{}'.format(v): v for v in range(page_size * 2)
    })
    assert result.status == 0

    result = scan(conn, cache, page_size)
    assert result.status == 0
    assert len(result.value['data']) == page_size
    assert result.value['more'] is True

    cursor = result.value['cursor']

    result = scan_cursor_get_page(conn, cursor)
    assert result.status == 0
    assert len(result.value['data']) == page_size
    assert result.value['more'] is False

    result = scan_cursor_get_page(conn, cursor)
    assert result.status != 0


def test_close_resource(client, cache):

    conn = client.random_node
    page_size = 10

    result = cache_put_all(conn, cache, {
        'key_{}'.format(v): v for v in range(page_size * 2)
    })
    assert result.status == 0

    result = scan(conn, cache, page_size)
    assert result.status == 0
    assert len(result.value['data']) == page_size
    assert result.value['more'] is True

    cursor = result.value['cursor']

    result = resource_close(conn, cursor)
    assert result.status == 0

    result = scan_cursor_get_page(conn, cursor)
    assert result.status != 0
