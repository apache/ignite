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

from datetime import datetime

from pyignite.api import *
from pyignite.datatypes import (
    CollectionObject, IntObject, MapObject, TimestampObject,
)


def test_put_get(client, cache):

    result = cache_put(client, cache, 'my_key', 5)
    assert result.status == 0

    result = cache_get(client, cache, 'my_key')
    assert result.status == 0
    assert result.value == 5


def test_get_all(client, cache):

    result = cache_get_all(client, cache, ['key_1', 2, (3, IntObject)])
    assert result.status == 0
    assert result.value == {}

    cache_put(client, cache, 'key_1', 4)
    cache_put(client, cache, 3, 18, key_hint=IntObject)

    result = cache_get_all(client, cache, ['key_1', 2, (3, IntObject)])
    assert result.status == 0
    assert result.value == {'key_1': 4, 3: 18}


def test_put_all(client, cache):

    test_dict = {
        1: 2,
        'key_1': 4,
        (3, IntObject): 18,
    }
    test_keys = ['key_1', 1, 3]

    result = cache_put_all(client, cache, test_dict)
    assert result.status == 0

    result = cache_get_all(client, cache, test_keys)
    assert result.status == 0
    assert len(test_dict) == 3

    for key in result.value:
        assert key in test_keys


def test_contains_key(client, cache):

    cache_put(client, cache, 'test_key', 42)

    result = cache_contains_key(client, cache, 'test_key')
    assert result.value is True

    result = cache_contains_key(client, cache, 'non-existant-key')
    assert result.value is False


def test_contains_keys(client, cache):

    cache_put(client, cache, 5, 6)
    cache_put(client, cache, 'test_key', 42)

    result = cache_contains_keys(client, cache, [5, 'test_key'])
    assert result.value is True

    result = cache_contains_keys(client, cache, [5, 'non-existent-key'])
    assert result.value is False


def test_get_and_put(client, cache):

    result = cache_get_and_put(client, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is None

    result = cache_get(client, cache, 'test_key')
    assert result.status == 0
    assert result.value is 42

    result = cache_get_and_put(client, cache, 'test_key', 1234)
    assert result.status == 0
    assert result.value == 42


def test_get_and_replace(client, cache):

    result = cache_get_and_replace(client, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is None

    result = cache_get(client, cache, 'test_key')
    assert result.status == 0
    assert result.value is None

    cache_put(client, cache, 'test_key', 42)

    result = cache_get_and_replace(client, cache, 'test_key', 1234)
    assert result.status == 0
    assert result.value == 42


def test_get_and_remove(client, cache):

    result = cache_get_and_remove(client, cache, 'test_key')
    assert result.status == 0
    assert result.value is None

    cache_put(client, cache, 'test_key', 42)

    result = cache_get_and_remove(client, cache, 'test_key')
    assert result.status == 0
    assert result.value == 42


def test_put_if_absent(client, cache):

    result = cache_put_if_absent(client, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is True

    result = cache_put_if_absent(client, cache, 'test_key', 1234)
    assert result.status == 0
    assert result.value is False


def test_get_and_put_if_absent(client, cache):

    result = cache_get_and_put_if_absent(client, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is None

    result = cache_get_and_put_if_absent(client, cache, 'test_key', 1234)
    assert result.status == 0
    assert result.value == 42

    result = cache_get_and_put_if_absent(client, cache, 'test_key', 5678)
    assert result.status == 0
    assert result.value == 42


def test_replace(client, cache):

    result = cache_replace(client, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is False

    cache_put(client, cache, 'test_key', 1234)

    result = cache_replace(client, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is True

    result = cache_get(client, cache, 'test_key')
    assert result.status == 0
    assert result.value == 42


def test_replace_if_equals(client, cache):

    result = cache_replace_if_equals(client, cache, 'my_test', 42, 1234)
    assert result.status == 0
    assert result.value is False

    cache_put(client, cache, 'my_test', 42)

    result = cache_replace_if_equals(client, cache, 'my_test', 42, 1234)
    assert result.status == 0
    assert result.value is True

    result = cache_get(client, cache, 'my_test')
    assert result.status == 0
    assert result.value == 1234


def test_clear(client, cache):

    result = cache_put(client, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_clear(client, cache)
    assert result.status == 0

    result = cache_get(client, cache, 'my_test')
    assert result.status == 0
    assert result.value is None


def test_clear_key(client, cache):

    result = cache_put(client, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_put(client, cache, 'another_test', 24)
    assert result.status == 0

    result = cache_clear_key(client, cache, 'my_test')
    assert result.status == 0

    result = cache_get(client, cache, 'my_test')
    assert result.status == 0
    assert result.value is None

    result = cache_get(client, cache, 'another_test')
    assert result.status == 0
    assert result.value == 24


def test_clear_keys(client, cache):

    result = cache_put(client, cache, 'my_test_key', 42)
    assert result.status == 0

    result = cache_put(client, cache, 'another_test', 24)
    assert result.status == 0

    result = cache_clear_keys(client, cache, [
        'my_test_key',
        'nonexistent_key',
    ])
    assert result.status == 0

    result = cache_get(client, cache, 'my_test_key')
    assert result.status == 0
    assert result.value is None

    result = cache_get(client, cache, 'another_test')
    assert result.status == 0
    assert result.value == 24


def test_remove_key(client, cache):

    result = cache_put(client, cache, 'my_test_key', 42)
    assert result.status == 0

    result = cache_remove_key(client, cache, 'my_test_key')
    assert result.status == 0
    assert result.value is True

    result = cache_remove_key(client, cache, 'non_existent_key')
    assert result.status == 0
    assert result.value is False


def test_remove_if_equals(client, cache):

    result = cache_put(client, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_remove_if_equals(client, cache, 'my_test', 1234)
    assert result.status == 0
    assert result.value is False

    result = cache_remove_if_equals(client, cache, 'my_test', 42)
    assert result.status == 0
    assert result.value is True

    result = cache_get(client, cache, 'my_test')
    assert result.status == 0
    assert result.value is None


def test_remove_keys(client, cache):

    result = cache_put(client, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_put(client, cache, 'another_test', 24)
    assert result.status == 0

    result = cache_remove_keys(client, cache, ['my_test', 'non_existent'])
    assert result.status == 0

    result = cache_get(client, cache, 'my_test')
    assert result.status == 0
    assert result.value is None

    result = cache_get(client, cache, 'another_test')
    assert result.status == 0
    assert result.value == 24


def test_remove_all(client, cache):

    result = cache_put(client, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_put(client, cache, 'another_test', 24)
    assert result.status == 0

    result = cache_remove_all(client, cache)
    assert result.status == 0

    result = cache_get(client, cache, 'my_test')
    assert result.status == 0
    assert result.value is None

    result = cache_get(client, cache, 'another_test')
    assert result.status == 0
    assert result.value is None


def test_cache_get_size(client, cache):

    result = cache_put(client, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_get_size(client, cache)
    assert result.status == 0
    assert result.value == 1


def test_put_get_collection(client):

    test_datetime = datetime(year=1996, month=3, day=1)

    cache = client.get_or_create_cache('test_coll_cache')
    cache.put(
        'simple',
        (
            1,
            [
                (123, IntObject),
                678,
                None,
                55.2,
                ((test_datetime, 0), TimestampObject),
            ]
        ),
        value_hint=CollectionObject
    )
    value = cache.get('simple')
    assert value == (1, [123, 678, None, 55.2, (test_datetime, 0)])

    cache.put(
        'nested',
        (
            1,
            [
                123,
                ((1, [456, 'inner_test_string', 789]), CollectionObject),
                'outer_test_string',
            ]
        ),
        value_hint=CollectionObject
    )
    value = cache.get('nested')
    assert value == (
        1,
        [
            123,
            (1, [456, 'inner_test_string', 789]),
            'outer_test_string'
        ]
    )


def test_put_get_map(client):

    cache = client.get_or_create_cache('test_map_cache')

    cache.put(
        'test_map',
        (
            MapObject.HASH_MAP,
            {
                (123, IntObject): 'test_data',
                456: ((1, [456, 'inner_test_string', 789]), CollectionObject),
                'test_key': 32.4,
            }
        ),
        value_hint=MapObject
    )
    value = cache.get('test_map')
    assert value == (MapObject.HASH_MAP, {
        123: 'test_data',
        456: (1, [456, 'inner_test_string', 789]),
        'test_key': 32.4,
    })
