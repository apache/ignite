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

from pyignite.api import *
from pyignite.datatypes import IntObject


def test_put_get(conn, cache):

    result = cache_put(conn, cache, 'my_key', 5)
    assert result.status == 0

    result = cache_get(conn, cache, 'my_key')
    assert result.status == 0
    assert result.value == 5


def test_get_all(conn, cache):

    result = cache_get_all(conn, cache, ['key_1', 2, (3, IntObject)])
    assert result.status == 0
    assert result.value == {}

    cache_put(conn, cache, 'key_1', 4)
    cache_put(conn, cache, 3, 18, key_hint=IntObject)

    result = cache_get_all(conn, cache, ['key_1', 2, (3, IntObject)])
    assert result.status == 0
    assert result.value == {'key_1': 4, 3: 18}


def test_put_all(conn, cache):

    test_dict = {
        1: 2,
        'key_1': 4,
        (3, IntObject): 18,
    }
    test_keys = ['key_1', 1, 3]

    result = cache_put_all(conn, cache, test_dict)
    assert result.status == 0

    result = cache_get_all(conn, cache, test_keys)
    assert result.status == 0
    assert len(test_dict) == 3

    for key in result.value:
        assert key in test_keys


def test_contains_key(conn, cache):

    cache_put(conn, cache, 'test_key', 42)

    result = cache_contains_key(conn, cache, 'test_key')
    assert result.value is True

    result = cache_contains_key(conn, cache, 'non-existant-key')
    assert result.value is False


def test_contains_keys(conn, cache):

    cache_put(conn, cache, 5, 6)
    cache_put(conn, cache, 'test_key', 42)

    result = cache_contains_keys(conn, cache, [5, 'test_key'])
    assert result.value is True

    result = cache_contains_keys(conn, cache, [5, 'non-existent-key'])
    assert result.value is False


def test_get_and_put(conn, cache):

    result = cache_get_and_put(conn, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is None

    result = cache_get(conn, cache, 'test_key')
    assert result.status == 0
    assert result.value is 42

    result = cache_get_and_put(conn, cache, 'test_key', 1234)
    assert result.status == 0
    assert result.value == 42


def test_get_and_replace(conn, cache):

    result = cache_get_and_replace(conn, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is None

    result = cache_get(conn, cache, 'test_key')
    assert result.status == 0
    assert result.value is None

    cache_put(conn, cache, 'test_key', 42)

    result = cache_get_and_replace(conn, cache, 'test_key', 1234)
    assert result.status == 0
    assert result.value == 42


def test_get_and_remove(conn, cache):

    result = cache_get_and_remove(conn, cache, 'test_key')
    assert result.status == 0
    assert result.value is None

    cache_put(conn, cache, 'test_key', 42)

    result = cache_get_and_remove(conn, cache, 'test_key')
    assert result.status == 0
    assert result.value == 42


def test_put_if_absent(conn, cache):

    result = cache_put_if_absent(conn, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is True

    result = cache_put_if_absent(conn, cache, 'test_key', 1234)
    assert result.status == 0
    assert result.value is False


def test_get_and_put_if_absent(conn, cache):

    result = cache_get_and_put_if_absent(conn, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is None

    result = cache_get_and_put_if_absent(conn, cache, 'test_key', 1234)
    assert result.status == 0
    assert result.value == 42

    result = cache_get_and_put_if_absent(conn, cache, 'test_key', 5678)
    assert result.status == 0
    assert result.value == 42


def test_replace(conn, cache):

    result = cache_replace(conn, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is False

    cache_put(conn, cache, 'test_key', 1234)

    result = cache_replace(conn, cache, 'test_key', 42)
    assert result.status == 0
    assert result.value is True

    result = cache_get(conn, cache, 'test_key')
    assert result.status == 0
    assert result.value == 42


def test_replace_if_equals(conn, cache):

    result = cache_replace_if_equals(conn, cache, 'my_test', 42, 1234)
    assert result.status == 0
    assert result.value is False

    cache_put(conn, cache, 'my_test', 42)

    result = cache_replace_if_equals(conn, cache, 'my_test', 42, 1234)
    assert result.status == 0
    assert result.value is True

    result = cache_get(conn, cache, 'my_test')
    assert result.status == 0
    assert result.value == 1234


def test_clear(conn, cache):

    result = cache_put(conn, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_clear(conn, cache)
    assert result.status == 0

    result = cache_get(conn, cache, 'my_test')
    assert result.status == 0
    assert result.value is None


def test_clear_key(conn, cache):

    result = cache_put(conn, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_put(conn, cache, 'another_test', 24)
    assert result.status == 0

    result = cache_clear_key(conn, cache, 'my_test')
    assert result.status == 0

    result = cache_get(conn, cache, 'my_test')
    assert result.status == 0
    assert result.value is None

    result = cache_get(conn, cache, 'another_test')
    assert result.status == 0
    assert result.value == 24


def test_clear_keys(conn, cache):

    result = cache_put(conn, cache, 'my_test_key', 42)
    assert result.status == 0

    result = cache_put(conn, cache, 'another_test', 24)
    assert result.status == 0

    result = cache_clear_keys(conn, cache, [
        'my_test_key',
        'nonexistent_key',
    ])
    assert result.status == 0

    result = cache_get(conn, cache, 'my_test_key')
    assert result.status == 0
    assert result.value is None

    result = cache_get(conn, cache, 'another_test')
    assert result.status == 0
    assert result.value == 24


def test_remove_key(conn, cache):

    result = cache_put(conn, cache, 'my_test_key', 42)
    assert result.status == 0

    result = cache_remove_key(conn, cache, 'my_test_key')
    assert result.status == 0
    assert result.value is True

    result = cache_remove_key(conn, cache, 'non_existent_key')
    assert result.status == 0
    assert result.value is False


def test_remove_if_equals(conn, cache):

    result = cache_put(conn, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_remove_if_equals(conn, cache, 'my_test', 1234)
    assert result.status == 0
    assert result.value is False

    result = cache_remove_if_equals(conn, cache, 'my_test', 42)
    assert result.status == 0
    assert result.value is True

    result = cache_get(conn, cache, 'my_test')
    assert result.status == 0
    assert result.value is None


def test_remove_keys(conn, cache):

    result = cache_put(conn, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_put(conn, cache, 'another_test', 24)
    assert result.status == 0

    result = cache_remove_keys(conn, cache, ['my_test', 'non_existent'])
    assert result.status == 0

    result = cache_get(conn, cache, 'my_test')
    assert result.status == 0
    assert result.value is None

    result = cache_get(conn, cache, 'another_test')
    assert result.status == 0
    assert result.value == 24


def test_remove_all(conn, cache):

    result = cache_put(conn, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_put(conn, cache, 'another_test', 24)
    assert result.status == 0

    result = cache_remove_all(conn, cache)
    assert result.status == 0

    result = cache_get(conn, cache, 'my_test')
    assert result.status == 0
    assert result.value is None

    result = cache_get(conn, cache, 'another_test')
    assert result.status == 0
    assert result.value is None


def test_cache_get_size(conn, cache):

    result = cache_put(conn, cache, 'my_test', 42)
    assert result.status == 0

    result = cache_get_size(conn, cache)
    assert result.status == 0
    assert result.value == 1
