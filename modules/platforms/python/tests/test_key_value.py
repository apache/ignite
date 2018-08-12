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
