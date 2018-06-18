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

from api import *
from datatypes.primitive_objects import IntObject


def test_put_get(conn, hash_code):

    result = cache_put(conn, hash_code, 'my_key', 5)
    assert result.status == 0

    result = cache_get(conn, hash_code, 'my_key')
    assert result.status == 0
    assert result.value == 5


def test_get_all(conn, hash_code):

    result = cache_get_all(conn, hash_code, ['key_1', 2, (3, IntObject)])
    assert result.status == 0
    assert result.value == {}

    cache_put(conn, hash_code, 'key_1', 4)
    cache_put(conn, hash_code, 3, 18, key_hint=IntObject)

    result = cache_get_all(conn, hash_code, ['key_1', 2, (3, IntObject)])
    assert result.status == 0
    assert result.value == {'key_1': 4, 3: 18}


def test_put_all(conn, hash_code):

    test_dict = {
        1: 2,
        'key_1': 4,
        (3, IntObject): 18,
    }
    test_keys = ['key_1', 1, 3]

    result = cache_put_all(conn, hash_code, test_dict)
    assert result.status == 0

    result = cache_get_all(conn, hash_code, test_keys)
    assert result.status == 0
    assert len(test_dict) == 3

    for key in result.value:
        assert key in test_keys


def test_contains_key(conn, hash_code):

    cache_put(conn, hash_code, 'test_key', 42)

    result = cache_contains_key(conn, hash_code, 'test_key')
    assert result.value == True

    result = cache_contains_key(conn, hash_code, 'non-existant-key')
    assert result.value == False


def test_contains_keys(conn, hash_code):

    cache_put(conn, hash_code, 5, 6)
    cache_put(conn, hash_code, 'test_key', 42)

    result = cache_contains_keys(conn, hash_code, [5, 'test_key'])
    assert result.value == True

    result = cache_contains_keys(conn, hash_code, [5, 'non-existant-key'])
    assert result.value == False
