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

from collections import OrderedDict
from decimal import Decimal

import pytest

from pyignite import GenericObjectMeta
from pyignite.datatypes import (
    BoolObject, DecimalObject, FloatObject, IntObject, String,
)
from pyignite.datatypes.prop_codes import *
from pyignite.exceptions import CacheError


def test_cache_create(client):
    cache = client.get_or_create_cache('my_oop_cache')
    assert cache.name == cache.settings[PROP_NAME] == 'my_oop_cache'
    cache.destroy()


def test_cache_remove(client):
    cache = client.get_or_create_cache('my_cache')
    cache.clear()
    assert cache.get_size() == 0

    cache.put_all({
        'key_1': 1,
        'key_2': 2,
        'key_3': 3,
        'key_4': 4,
        'key_5': 5,
    })
    assert cache.get_size() == 5

    result = cache.remove_if_equals('key_1', 42)
    assert result is False
    assert cache.get_size() == 5

    result = cache.remove_if_equals('key_1', 1)
    assert result is True
    assert cache.get_size() == 4

    cache.remove_keys(['key_1', 'key_3', 'key_5', 'key_7'])
    assert cache.get_size() == 2

    cache.remove_all()
    assert cache.get_size() == 0


def test_cache_get(client):
    client.get_or_create_cache('my_cache')

    my_cache = client.get_cache('my_cache')
    assert my_cache.settings[PROP_NAME] == 'my_cache'
    my_cache.destroy()

    error = None

    my_cache = client.get_cache('my_cache')
    try:
        _ = my_cache.settings[PROP_NAME]
    except CacheError as e:
        error = e

    assert type(error) is CacheError


def test_cache_config(client):
    cache_config = {
        PROP_NAME: 'my_oop_cache',
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'blah',
                'affinity_key_field_name': 'abc1234',
            },
        ],
    }
    client.create_cache(cache_config)

    cache = client.get_or_create_cache('my_oop_cache')
    assert cache.name == cache_config[PROP_NAME]
    assert (
        cache.settings[PROP_CACHE_KEY_CONFIGURATION]
        == cache_config[PROP_CACHE_KEY_CONFIGURATION]
    )

    cache.destroy()


def test_cache_get_put(client):
    cache = client.get_or_create_cache('my_oop_cache')
    cache.put('my_key', 42)
    result = cache.get('my_key')
    assert result, 42
    cache.destroy()


def test_cache_binary_get_put(client):

    class TestBinaryType(
        metaclass=GenericObjectMeta,
        schema=OrderedDict([
            ('test_bool', BoolObject),
            ('test_str', String),
            ('test_int', IntObject),
            ('test_decimal', DecimalObject),
        ]),
    ):
        pass

    cache = client.create_cache('my_oop_cache')

    my_value = TestBinaryType(
        test_bool=True,
        test_str='This is a test',
        test_int=42,
        test_decimal=Decimal('34.56'),
    )
    cache.put('my_key', my_value)

    value = cache.get('my_key')
    assert value.test_bool is True
    assert value.test_str == 'This is a test'
    assert value.test_int == 42
    assert value.test_decimal == Decimal('34.56')

    cache.destroy()


def test_get_binary_type(client):
    client.put_binary_type(
        'TestBinaryType',
        schema=OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
        ])
    )
    client.put_binary_type(
        'TestBinaryType',
        schema=OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
            ('TEST_FLOAT', FloatObject),
        ])
    )
    client.put_binary_type(
        'TestBinaryType',
        schema=OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
            ('TEST_DECIMAL', DecimalObject),
        ])
    )

    binary_type_info = client.get_binary_type('TestBinaryType')
    assert len(binary_type_info['schemas']) == 3

    binary_type_info = client.get_binary_type('NonExistentType')
    assert binary_type_info['type_exists'] is False
    assert len(binary_type_info) == 1


@pytest.mark.parametrize('page_size', range(1, 17, 5))
def test_cache_scan(client, page_size):
    test_data = {
        1: 'This is a test',
        2: 'One more test',
        3: 'Foo',
        4: 'Buzz',
        5: 'Bar',
        6: 'Lorem ipsum',
        7: 'dolor sit amet',
        8: 'consectetur adipiscing elit',
        9: 'Nullam aliquet',
        10: 'nisl at ante',
        11: 'suscipit',
        12: 'ut cursus',
        13: 'metus interdum',
        14: 'Nulla tincidunt',
        15: 'sollicitudin iaculis',
    }

    cache = client.get_or_create_cache('my_oop_cache')
    cache.put_all(test_data)

    gen = cache.scan(page_size=page_size)
    received_data = []
    for k, v in gen:
        assert k in test_data.keys()
        assert v in test_data.values()
        received_data.append((k, v))
    assert len(received_data) == len(test_data)

    cache.destroy()


def test_get_and_put_if_absent(client):
    cache = client.get_or_create_cache('my_oop_cache')

    value = cache.get_and_put_if_absent('my_key', 42)
    assert value is None
    cache.put('my_key', 43)
    value = cache.get_and_put_if_absent('my_key', 42)
    assert value is 43
