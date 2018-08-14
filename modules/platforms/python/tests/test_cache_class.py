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

from pyignite.datatypes import (
    BinaryObject, BoolObject, DecimalObject, FloatObject, IntObject, String,
)
from pyignite.datatypes.prop_codes import *
from pyignite.exceptions import CacheError


def test_cache_create(client):
    cache = client.create_cache('my_oop_cache')
    assert cache.name == cache.settings[PROP_NAME] == 'my_oop_cache'
    cache.destroy()


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
    cache = client.create_cache('my_oop_cache')
    cache.put('my_key', 42)
    result = cache.get('my_key')
    assert result, 42
    cache.destroy()


def test_cache_binary_get_put(client):
    binary_type = client.put_binary_type(
        'TestBinaryType',
        schema=OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
            ('TEST_DECIMAL', DecimalObject),
        ])
    )
    cache = client.create_cache('my_oop_cache')
    cache.put(
        'my_key',
        {
            'version': 1,
            'type_id': binary_type['type_id'],
            'schema_id': binary_type['schema_id'],
            'fields': OrderedDict([
                ('TEST_BOOL', True),
                ('TEST_STR', 'This is a test'),
                ('TEST_INT', (42, IntObject)),
                ('TEST_DECIMAL', Decimal('34.56')),
            ]),
        },
        value_hint=BinaryObject
    )
    value = cache.get('my_key')
    assert value['fields']['TEST_BOOL'] is True
    assert value['fields']['TEST_STR'] == 'This is a test'
    assert value['fields']['TEST_INT'] == 42
    assert value['fields']['TEST_DECIMAL'] == Decimal('34.56')

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
    assert len(binary_type_info['binary_fields']) == 5
    assert len(binary_type_info['schema']) == 3


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

    cache = client.create_cache('my_oop_cache')
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
    cache = client.create_cache('my_oop_cache')

    value = cache.get_and_put_if_absent('my_key', 42)
    assert value is None
    cache.put('my_key', 43)
    value = cache.get_and_put_if_absent('my_key', 42)
    assert value is 43
