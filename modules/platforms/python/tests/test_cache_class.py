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

from pyignite.datatypes import (
    BinaryObject, BoolObject, DecimalObject, FloatObject, IntObject, String,
)


def test_cache_create(conn):
    cache = conn.create_cache('my_oop_cache')
    assert cache.name == 'my_oop_cache'
    cache.destroy()


def test_cache_get_put(conn):
    cache = conn.create_cache('my_oop_cache')
    cache.put('my_key', 42)
    result = cache.get('my_key')
    assert result, 42
    cache.destroy()


def test_cache_binary_get_put(conn):
    binary_type = conn.put_binary_type(
        'TestBinaryType',
        schema=OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
            ('TEST_DECIMAL', DecimalObject),
        ])
    )
    cache = conn.create_cache('my_oop_cache')
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


def test_get_binary_type(conn):
    conn.put_binary_type(
        'TestBinaryType',
        schema=OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
        ])
    )
    conn.put_binary_type(
        'TestBinaryType',
        schema=OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
            ('TEST_FLOAT', FloatObject),
        ])
    )
    conn.put_binary_type(
        'TestBinaryType',
        schema=OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
            ('TEST_DECIMAL', DecimalObject),
        ])
    )

    binary_type_info = conn.get_binary_type('TestBinaryType')
    assert len(binary_type_info['binary_fields']) == 5
    assert len(binary_type_info['schema']) == 3
