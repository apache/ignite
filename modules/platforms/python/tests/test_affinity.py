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
from datetime import datetime, timedelta
import decimal
from uuid import UUID, uuid4

import pytest

from pygridgain import GenericObjectMeta
from pygridgain.api import *
from pygridgain.constants import *
from pygridgain.datatypes import *
from pygridgain.datatypes.cache_config import CacheMode
from pygridgain.datatypes.prop_codes import *


def test_get_node_partitions(client):

    if client.protocol_version < (1, 4, 0):
        pytest.skip(
            'Best effort affinity is not supported by the protocol {}.'.format(
                client.protocol_version
            )
        )

    conn = client.random_node

    cache_1 = client.get_or_create_cache('test_cache_1')
    cache_2 = client.get_or_create_cache({
        PROP_NAME: 'test_cache_2',
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': ByteArray.type_name,
                'affinity_key_field_name': 'byte_affinity',
            }
        ],
    })
    cache_3 = client.get_or_create_cache('test_cache_3')
    cache_4 = client.get_or_create_cache('test_cache_4')
    cache_5 = client.get_or_create_cache('test_cache_5')

    result = cache_get_node_partitions(
        conn,
        [cache_1.cache_id, cache_2.cache_id]
    )
    assert result.status == 0, result.message


@pytest.mark.parametrize(
    'key, key_hint', [
        # integers
        (42, None),
        (43, ByteObject),
        (-44, ByteObject),
        (45, IntObject),
        (-46, IntObject),
        (47, ShortObject),
        (-48, ShortObject),
        (49, LongObject),
        (MAX_INT-50, LongObject),
        (MAX_INT+51, LongObject),

        # floating point
        (5.2, None),
        (5.354, FloatObject),
        (-5.556, FloatObject),
        (-57.58, DoubleObject),

        # boolean
        (True, None),
        (True, BoolObject),
        (False, BoolObject),

        # char
        ('A', CharObject),
        ('Z', CharObject),
        ('⅓', CharObject),
        ('á', CharObject),
        ('ы', CharObject),
        ('カ', CharObject),
        ('Ø', CharObject),
        ('ß', CharObject),

        # string
        ('This is a test string', None),
        ('Кириллица', None),
        ('Little Mary had a lamb', String),

        # UUID
        (UUID('12345678123456789876543298765432'), None),
        (UUID('74274274274274274274274274274274'), UUIDObject),
        (uuid4(), None),

        # decimal (long internal representation in Java)
        (decimal.Decimal('-234.567'), None),
        (decimal.Decimal('200.0'), None),
        (decimal.Decimal('123.456'), DecimalObject),
        (decimal.Decimal('1.0'), None),
        (decimal.Decimal('0.02'), None),

        # decimal (BigInteger internal representation in Java)
        (decimal.Decimal('12345671234567123.45671234567'), None),
        (decimal.Decimal('-845678456.7845678456784567845'), None),

        # date and time
        (datetime(1980, 1, 1), None),
        ((datetime(1980, 1, 1), 999), TimestampObject),
        (timedelta(days=99), TimeObject),

    ],
)
def test_affinity(client, key, key_hint):

    if client.protocol_version < (1, 4, 0):
        pytest.skip(
            'Best effort affinity is not supported by the protocol {}.'.format(
                client.protocol_version
            )
        )

    cache_1 = client.get_or_create_cache({
        PROP_NAME: 'test_cache_1',
        PROP_CACHE_MODE: CacheMode.PARTITIONED,
    })
    value = 42
    cache_1.put(key, value, key_hint=key_hint)

    best_node = cache_1.get_best_node(key, key_hint=key_hint)

    for node in filter(lambda n: n.alive, client._nodes):
        result = cache_local_peek(
            node, cache_1.cache_id, key, key_hint=key_hint,
        )
        if node is best_node:
            assert result.value == value, (
                'Affinity calculation error for {}'.format(key)
            )
        else:
            assert result.value is None, (
                'Affinity calculation error for {}'.format(key)
            )

    cache_1.destroy()


def test_affinity_for_generic_object(client):

    if client.protocol_version < (1, 4, 0):
        pytest.skip(
            'Best effort affinity is not supported by the protocol {}.'.format(
                client.protocol_version
            )
        )

    cache_1 = client.get_or_create_cache({
        PROP_NAME: 'test_cache_1',
        PROP_CACHE_MODE: CacheMode.PARTITIONED,
    })

    class KeyClass(
        metaclass=GenericObjectMeta,
        schema={
            'NO': IntObject,
            'NAME': String,
        },
    ):
        pass

    key = KeyClass()
    key.NO = 1
    key.NAME = 'test_string'

    cache_1.put(key, 42, key_hint=BinaryObject)

    best_node = cache_1.get_best_node(key, key_hint=BinaryObject)

    for node in filter(lambda n: n.alive, client._nodes):
        result = cache_local_peek(
            node, cache_1.cache_id, key, key_hint=BinaryObject,
        )
        if node is best_node:
            assert result.value == 42, (
                'Affinity calculation error for {}'.format(key)
            )
        else:
            assert result.value is None, (
                'Affinity calculation error for {}'.format(key)
            )

    cache_1.destroy()


def test_affinity_for_generic_object_without_type_hints(client):

    if client.protocol_version < (1, 4, 0):
        pytest.skip(
            'Best effort affinity is not supported by the protocol {}.'.format(
                client.protocol_version
            )
        )

    cache_1 = client.get_or_create_cache({
        PROP_NAME: 'test_cache_1',
        PROP_CACHE_MODE: CacheMode.PARTITIONED,
    })

    class KeyClass(
        metaclass=GenericObjectMeta,
        schema={
            'NO': IntObject,
            'NAME': String,
        },
    ):
        pass

    key = KeyClass()
    key.NO = 2
    key.NAME = 'another_test_string'

    cache_1.put(key, 42)

    best_node = cache_1.get_best_node(key)

    for node in filter(lambda n: n.alive, client._nodes):
        result = cache_local_peek(
            node, cache_1.cache_id, key
        )
        if node is best_node:
            assert result.value == 42, (
                'Affinity calculation error for {}'.format(key)
            )
        else:
            assert result.value is None, (
                'Affinity calculation error for {}'.format(key)
            )

    cache_1.destroy()
