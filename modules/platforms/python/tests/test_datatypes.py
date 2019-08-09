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
import ctypes
from datetime import datetime, timedelta
import decimal
import pytest
import uuid

from pygridgain.api.key_value import cache_get, cache_put
from pygridgain.datatypes import *
from pygridgain.utils import unsigned


@pytest.mark.parametrize(
    'value, value_hint',
    [
        # integers
        (42, None),
        (42, ByteObject),
        (42, ShortObject),
        (42, IntObject),

        # floats
        (3.1415, None),  # True for Double but not Float
        (3.5, FloatObject),

        # char is never autodetected
        ('ы', CharObject),
        ('カ', CharObject),

        # bool
        (True, None),
        (False, None),
        (True, BoolObject),
        (False, BoolObject),

        # arrays of integers
        ([1, 2, 3, 5], None),
        (b'buzz', ByteArrayObject),
        (bytearray([7, 8, 8, 11]), None),
        (bytearray([7, 8, 8, 11]), ByteArrayObject),
        ([1, 2, 3, 5], ShortArrayObject),
        ([1, 2, 3, 5], IntArrayObject),

        # arrays of floats
        ([2.2, 4.4, 6.6], None),
        ([2.5, 6.5], FloatArrayObject),

        # array of char
        (['ы', 'カ'], CharArrayObject),

        # array of bool
        ([True, False, True], None),

        # string
        ('Little Mary had a lamb', None),
        ('This is a test', String),

        # decimals
        (decimal.Decimal('2.5'), None),
        (decimal.Decimal('-1.3'), None),

        # uuid
        (uuid.uuid4(), None),

        # date
        (datetime(year=1998, month=4, day=6, hour=18, minute=30), None),

        # no autodetection for timestamp either
        (
            (datetime(year=1998, month=4, day=6, hour=18, minute=30), 1000),
            TimestampObject
        ),

        # time
        (timedelta(days=4, hours=4, minutes=24), None),

        # enum is useless in Python, except for interoperability with Java.
        # Also no autodetection
        ((5, 6), BinaryEnumObject),

        # arrays of standard types
        (['String 1', 'String 2'], None),
        (['Some of us are empty', None, 'But not the others'], None),

        ([decimal.Decimal('2.71828'), decimal.Decimal('100')], None),
        ([decimal.Decimal('2.1'), None, decimal.Decimal('3.1415')], None),

        ([uuid.uuid4(), uuid.uuid4()], None),
        (
            [
                datetime(year=2010, month=1, day=1),
                datetime(year=2010, month=12, day=31),
            ],
            None,
        ),
        ([timedelta(minutes=30), timedelta(hours=2)], None),
        (
            [
                (datetime(year=2010, month=1, day=1), 1000),
                (datetime(year=2010, month=12, day=31), 200),
            ],
            TimestampArrayObject
        ),
        ((-1, [(6001, 1), (6002, 2), (6003, 3)]), BinaryEnumArrayObject),

        # object array
        ((-1, [1, 2, decimal.Decimal('3')]), None),

        # collection
        ((3, [1, 2, 3]), CollectionObject),

        # map
        ((1, {'key': 4, 5: 6.0}), None),
        ((2, {'key': 4, 5: 6.0}), None),
    ]
)
def test_put_get_data(client, cache, value, value_hint):

    conn = client.random_node

    result = cache_put(conn, cache, 'my_key', value, value_hint=value_hint)
    assert result.status == 0

    result = cache_get(conn, cache, 'my_key')
    assert result.status == 0
    assert result.value == value


@pytest.mark.parametrize(
    'value',
    [
        [1, 2, 3, 5],
        (7, 8, 13, 18),
        (-128, -1, 0, 1, 127, 255),
    ]
)
def test_bytearray_from_list_or_tuple(client, cache, value):
    """
    ByteArrayObject's pythonic type is `bytearray`, but it should also accept
    lists or tuples as a content.
    """

    conn = client.random_node

    result = cache_put(
        conn, cache, 'my_key', value, value_hint=ByteArrayObject
    )
    assert result.status == 0

    result = cache_get(conn, cache, 'my_key')
    assert result.status == 0
    assert result.value == bytearray([
        unsigned(ch, ctypes.c_ubyte) for ch in value
    ])


@pytest.mark.parametrize(
    'uuid_string',
    [
        'd57babad-7bc1-4c82-9f9c-e72841b92a85',
        '5946c0c0-2b76-479d-8694-a2e64a3968da',
        'a521723d-ad5d-46a6-94ad-300f850ef704',
    ]
)
def test_uuid_representation(client, uuid_string):
    """ Test if textual UUID representation is correct. """
    uuid_value = uuid.UUID(uuid_string)

    # initial cleanup
    client.sql("DROP TABLE test_uuid_repr IF EXISTS")
    # create table with UUID field
    client.sql(
        "CREATE TABLE test_uuid_repr (id INTEGER PRIMARY KEY, uuid_field UUID)"
    )
    # use uuid.UUID class to insert data
    client.sql(
        "INSERT INTO test_uuid_repr(id, uuid_field) VALUES (?, ?)",
        query_args=[1, uuid_value]
    )
    # use hex string to retrieve data
    result = client.sql(
        "SELECT * FROM test_uuid_repr WHERE uuid_field='{}'".format(
            uuid_string
        )
    )

    # finalize query
    result = list(result)

    # final cleanup
    client.sql("DROP TABLE test_uuid_repr IF EXISTS")

    # if a line was retrieved, our test was successful
    assert len(result) == 1
    # doublecheck
    assert result[0][1] == uuid_value
