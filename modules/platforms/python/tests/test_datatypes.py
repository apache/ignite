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

from datetime import datetime, timedelta
import decimal
import pytest
import uuid

from pyignite.api.key_value import cache_get, cache_put
from pyignite.datatypes import *


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
        ([1, 2, 3, 5], ByteArrayObject),
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
def test_put_get_data(conn, hash_code, value, value_hint):

    result = cache_put(conn, hash_code, 'my_key', value, value_hint=value_hint)
    assert result.status == 0

    result = cache_get(conn, hash_code, 'my_key')
    assert result.status == 0
    assert result.value == value
