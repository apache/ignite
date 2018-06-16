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

import pytest

from api import hashcode
from api.key_value import cache_get, cache_put
from api.cache_config import cache_create, cache_destroy
from connection import Connection
from datatypes.primitive_objects import *
from datatypes.primitive_arrays import *
from datatypes.strings import *


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

        # char
        ('ы', CharObject),  # Char is never autodetected
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
        ('This is a test', PString),

        # array of string
        (['String 1', 'String 2'], None),
        (['Some of us are empty', None, 'But not the others'], None),
        
    ]
)
def test_put_get_data(ignite_host, ignite_port, value, value_hint):
    conn = Connection()
    conn.connect(ignite_host, ignite_port)

    cache_create(conn, 'my_bucket')

    result = cache_put(
        conn, hashcode('my_bucket'), 'my_key',
        value, value_hint=value_hint
    )
    assert result.status == 0

    result = cache_get(conn, hashcode('my_bucket'), 'my_key')
    assert result.status == 0
    assert result.value == value

    # cleanup
    cache_destroy(conn, hashcode('my_bucket'))
    conn.close()
