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
from datatypes.primitive_objects import CharObject


@pytest.mark.parametrize(
    'value, value_hint',
    [
        ('ы', CharObject),
        ('カ', CharObject),
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
