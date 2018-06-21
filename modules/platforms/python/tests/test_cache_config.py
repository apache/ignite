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
from pyignite.datatypes.prop_codes import *


def test_get_configuration(conn):

    result = cache_get_or_create(conn, 'my_unique_cache')
    assert result.status == 0

    result = cache_get_configuration(conn, hashcode('my_unique_cache'))
    assert result.status == 0
    assert result.value['name'] == 'my_unique_cache'


def test_create_with_config(conn):

    cache_name = 'my_very_unique_name'

    result = cache_create_with_config(conn, {
        PROP_NAME: cache_name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'name': '123_key',
                'type_name': 'blah',
                'is_key_field': False,
                'is_notnull_constraint_field': False,
            }
        ],
    })
    assert result.status == 0

    result = cache_get_names(conn)
    assert cache_name in result.value

    result = cache_create_with_config(conn, {
        PROP_NAME: cache_name,
    })
    assert result.status != 0


def test_get_or_create_with_config(conn):

    cache_name = 'my_very_unique_name'

    result = cache_get_or_create_with_config(conn, {
        PROP_NAME: cache_name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'name': '123_key',
                'type_name': 'blah',
                'is_key_field': False,
                'is_notnull_constraint_field': False,
            }
        ],
    })
    assert result.status == 0

    result = cache_get_names(conn)
    assert cache_name in result.value

    result = cache_get_or_create_with_config(conn, {
        PROP_NAME: cache_name,
    })
    assert result.status == 0
