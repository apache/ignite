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
from pygridgain.api import *
from pygridgain.datatypes.prop_codes import *


def test_get_configuration(client):

    conn = client.random_node

    result = cache_get_or_create(conn, 'my_unique_cache')
    assert result.status == 0

    result = cache_get_configuration(conn, 'my_unique_cache')
    assert result.status == 0
    assert result.value[PROP_NAME] == 'my_unique_cache'


def test_create_with_config(client):

    cache_name = 'my_very_unique_name'
    conn = client.random_node

    result = cache_create_with_config(conn, {
        PROP_NAME: cache_name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'blah',
                'affinity_key_field_name': 'abc1234',
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


def test_get_or_create_with_config(client):

    cache_name = 'my_very_unique_name'
    conn = client.random_node

    result = cache_get_or_create_with_config(conn, {
        PROP_NAME: cache_name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'blah',
                'affinity_key_field_name': 'abc1234',
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
