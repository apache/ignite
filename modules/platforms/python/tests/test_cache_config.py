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


def test_get_configuration(client):

    result = cache_get_or_create(client, 'my_unique_cache')
    assert result.status == 0

    result = cache_get_configuration(client, 'my_unique_cache')
    assert result.status == 0
    assert result.value[PROP_NAME] == 'my_unique_cache'


def test_create_with_config(client):

    cache_name = 'my_very_unique_name'

    result = cache_create_with_config(client, {
        PROP_NAME: cache_name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'blah',
                'affinity_key_field_name': 'abc1234',
            }
        ],
    })
    assert result.status == 0

    result = cache_get_names(client)
    assert cache_name in result.value

    result = cache_create_with_config(client, {
        PROP_NAME: cache_name,
    })
    assert result.status != 0


def test_get_or_create_with_config(client):

    cache_name = 'my_very_unique_name'

    result = cache_get_or_create_with_config(client, {
        PROP_NAME: cache_name,
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': 'blah',
                'affinity_key_field_name': 'abc1234',
            }
        ],
    })
    assert result.status == 0

    result = cache_get_names(client)
    assert cache_name in result.value

    result = cache_get_or_create_with_config(client, {
        PROP_NAME: cache_name,
    })
    assert result.status == 0
