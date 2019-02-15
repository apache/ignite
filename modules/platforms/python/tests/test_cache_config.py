#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.

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
