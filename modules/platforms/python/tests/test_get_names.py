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
from pygridgain.api import cache_create, cache_get_names


def test_get_names(client):

    conn = client.random_node

    bucket_names = ['my_bucket', 'my_bucket_2', 'my_bucket_3']
    for name in bucket_names:
        cache_create(conn, name)

    result = cache_get_names(conn)
    assert result.status == 0
    assert type(result.value) == list
    assert len(result.value) >= len(bucket_names)
    for i, name in enumerate(bucket_names):
        assert name in result.value
