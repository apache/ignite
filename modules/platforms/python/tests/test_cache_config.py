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

from pyignite.connection import Connection
from pyignite.api import (
    cache_destroy, cache_get_or_create, cache_get_configuration, hashcode,
)


def test_put_get(ignite_host, ignite_port):
    conn = Connection()
    conn.connect(ignite_host, ignite_port)

    result = cache_get_or_create(conn, 'my_cache')
    assert result.status == 0

    result = cache_get_configuration(conn, hashcode('my_cache'))
    assert result.status == 0
    assert result.value['name'] == 'my_cache'

    # cleanup
    cache_destroy(conn, hashcode('my_cache'))
    conn.close()
