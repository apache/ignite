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

from connection import Connection
from api import cache_create, cache_destroy, hashcode


@pytest.fixture(scope='module')
def conn(ignite_host, ignite_port):
    conn = Connection()
    conn.connect(ignite_host, ignite_port)
    yield conn
    conn.close()


@pytest.fixture
def hash_code(conn):
    cache_name = 'my_bucket'
    cache_create(conn, cache_name)
    yield hashcode(cache_name)
    cache_destroy(conn, hashcode(cache_name))
