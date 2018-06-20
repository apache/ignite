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

from pyignite.connection import Connection
from pyignite.api import cache_create, cache_get_names, cache_destroy, hashcode


IGNITE_DEFAULT_HOST = 'localhost'
IGNITE_DEFAULT_PORT = 10800


@pytest.fixture(scope='module')
def conn(ignite_host, ignite_port):
    conn = Connection()
    conn.connect(ignite_host, ignite_port)
    yield conn
    for cache_name in cache_get_names(conn).value:
        cache_destroy(conn, hashcode(cache_name))
    conn.close()


@pytest.fixture
def hash_code(conn):
    cache_name = 'my_bucket'
    cache_create(conn, cache_name)
    yield hashcode(cache_name)
    cache_destroy(conn, hashcode(cache_name))


def pytest_addoption(parser):
    parser.addoption(
        '--ignite-host',
        action='append',
        default=[IGNITE_DEFAULT_HOST],
        help='Ignite binary protocol test server host (default: localhost)'
    )
    parser.addoption(
        '--ignite-port',
        action='append',
        default=[IGNITE_DEFAULT_PORT],
        type=int,
        help='Ignite binary protocol test server port (default: 10800)'
    )


def pytest_generate_tests(metafunc):
    if 'ignite_host' in metafunc.fixturenames:
        metafunc.parametrize(
            'ignite_host',
            metafunc.config.getoption('ignite_host') or [IGNITE_DEFAULT_HOST],
            scope='session'
        )
    if 'ignite_port' in metafunc.fixturenames:
        metafunc.parametrize(
            'ignite_port',
            metafunc.config.getoption('ignite_port') or [IGNITE_DEFAULT_PORT],
            scope='session'
        )
