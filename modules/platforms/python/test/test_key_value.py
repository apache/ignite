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

import socket

from queries import HandshakeRequest, read_response
from queries.common import hashcode
from queries.key_value import cache_create, cache_get, cache_put


def test_put_get():
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect(('localhost', 10900))
    hs_request = HandshakeRequest()
    conn.send(hs_request)
    hs_response = read_response(conn)
    assert hs_response.op_code != 0

    cache_create(conn, 'my_bucket')

    result = cache_put(conn, hashcode('my_bucket'), 'my_key', 5)
    assert 0 in result.keys()

    result = cache_get(conn, hashcode('my_bucket'), 'my_key')
    assert 0 in result.keys()
    assert result['result'] == 5

    conn.close()
