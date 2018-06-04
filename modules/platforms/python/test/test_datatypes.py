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

from datatypes import simple_data_object, string_object
from datatypes.type_codes import *


class MockSocket:

    def __init__(self, buffer: bytes):
        self.buffer = buffer
        self.pos = 0

    def send(self, data: bytes):
        print(f'Received: {data}')

    def recv(self, buffersize: int):
        received = self.buffer[self.pos:self.pos+buffersize]
        self.pos += buffersize
        return received


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (MockSocket(b'\x01\xfe'), -2),
        (MockSocket(b'\x01\x02'), 2),
    ]
)
def test_byte(conn, expected_value):
    byte_var = simple_data_object(conn)
    assert byte_var.type_code == int.from_bytes(TC_BYTE, byteorder='little')
    assert byte_var.value == expected_value


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (MockSocket(b'\x05\x00\x00\x00\xc0'), -2),
        (MockSocket(b'\x05\x00\x00\x00\x40'), 2),
    ]
)
def test_float(conn, expected_value):
    float_var = simple_data_object(conn)
    assert float_var.type_code == int.from_bytes(TC_FLOAT, byteorder='little')
    assert float_var.value == expected_value


@pytest.mark.parametrize(
    'conn, expected_length, expected_data',
    [
        (MockSocket(b'\x09\x02\x00\x00\x00\x20\x20'), 2, '  '),
        (MockSocket(b'\x09\x03\x00\x00\x00\x61\x62\x63'), 3, 'abc'),
        (MockSocket(b'\x09\x04\x00\x00\x00\xf0\x9f\x98\xbc'), 4, '😼'),
    ]
)
def test_string(conn,  expected_length, expected_data):
    string_var = string_object(conn)
    assert string_var.type_code == int.from_bytes(TC_STRING, byteorder='little')
    assert string_var.length == expected_length
    assert string_var.data.decode('utf-8') == expected_data
