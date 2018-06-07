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

from datetime import datetime, timedelta
import uuid

import pytest

from constants import *
from datatypes import data_object
from datatypes.type_codes import *


class MockSocket:

    def __init__(self, buffer: bytes):
        self.buffer = buffer
        self.pos = 0

    def send(self, data: bytes):
        print('Received: {}'.format(data))

    def recv(self, buffersize: int):
        received = self.buffer[self.pos:self.pos+buffersize]
        self.pos += buffersize
        return received


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (MockSocket(b'\x65'), None),
    ]
)
def test_null(conn, expected_value):
    null_var = data_object(conn)
    assert null_var.type_code == int.from_bytes(
        TC_NULL,
        byteorder=PROTOCOL_BYTE_ORDER
    )
    assert null_var.get_attribute() is expected_value


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (MockSocket(b'\x01\xfe'), -2),
        (MockSocket(b'\x01\x02'), 2),
    ]
)
def test_byte(conn, expected_value):
    byte_var = data_object(conn)
    assert byte_var.type_code == int.from_bytes(
        TC_BYTE,
        byteorder=PROTOCOL_BYTE_ORDER
    )
    assert byte_var.value == expected_value


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (MockSocket(b'\x05\x00\x00\x00\xc0'), -2),
        (MockSocket(b'\x05\x00\x00\x00\x40'), 2),
    ]
)
def test_float(conn, expected_value):
    float_var = data_object(conn)
    assert float_var.type_code == int.from_bytes(
        TC_FLOAT,
        byteorder=PROTOCOL_BYTE_ORDER
    )
    assert float_var.value == expected_value


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (MockSocket(b'\x08\x01'), True),
        (MockSocket(b'\x08\x00'), False),
    ]
)
def test_bool(conn, expected_value):
    bool_var = data_object(conn)
    assert bool_var.type_code == int.from_bytes(
        TC_BOOL,
        byteorder=PROTOCOL_BYTE_ORDER
    )
    assert bool_var.get_attribute() == expected_value


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (
            MockSocket(b'\x0a\x1ePf\xda\x88\xa5MZ\x86\xdc#\xdf\xd0\xa9\x13\x03'),
            uuid.UUID('1e5066da-88a5-4d5a-86dc-23dfd0a91303'),
        ),
    ]
)
def test_uuid(conn, expected_value):
    uuid_var = data_object(conn)
    assert uuid_var.type_code == int.from_bytes(
        TC_UUID,
        byteorder=PROTOCOL_BYTE_ORDER
    )
    assert uuid_var.get_attribute().bytes == expected_value.bytes

    uuid_var.set_attribute(expected_value)
    assert uuid_var.get_attribute().bytes == expected_value.bytes


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (
            MockSocket(b'\x0b\x40\xa8\x47\xe1\xcf\x00\x00\x00'),
            datetime(year=1998, month=4, day=18, hour=5, minute=30),
        ),
    ]
)
def test_date(conn, expected_value):
    date_var = data_object(conn)
    assert date_var.type_code == int.from_bytes(
        TC_DATE,
        byteorder=PROTOCOL_BYTE_ORDER
    )
    assert date_var.get_attribute() == expected_value

    date_var.set_attribute(expected_value)
    assert date_var.get_attribute() == expected_value


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (
            MockSocket(b'\x24\x80\xf0\xfa\x02\x00\x00\x00\x00'),
            timedelta(milliseconds=50000000),
        ),
    ]
)
def test_time(conn, expected_value):
    time_var = data_object(conn)
    assert time_var.type_code == int.from_bytes(
        TC_TIME,
        byteorder=PROTOCOL_BYTE_ORDER
    )
    assert time_var.get_attribute() == expected_value

    time_var.set_attribute(expected_value)
    assert time_var.get_attribute() == expected_value


@pytest.mark.parametrize(
    'conn, expected_value',
    [
        (MockSocket(b'\x07\x4b\x04'), 'ы'),
        (MockSocket(b'\x07\xab\x30'), 'カ'),
    ]
)
def test_char(conn, expected_value):
    char_var = data_object(conn)
    assert char_var.type_code == int.from_bytes(
        TC_CHAR,
        byteorder=PROTOCOL_BYTE_ORDER
    )
    assert char_var.get_attribute() == expected_value


@pytest.mark.parametrize(
    'conn, expected_length, expected_data',
    [
        (MockSocket(b'\x09\x02\x00\x00\x00\x20\x20'), 2, '  '),
        (MockSocket(b'\x09\x03\x00\x00\x00\x61\x62\x63'), 3, 'abc'),
        (MockSocket(b'\x09\x04\x00\x00\x00\xf0\x9f\x98\xbc'), 4, '😼'),
    ]
)
def test_string(conn,  expected_length, expected_data):
    string_var = data_object(conn)
    assert string_var.type_code == int.from_bytes(
        TC_STRING,
        byteorder=PROTOCOL_BYTE_ORDER
    )
    assert string_var.length == expected_length
    assert string_var.data.decode(PROTOCOL_STRING_ENCODING) == expected_data
