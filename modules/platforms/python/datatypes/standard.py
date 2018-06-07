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

"""
Non-serial data types which values require conversion between ctypes type
and python type.
"""

from datetime import datetime, timedelta
import ctypes
import socket
from uuid import UUID

from constants import *
from datatypes.class_configs import standard_from_python, standard_type_config
from datatypes.type_codes import *
from .simple import init


standard_get_attribute = {
    TC_BOOL: lambda self: bool(self.value),
    TC_CHAR: lambda self: self.value.to_bytes(
        2,
        byteorder=PROTOCOL_BYTE_ORDER
    ).decode(PROTOCOL_CHAR_ENCODING),
    TC_UUID: lambda self: UUID(bytes=bytes(self.value)),
    TC_DATE: lambda self: datetime.fromtimestamp(self.value/1000),
    TC_TIME: lambda self: timedelta(milliseconds=self.value),
    TC_TIMESTAMP: lambda self: (
            datetime.fromtimestamp(self.value.epoch/1000),
            self.value.fraction,
        ),
    TC_ENUM: lambda self: (self.value.type_id, self.value.ordinal),
}


def bool_set_attribute(self, value):
    self.value = 1 if value else 0


def char_set_attribute(self, value):
    self.value = int.from_bytes(
        value.encode(PROTOCOL_CHAR_ENCODING),
        byteorder=PROTOCOL_BYTE_ORDER
    )


def uuid_set_attribute(self, value):
    self.value = (ctypes.c_byte*16)(*bytearray(value.bytes))


def date_set_attribute(self, value):
    self.value = int(value.timestamp() * 1000)


def time_set_attribute(self, value):
    self.value = int(value.total_seconds()*1000)


def timestamp_set_attribute(self, value):
    self.value.epoch = int(value[0].timestamp()*1000)
    self.value.fraction = value[1]


def enum_set_attribute(self, value):
    self.value.type_id, self.value.ordinal = value


standard_set_attribute = {
    TC_BOOL: bool_set_attribute,
    TC_CHAR: char_set_attribute,
    TC_UUID: uuid_set_attribute,
    TC_DATE: date_set_attribute,
    TC_TIME: time_set_attribute,
    TC_TIMESTAMP: timestamp_set_attribute,
    TC_ENUM: enum_set_attribute,
}


def from_python(python_type):
    try:
        return standard_from_python[python_type]
    except KeyError:
        pass


def standard_data_class(python_var, tc_hint=None, **kwargs):
    python_type = type(python_var)
    type_code = tc_hint or from_python(python_type)
    assert type_code is not None, (
        'Can not map python type {} to standard data class.'.format(
            python_type
        )
    )
    class_name, ctypes_type = standard_type_config[type_code]
    return type(
        class_name,
        (ctypes.LittleEndianStructure,),
        {
            '_pack_': 1,
            '_fields_': [
                ('type_code', ctypes.c_byte),
                ('value', ctypes_type),
            ],
            '_type_code': type_code,
            'init': init,
            'get_attribute': standard_get_attribute[type_code],
            'set_attribute': standard_set_attribute[type_code],
        },
    )


def standard_data_object(connection: socket.socket, initial=None):
    buffer = initial or connection.recv(1)
    type_code = buffer
    data_class = standard_data_class(None, tc_hint=type_code)
    buffer += connection.recv(
        ctypes.sizeof(standard_type_config[type_code][1])
    )
    data_object = data_class.from_buffer_copy(buffer)
    return data_object
