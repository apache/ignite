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

from datetime import date, datetime, time
import ctypes
import socket
from uuid import UUID, uuid4

from constants import *
from datatypes.class_configs import *
from datatypes.type_codes import *
from .simple import init


def get_attribute(self):
    if self._type_code == TC_BOOL:
        return bool(self.value)
    if self._type_code == TC_CHAR:
        return self.value.to_bytes(
            2,
            byteorder=PROTOCOL_BYTE_ORDER
        ).decode(
            PROTOCOL_CHAR_ENCODING
        )
    if self._type_code == TC_UUID:
        return UUID(bytes=bytes(self.value))
    return self.value


def set_attribute(self, value):
    if self._type_code == TC_BOOL:
        self.value = 1 if value else 0
    elif self._type_code == TC_CHAR:
        self.value = int.from_bytes(
            value.encode(PROTOCOL_CHAR_ENCODING),
            byteorder=PROTOCOL_BYTE_ORDER
        )
    elif self._type_code == TC_UUID:
        self.value = (ctypes.c_byte*16)(*bytearray(value.bytes))
    else:
        self.value = value


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
            'get_attribute': get_attribute,
            'set_attribute': set_attribute,
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
