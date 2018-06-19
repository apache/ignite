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

import ctypes

from pyignite.connection import Connection
from pyignite.constants import *


__all__ = [
    'Primitive',
    'Byte', 'Short', 'Int', 'Long', 'Float', 'Double', 'Char', 'Bool',
]


class Primitive:
    """
    Ignite primitive type. Base type for the following types:

    - Byte,
    - Short,
    - Int,
    - Long,
    - Float,
    - Double,
    - Char,
    - Bool.
    """

    c_type = None

    @classmethod
    def parse(cls, conn: Connection):
        return cls.c_type, conn.recv(ctypes.sizeof(cls.c_type))

    @staticmethod
    def to_python(ctype_object):
        return ctype_object

    @classmethod
    def from_python(cls, value):
        return bytes(cls.c_type(value))


class Byte(Primitive):
    c_type = ctypes.c_byte


class Short(Primitive):
    c_type = ctypes.c_short


class Int(Primitive):
    c_type = ctypes.c_int


class Long(Primitive):
    c_type = ctypes.c_long


class Float(Primitive):
    c_type = ctypes.c_float


class Double(Primitive):
    c_type = ctypes.c_double


class Char(Primitive):
    c_type = ctypes.c_short

    @classmethod
    def to_python(cls, ctype_object):
        return ctype_object.value.to_bytes(
            ctypes.sizeof(cls.c_type),
            byteorder=PROTOCOL_BYTE_ORDER
        ).decode(PROTOCOL_CHAR_ENCODING)

    @classmethod
    def from_python(cls, value):
        if type(value) is str:
            value = value.encode(PROTOCOL_CHAR_ENCODING)
        # assuming either a bytes or an integer
        if type(value) is bytes:
            value = int.from_bytes(value, byteorder=PROTOCOL_BYTE_ORDER)
        # assuming a valid integer
        return value.to_bytes(
            ctypes.sizeof(cls.c_type),
            byteorder=PROTOCOL_BYTE_ORDER
        )


class Bool(Primitive):
    c_type = ctypes.c_bool
