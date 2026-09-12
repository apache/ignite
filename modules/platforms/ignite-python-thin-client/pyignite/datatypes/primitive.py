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
import struct
from io import SEEK_CUR

from pyignite.constants import *
from .base import IgniteDataType
from .type_ids import *
from .type_names import *


__all__ = [
    'Primitive',
    'Byte', 'Short', 'Int', 'Long', 'Float', 'Double', 'Char', 'Bool',
]


class Primitive(IgniteDataType):
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
    _type_name = None
    _type_id = None
    c_type = None

    @classmethod
    def parse(cls, stream):
        stream.seek(ctypes.sizeof(cls.c_type), SEEK_CUR)
        return cls.c_type

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        return ctypes_object


class Byte(Primitive):
    _type_name = NAME_BYTE
    _type_id = TYPE_BYTE
    c_type = ctypes.c_byte

    @classmethod
    def from_python(cls, stream, value):
        stream.write(struct.pack("<b", value))


class Short(Primitive):
    _type_name = NAME_SHORT
    _type_id = TYPE_SHORT
    c_type = ctypes.c_short

    @classmethod
    def from_python(cls, stream, value):
        stream.write(struct.pack("<h", value))


class Int(Primitive):
    _type_name = NAME_INT
    _type_id = TYPE_INT
    c_type = ctypes.c_int

    @classmethod
    def from_python(cls, stream, value):
        stream.write(struct.pack("<i", value))


class Long(Primitive):
    _type_name = NAME_LONG
    _type_id = TYPE_LONG
    c_type = ctypes.c_longlong

    @classmethod
    def from_python(cls, stream, value):
        stream.write(struct.pack("<q", value))


class Float(Primitive):
    _type_name = NAME_FLOAT
    _type_id = TYPE_FLOAT
    c_type = ctypes.c_float

    @classmethod
    def from_python(cls, stream, value):
        stream.write(struct.pack("<f", value))


class Double(Primitive):
    _type_name = NAME_DOUBLE
    _type_id = TYPE_DOUBLE
    c_type = ctypes.c_double

    @classmethod
    def from_python(cls, stream, value):
        stream.write(struct.pack("<d", value))


class Char(Primitive):
    _type_name = NAME_CHAR
    _type_id = TYPE_CHAR
    c_type = ctypes.c_short

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        return ctypes_object.value.to_bytes(
            ctypes.sizeof(cls.c_type),
            byteorder=PROTOCOL_BYTE_ORDER
        ).decode(PROTOCOL_CHAR_ENCODING)

    @classmethod
    def from_python(cls, stream, value):
        if type(value) is str:
            value = value.encode(PROTOCOL_CHAR_ENCODING)
        # assuming either a bytes or an integer
        if type(value) is bytes:
            value = int.from_bytes(value, byteorder=PROTOCOL_BYTE_ORDER)
        # assuming a valid integer
        stream.write(
            value.to_bytes(ctypes.sizeof(cls.c_type), byteorder=PROTOCOL_BYTE_ORDER)
        )


class Bool(Primitive):
    _type_name = NAME_BOOLEAN
    _type_id = TYPE_BOOLEAN
    c_type = ctypes.c_byte  # Use c_byte because c_bool throws endianness conversion error on BE systems.

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        return ctypes_object != 0

    @classmethod
    def from_python(cls, stream, value, **kwargs):
        stream.write(struct.pack("<b", 1 if value else 0))
