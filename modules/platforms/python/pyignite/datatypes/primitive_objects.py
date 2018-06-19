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
from .type_codes import *


__all__ = [
    'DataObject', 'ByteObject', 'ShortObject', 'IntObject', 'LongObject',
    'FloatObject', 'DoubleObject', 'CharObject', 'BoolObject',
]


class DataObject:
    """
    Base class for primitive data objects.

    Primitive data objects are built of primitive data prepended by
    the corresponding type code.
    """

    c_type = None
    type_code = None

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('value', cls.c_type),
                ],
            },
        )

    @classmethod
    def parse(cls, conn: Connection):
        data_type = cls.build_c_type()
        buffer = conn.recv(ctypes.sizeof(data_type))
        return data_type, buffer

    @staticmethod
    def to_python(ctype_object):
        return ctype_object.value

    @classmethod
    def from_python(cls, value):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.value = value
        return bytes(data_object)


class ByteObject(DataObject):
    c_type = ctypes.c_byte
    type_code = TC_BYTE


class ShortObject(DataObject):
    c_type = ctypes.c_short
    type_code = TC_SHORT


class IntObject(DataObject):
    c_type = ctypes.c_int
    type_code = TC_INT


class LongObject(DataObject):
    c_type = ctypes.c_long
    type_code = TC_LONG


class FloatObject(DataObject):
    c_type = ctypes.c_float
    type_code = TC_FLOAT


class DoubleObject(DataObject):
    c_type = ctypes.c_double
    type_code = TC_DOUBLE


class CharObject(DataObject):
    """
    This type is a little tricky. It stores character values in
    UTF-16 Little-endian encoding. We have to encode/decode it
    to/from UTF-8 to keep the coding hassle to minimum. Bear in mind
    though: decoded character may take 1..4 bytes in UTF-8.
    """
    c_type = ctypes.c_short
    type_code = TC_CHAR

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
        return cls.type_code + value.to_bytes(
            ctypes.sizeof(cls.c_type),
            byteorder=PROTOCOL_BYTE_ORDER
        )


class BoolObject(DataObject):
    c_type = ctypes.c_bool
    type_code = TC_BOOL
