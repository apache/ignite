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
from io import SEEK_CUR

from pyignite.constants import *
from pyignite.utils import unsigned
from .type_codes import *
from .type_ids import *
from .type_names import *
from .null_object import Nullable

__all__ = [
    'DataObject', 'ByteObject', 'ShortObject', 'IntObject', 'LongObject',
    'FloatObject', 'DoubleObject', 'CharObject', 'BoolObject',
]


class DataObject(Nullable):
    """
    Base class for primitive data objects.

    Primitive data objects are built of primitive data prepended by
    the corresponding type code.
    """
    _type_name = None
    _type_id = None
    _object_c_type = None
    c_type = None
    type_code = None

    @classmethod
    def build_c_type(cls):
        if cls._object_c_type is None:
            cls._object_c_type = type(
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
        return cls._object_c_type

    @classmethod
    def parse_not_null(cls, stream):
        data_type = cls.build_c_type()
        stream.seek(ctypes.sizeof(data_type), SEEK_CUR)
        return data_type

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return ctypes_object.value

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.value = value
        stream.write(data_object)


class ByteObject(DataObject):
    _type_name = NAME_BYTE
    _type_id = TYPE_BYTE
    c_type = ctypes.c_byte
    type_code = TC_BYTE
    pythonic = int
    default = 0

    @classmethod
    def hashcode(cls, value: int, **kwargs) -> int:
        return value


class ShortObject(DataObject):
    _type_name = NAME_SHORT
    _type_id = TYPE_SHORT
    c_type = ctypes.c_short
    type_code = TC_SHORT
    pythonic = int
    default = 0

    @classmethod
    def hashcode(cls, value: int, **kwargs) -> int:
        return value


class IntObject(DataObject):
    _type_name = NAME_INT
    _type_id = TYPE_INT
    c_type = ctypes.c_int
    type_code = TC_INT
    pythonic = int
    default = 0

    @classmethod
    def hashcode(cls, value: int, **kwargs) -> int:
        return value


class LongObject(DataObject):
    _type_name = NAME_LONG
    _type_id = TYPE_LONG
    c_type = ctypes.c_longlong
    type_code = TC_LONG
    pythonic = int
    default = 0

    @classmethod
    def hashcode(cls, value: int, **kwargs) -> int:
        return value ^ (unsigned(value, ctypes.c_ulonglong) >> 32)


class FloatObject(DataObject):
    _type_name = NAME_FLOAT
    _type_id = TYPE_FLOAT
    c_type = ctypes.c_float
    type_code = TC_FLOAT
    pythonic = float
    default = 0.0

    @classmethod
    def hashcode(cls, value: float, **kwargs) -> int:
        return ctypes.cast(
            ctypes.pointer(ctypes.c_float(value)),
            ctypes.POINTER(ctypes.c_int)
        ).contents.value


class DoubleObject(DataObject):
    _type_name = NAME_DOUBLE
    _type_id = TYPE_DOUBLE
    c_type = ctypes.c_double
    type_code = TC_DOUBLE
    pythonic = float
    default = 0.0

    @classmethod
    def hashcode(cls, value: float, **kwargs) -> int:
        bits = ctypes.cast(
            ctypes.pointer(ctypes.c_double(value)),
            ctypes.POINTER(ctypes.c_longlong)
        ).contents.value
        return (bits & 0xffffffff) ^ (unsigned(bits, ctypes.c_longlong) >> 32)


class CharObject(DataObject):
    """
    This type is a little tricky. It stores character values in
    UTF-16 Little-endian encoding. We have to encode/decode it
    to/from UTF-8 to keep the coding hassle to minimum. Bear in mind
    though: decoded character may take 1..4 bytes in UTF-8.
    """
    _type_name = NAME_CHAR
    _type_id = TYPE_CHAR
    c_type = ctypes.c_short
    type_code = TC_CHAR
    pythonic = str
    default = ' '

    @classmethod
    def hashcode(cls, value: str, **kwargs) -> int:
        return ord(value)

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        value = ctypes_object.value
        return value.to_bytes(
            ctypes.sizeof(cls.c_type),
            byteorder=PROTOCOL_BYTE_ORDER
        ).decode(PROTOCOL_CHAR_ENCODING)

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        if type(value) is str:
            value = value.encode(PROTOCOL_CHAR_ENCODING)
        # assuming either a bytes or an integer
        if type(value) is bytes:
            value = int.from_bytes(value, byteorder=PROTOCOL_BYTE_ORDER)
        # assuming a valid integer
        stream.write(cls.type_code)
        stream.write(
            value.to_bytes(ctypes.sizeof(cls.c_type), byteorder=PROTOCOL_BYTE_ORDER)
        )


class BoolObject(DataObject):
    _type_name = NAME_BOOLEAN
    _type_id = TYPE_BOOLEAN
    c_type = ctypes.c_byte  # Use c_byte because c_bool throws endianness conversion error on BE systems.
    type_code = TC_BOOL
    pythonic = bool
    default = False

    @classmethod
    def hashcode(cls, value: bool, **kwargs) -> int:
        return 1231 if value else 1237

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return ctypes_object.value != 0
