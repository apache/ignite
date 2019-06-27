#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import ctypes

from pygridgain.constants import *
from pygridgain.utils import unsigned
from .base import GridGainDataType
from .type_codes import *
from .type_ids import *
from .type_names import *


__all__ = [
    'DataObject', 'ByteObject', 'ShortObject', 'IntObject', 'LongObject',
    'FloatObject', 'DoubleObject', 'CharObject', 'BoolObject',
]


class DataObject(GridGainDataType):
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
    def parse(cls, client: 'Client'):
        data_type = cls.build_c_type()
        buffer = client.recv(ctypes.sizeof(data_type))
        return data_type, buffer

    @staticmethod
    def to_python(ctype_object, *args, **kwargs):
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
    _type_name = NAME_BYTE
    _type_id = TYPE_BYTE
    c_type = ctypes.c_byte
    type_code = TC_BYTE
    pythonic = int
    default = 0

    @staticmethod
    def hashcode(value: int, *args, **kwargs) -> int:
        return value


class ShortObject(DataObject):
    _type_name = NAME_SHORT
    _type_id = TYPE_SHORT
    c_type = ctypes.c_short
    type_code = TC_SHORT
    pythonic = int
    default = 0

    @staticmethod
    def hashcode(value: int, *args, **kwargs) -> int:
        return value


class IntObject(DataObject):
    _type_name = NAME_INT
    _type_id = TYPE_INT
    c_type = ctypes.c_int
    type_code = TC_INT
    pythonic = int
    default = 0

    @staticmethod
    def hashcode(value: int, *args, **kwargs) -> int:
        return value


class LongObject(DataObject):
    _type_name = NAME_LONG
    _type_id = TYPE_LONG
    c_type = ctypes.c_longlong
    type_code = TC_LONG
    pythonic = int
    default = 0

    @staticmethod
    def hashcode(value: int, *args, **kwargs) -> int:
        return value ^ (unsigned(value, ctypes.c_ulonglong) >> 32)


class FloatObject(DataObject):
    _type_name = NAME_FLOAT
    _type_id = TYPE_FLOAT
    c_type = ctypes.c_float
    type_code = TC_FLOAT
    pythonic = float
    default = 0.0

    @staticmethod
    def hashcode(value: float, *args, **kwargs) -> int:
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

    @staticmethod
    def hashcode(value: float, *args, **kwargs) -> int:
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

    @staticmethod
    def hashcode(value: str, *args, **kwargs) -> int:
        return ord(value)

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
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
    _type_name = NAME_BOOLEAN
    _type_id = TYPE_BOOLEAN
    c_type = ctypes.c_bool
    type_code = TC_BOOL
    pythonic = bool
    default = False

    @staticmethod
    def hashcode(value: bool, *args, **kwargs) -> int:
        return 1231 if value else 1237
