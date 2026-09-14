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
from .base import IgniteDataType
from .null_object import Nullable
from .primitive import *
from .type_codes import *
from .type_ids import *
from .type_names import *


__all__ = [
    'ByteArray', 'ByteArrayObject', 'ShortArray', 'ShortArrayObject',
    'IntArray', 'IntArrayObject', 'LongArray', 'LongArrayObject',
    'FloatArray', 'FloatArrayObject', 'DoubleArray', 'DoubleArrayObject',
    'CharArray', 'CharArrayObject', 'BoolArray', 'BoolArrayObject',
]


class PrimitiveArray(IgniteDataType):
    """
    Base class for array of primitives. Payload-only.
    """
    _type_name = None
    _type_id = None
    primitive_type = None

    @classmethod
    def build_c_type(cls, stream):
        length = int.from_bytes(
            stream.slice(stream.tell(), ctypes.sizeof(ctypes.c_int)),
            byteorder=PROTOCOL_BYTE_ORDER
        )

        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure, ),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                    ('data', cls.primitive_type.c_type * length),
                ],
            }
        )

    @classmethod
    def parse(cls, stream):
        c_type = cls.build_c_type(stream)
        stream.seek(ctypes.sizeof(c_type), SEEK_CUR)
        return c_type

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        return [ctypes_object.data[i] for i in range(ctypes_object.length)]

    @classmethod
    def _write_header(cls, stream, value):
        stream.write(len(value).to_bytes(ctypes.sizeof(ctypes.c_int), byteorder=PROTOCOL_BYTE_ORDER))

    @classmethod
    def from_python(cls, stream, value, **kwargs):
        cls._write_header(stream, value)
        for x in value:
            cls.primitive_type.from_python(stream, x)


class ByteArray(PrimitiveArray):
    _type_name = NAME_BYTE_ARR
    _type_id = TYPE_BYTE_ARR
    primitive_type = Byte
    type_code = TC_BYTE_ARRAY

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        return bytes(ctypes_object.data)

    @classmethod
    def from_python(cls, stream, value, **kwargs):
        cls._write_header(stream, value)
        stream.write(bytearray(value))


class ShortArray(PrimitiveArray):
    _type_name = NAME_SHORT_ARR
    _type_id = TYPE_SHORT_ARR
    primitive_type = Short
    type_code = TC_SHORT_ARRAY


class IntArray(PrimitiveArray):
    _type_name = NAME_INT_ARR
    _type_id = TYPE_INT_ARR
    primitive_type = Int
    type_code = TC_INT_ARRAY


class LongArray(PrimitiveArray):
    _type_name = NAME_LONG_ARR
    _type_id = TYPE_LONG_ARR
    primitive_type = Long
    type_code = TC_LONG_ARRAY


class FloatArray(PrimitiveArray):
    _type_name = NAME_FLOAT_ARR
    _type_id = TYPE_FLOAT_ARR
    primitive_type = Float
    type_code = TC_FLOAT_ARRAY


class DoubleArray(PrimitiveArray):
    _type_name = NAME_DOUBLE_ARR
    _type_id = TYPE_DOUBLE_ARR
    primitive_type = Double
    type_code = TC_DOUBLE_ARRAY


class CharArray(PrimitiveArray):
    _type_name = NAME_CHAR_ARR
    _type_id = TYPE_CHAR_ARR
    primitive_type = Char
    type_code = TC_CHAR_ARRAY


class BoolArray(PrimitiveArray):
    _type_name = NAME_BOOLEAN_ARR
    _type_id = TYPE_BOOLEAN_ARR
    primitive_type = Bool
    type_code = TC_BOOL_ARRAY


class PrimitiveArrayObject(Nullable):
    """
    Base class for primitive array object. Type code plus payload.
    """
    _type_name = None
    _type_id = None
    primitive_type = None
    type_code = None
    pythonic = list
    default = []

    @classmethod
    def build_c_type(cls, stream):
        length = int.from_bytes(
            stream.slice(stream.tell() + ctypes.sizeof(ctypes.c_byte), ctypes.sizeof(ctypes.c_int)),
            byteorder=PROTOCOL_BYTE_ORDER
        )

        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                    ('data', cls.primitive_type.c_type * length),
                ],
            }
        )

    @classmethod
    def parse_not_null(cls, stream):
        c_type = cls.build_c_type(stream)
        stream.seek(ctypes.sizeof(c_type), SEEK_CUR)
        return c_type

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return [ctypes_object.data[i] for i in range(ctypes_object.length)]

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        cls._write_header(stream, value)
        for x in value:
            cls.primitive_type.from_python(stream, x)

    @classmethod
    def _write_header(cls, stream, value):
        stream.write(cls.type_code)
        stream.write(len(value).to_bytes(ctypes.sizeof(ctypes.c_int), byteorder=PROTOCOL_BYTE_ORDER))


class ByteArrayObject(PrimitiveArrayObject):
    _type_name = NAME_BYTE_ARR
    _type_id = TYPE_BYTE_ARR
    primitive_type = Byte
    type_code = TC_BYTE_ARRAY

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return bytes(ctypes_object.data)

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        cls._write_header(stream, value)

        if isinstance(value, (bytes, bytearray)):
            stream.write(value)
            return

        try:
            # `value` is a `bytearray` or a sequence of integer values
            # in range 0 to 255
            value_buffer = bytearray(value)
        except ValueError:
            # `value` is a sequence of integers in range -128 to 127
            value_buffer = bytearray()
            for ch in value:
                if -128 <= ch <= 255:
                    value_buffer.append(ctypes.c_ubyte(ch).value)
                else:
                    raise ValueError(
                        'byte must be in range(-128, 256)!'
                    ) from None

        stream.write(value_buffer)


class ShortArrayObject(PrimitiveArrayObject):
    _type_name = NAME_SHORT_ARR
    _type_id = TYPE_SHORT_ARR
    primitive_type = Short
    type_code = TC_SHORT_ARRAY


class IntArrayObject(PrimitiveArrayObject):
    _type_name = NAME_INT_ARR
    _type_id = TYPE_INT_ARR
    primitive_type = Int
    type_code = TC_INT_ARRAY


class LongArrayObject(PrimitiveArrayObject):
    _type_name = NAME_LONG_ARR
    _type_id = TYPE_LONG_ARR
    primitive_type = Long
    type_code = TC_LONG_ARRAY


class FloatArrayObject(PrimitiveArrayObject):
    _type_name = NAME_FLOAT_ARR
    _type_id = TYPE_FLOAT_ARR
    primitive_type = Float
    type_code = TC_FLOAT_ARRAY


class DoubleArrayObject(PrimitiveArrayObject):
    _type_name = NAME_DOUBLE_ARR
    _type_id = TYPE_DOUBLE_ARR
    primitive_type = Double
    type_code = TC_DOUBLE_ARRAY


class CharArrayObject(PrimitiveArrayObject):
    _type_name = NAME_CHAR_ARR
    _type_id = TYPE_CHAR_ARR
    primitive_type = Char
    type_code = TC_CHAR_ARRAY

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        values = super().to_python_not_null(ctypes_object, **kwargs)
        return [
            v.to_bytes(
                ctypes.sizeof(cls.primitive_type.c_type),
                byteorder=PROTOCOL_BYTE_ORDER
            ).decode(
                PROTOCOL_CHAR_ENCODING
            ) for v in values
        ]


class BoolArrayObject(PrimitiveArrayObject):
    _type_name = NAME_BOOLEAN_ARR
    _type_id = TYPE_BOOLEAN_ARR
    primitive_type = Bool
    type_code = TC_BOOL_ARRAY

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return [ctypes_object.data[i] != 0 for i in range(ctypes_object.length)]
