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
from typing import Any

from pygridgain.constants import *
from .base import GridGainDataType
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


class PrimitiveArray(GridGainDataType):
    """
    Base class for array of primitives. Payload-only.
    """
    _type_name = None
    _type_id = None
    primitive_type = None
    type_code = None

    @staticmethod
    def hashcode(value: Any) -> int:
        # Arrays are not supported as keys at the moment.
        return 0

    @classmethod
    def build_header_class(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def parse(cls, client: 'Client'):
        header_class = cls.build_header_class()
        buffer = client.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('data', cls.primitive_type.c_type * header.length),
                ],
            }
        )
        buffer += client.recv(
            ctypes.sizeof(final_class) - ctypes.sizeof(header_class)
        )
        return final_class, buffer

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        result = []
        for i in range(ctype_object.length):
            result.append(ctype_object.data[i])
        return result

    @classmethod
    def from_python(cls, value):
        header_class = cls.build_header_class()
        header = header_class()
        if hasattr(header, 'type_code'):
            header.type_code = int.from_bytes(
                cls.type_code,
                byteorder=PROTOCOL_BYTE_ORDER
            )
        length = len(value)
        header.length = length
        buffer = bytearray(header)

        for x in value:
            buffer += cls.primitive_type.from_python(x)
        return bytes(buffer)


class ByteArray(PrimitiveArray):
    _type_name = NAME_BYTE_ARR
    _type_id = TYPE_BYTE_ARR
    primitive_type = Byte
    type_code = TC_BYTE_ARRAY

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        return bytearray(ctype_object.data)

    @classmethod
    def from_python(cls, value):
        header_class = cls.build_header_class()
        header = header_class()

        # no need to iterate on bytes or bytearray
        # to create ByteArray data buffer
        header.length = len(value)
        return bytes(bytearray(header) + bytearray(value))


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


class PrimitiveArrayObject(PrimitiveArray):
    """
    Base class for primitive array object. Type code plus payload.
    """
    _type_name = None
    _type_id = None
    pythonic = list
    default = []

    @classmethod
    def build_header_class(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                ],
            }
        )


class ByteArrayObject(PrimitiveArrayObject):
    _type_name = NAME_BYTE_ARR
    _type_id = TYPE_BYTE_ARR
    primitive_type = Byte
    type_code = TC_BYTE_ARRAY

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        return ByteArray.to_python(ctype_object, *args, **kwargs)

    @classmethod
    def from_python(cls, value):
        header_class = cls.build_header_class()
        header = header_class()
        header.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )

        # no need to iterate on bytes or bytearray
        # to create ByteArrayObject data buffer
        header.length = len(value)
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

        return bytes(bytearray(header) + value_buffer)


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
    def to_python(cls, ctype_object, *args, **kwargs):
        values = super().to_python(ctype_object, *args, **kwargs)
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
