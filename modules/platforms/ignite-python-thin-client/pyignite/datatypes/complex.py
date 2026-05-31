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
import asyncio
from collections import OrderedDict
import ctypes
from io import SEEK_CUR
from typing import Optional

from pyignite.constants import *
from pyignite.exceptions import ParseError
from .base import IgniteDataType
from .internal import AnyDataObject, Struct, infer_from_python, infer_from_python_async
from .type_codes import *
from .type_ids import *
from .type_names import *
from .null_object import Null, Nullable
from ..stream import AioBinaryStream, BinaryStream

__all__ = ['Map', 'ObjectArrayObject', 'CollectionObject', 'MapObject', 'WrappedDataObject', 'BinaryObject']


class ObjectArrayObject(Nullable):
    """
    Array of Ignite objects of any consistent type. Its Python representation
    is tuple(type_id, iterable of any type). The only type ID that makes sense
    in Python client is :py:attr:`~OBJECT`, that corresponds directly to
    the root object type in Java type hierarchy (`java.lang.Object`).
    """
    OBJECT = -1

    _type_name = NAME_OBJ_ARR
    _type_id = TYPE_OBJ_ARR
    _fields = [
        ('type_code', ctypes.c_byte),
        ('type_id', ctypes.c_int),
        ('length', ctypes.c_int)
    ]
    type_code = TC_OBJECT_ARRAY

    @classmethod
    def parse_not_null(cls, stream):
        length, fields = cls.__get_length(stream), []

        for i in range(length):
            c_type = AnyDataObject.parse(stream)
            fields.append((f'element_{i}', c_type))

        return cls.__build_final_class(fields)

    @classmethod
    async def parse_not_null_async(cls, stream):
        length, fields = cls.__get_length(stream), []
        for i in range(length):
            c_type = await AnyDataObject.parse_async(stream)
            fields.append((f'element_{i}', c_type))

        return cls.__build_final_class(fields)

    @classmethod
    def __get_length(cls, stream):
        int_sz, b_sz = ctypes.sizeof(ctypes.c_int), ctypes.sizeof(ctypes.c_byte)
        length = int.from_bytes(
            stream.slice(stream.tell() + b_sz + int_sz, int_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        stream.seek(2 * int_sz + b_sz, SEEK_CUR)
        return length

    @classmethod
    def __build_final_class(cls, fields):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': cls._fields + fields,
            }
        )

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        result = []
        for i in range(ctypes_object.length):
            result.append(
                AnyDataObject.to_python(
                    getattr(ctypes_object, f'element_{i}'), **kwargs
                )
            )
        return ctypes_object.type_id, result

    @classmethod
    async def to_python_not_null_async(cls, ctypes_object, **kwargs):
        result = [
            await AnyDataObject.to_python_async(
                getattr(ctypes_object, f'element_{i}'), **kwargs
            )
            for i in range(ctypes_object.length)]
        return ctypes_object.type_id, result

    @classmethod
    def from_python_not_null(cls, stream, value, *args, **kwargs):
        value = cls.__write_header(stream, value)
        for x in value:
            infer_from_python(stream, x)

    @classmethod
    async def from_python_not_null_async(cls, stream, value, *args, **kwargs):
        value = cls.__write_header(stream, value)
        for x in value:
            await infer_from_python_async(stream, x)

    @classmethod
    def __write_header(cls, stream, value):
        type_id, value = value
        try:
            length = len(value)
        except TypeError:
            value = [value]
            length = 1

        stream.write(cls.type_code)
        stream.write(type_id.to_bytes(ctypes.sizeof(ctypes.c_int), byteorder=PROTOCOL_BYTE_ORDER, signed=True))
        stream.write(length.to_bytes(ctypes.sizeof(ctypes.c_int), byteorder=PROTOCOL_BYTE_ORDER))

        return value


class WrappedDataObject(Nullable):
    """
    One or more binary objects can be wrapped in an array. This allows reading,
    storing, passing and writing objects efficiently without understanding
    their contents, performing simple byte copy.

    Python representation: tuple(payload: bytes, offset: integer). Offset
    points to the root object of the array.
    """
    type_code = TC_ARRAY_WRAPPED_OBJECTS

    @classmethod
    def parse_not_null(cls, stream):
        int_sz, b_sz = ctypes.sizeof(ctypes.c_int), ctypes.sizeof(ctypes.c_byte)
        length = int.from_bytes(
            stream.slice(stream.tell() + b_sz, int_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )

        final_class = type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                    ('payload', ctypes.c_byte * length),
                    ('offset', ctypes.c_int),
                ],
            }
        )

        stream.seek(ctypes.sizeof(final_class), SEEK_CUR)
        return final_class

    @classmethod
    def to_python_not_null(cls, ctypes_object, *args, **kwargs):
        return bytes(ctypes_object.payload), ctypes_object.offset

    @classmethod
    def from_python_not_null(cls, stream, value, *args, **kwargs):
        raise ParseError('Send unwrapped data.')


class CollectionObject(Nullable):
    """
    Similar to object array, but contains platform-agnostic deserialization
    type hint instead of type ID.

    Represented as tuple(hint, iterable of any type) in Python. Hints are:

    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.USER_SET` −
      a set of unique Ignite thin data objects. The exact Java type of a set
      is undefined,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.USER_COL` −
      a collection of Ignite thin data objects. The exact Java type
      of a collection is undefined,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.ARR_LIST` −
      represents the `java.util.ArrayList` type,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.LINKED_LIST` −
      represents the `java.util.LinkedList` type,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.HASH_SET`−
      represents the `java.util.HashSet` type,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.LINKED_HASH_SET` −
      represents the `java.util.LinkedHashSet` type,
    * :py:attr:`~pyignite.datatypes.complex.CollectionObject.SINGLETON_LIST` −
      represents the return type of the `java.util.Collection.singletonList`
      method.

    It is safe to say that `USER_SET` (`set` in Python) and `USER_COL` (`list`)
    can cover all the imaginable use cases from Python perspective.
    """
    USER_SET = -1
    USER_COL = 0
    ARR_LIST = 1
    LINKED_LIST = 2
    HASH_SET = 3
    LINKED_HASH_SET = 4
    SINGLETON_LIST = 5

    _type_name = NAME_COL
    _type_id = TYPE_COL
    _header_class = None
    type_code = TC_COLLECTION

    @classmethod
    def parse_not_null(cls, stream):
        fields, length = cls.__parse_header(stream)

        for i in range(length):
            c_type = AnyDataObject.parse(stream)
            fields.append((f'element_{i}', c_type))

        return cls.__build_final_class(fields)

    @classmethod
    async def parse_not_null_async(cls, stream):
        fields, length = cls.__parse_header(stream)

        for i in range(length):
            c_type = await AnyDataObject.parse_async(stream)
            fields.append((f'element_{i}', c_type))

        return cls.__build_final_class(fields)

    @classmethod
    def __parse_header(cls, stream):
        int_sz, b_sz = ctypes.sizeof(ctypes.c_int), ctypes.sizeof(ctypes.c_byte)
        header_fields = [('type_code', ctypes.c_byte), ('length', ctypes.c_int), ('type', ctypes.c_byte)]
        length = int.from_bytes(
            stream.slice(stream.tell() + b_sz, int_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        stream.seek(int_sz + 2 * b_sz, SEEK_CUR)
        return header_fields, length

    @classmethod
    def __build_final_class(cls, fields):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )

    @classmethod
    def to_python_not_null(cls, ctypes_object, *args, **kwargs):
        result = [
            AnyDataObject.to_python(getattr(ctypes_object, f'element_{i}'), **kwargs)
            for i in range(ctypes_object.length)
        ]
        return ctypes_object.type, result

    @classmethod
    async def to_python_not_null_async(cls, ctypes_object, *args, **kwargs):
        result_coro = [
            AnyDataObject.to_python_async(getattr(ctypes_object, f'element_{i}'), **kwargs)
            for i in range(ctypes_object.length)
        ]

        return ctypes_object.type, await asyncio.gather(*result_coro)

    @classmethod
    def from_python_not_null(cls, stream, value, *args, **kwargs):
        type_id, value = value
        try:
            length = len(value)
        except TypeError:
            value = [value]
            length = 1

        cls.__write_header(stream, type_id, length)
        for x in value:
            infer_from_python(stream, x)

    @classmethod
    async def from_python_not_null_async(cls, stream, value, *args, **kwargs):
        type_id, value = value
        try:
            length = len(value)
        except TypeError:
            value = [value]
            length = 1

        cls.__write_header(stream, type_id, length)
        for x in value:
            await infer_from_python_async(stream, x)

    @classmethod
    def __write_header(cls, stream, type_id, length):
        stream.write(cls.type_code)
        stream.write(length.to_bytes(
            ctypes.sizeof(ctypes.c_int), byteorder=PROTOCOL_BYTE_ORDER
        ))
        stream.write(type_id.to_bytes(
            length=ctypes.sizeof(ctypes.c_byte),
            byteorder=PROTOCOL_BYTE_ORDER,
            signed=True)
        )


class _MapBase:
    HASH_MAP = 1
    LINKED_HASH_MAP = 2

    @classmethod
    def _parse_header(cls, stream):
        raise NotImplementedError

    @classmethod
    def _parse(cls, stream):
        fields, length = cls._parse_header(stream)
        for i in range(length << 1):
            c_type = AnyDataObject.parse(stream)
            fields.append((f'element_{i}', c_type))
        return cls.__build_final_class(fields)

    @classmethod
    async def _parse_async(cls, stream):
        fields, length = cls._parse_header(stream)
        for i in range(length << 1):
            c_type = await AnyDataObject.parse_async(stream)
            fields.append((f'element_{i}', c_type))

        return cls.__build_final_class(fields)

    @classmethod
    def __build_final_class(cls, fields):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )

    @classmethod
    def _to_python(cls, ctypes_object, **kwargs):
        map_cls = cls.__get_map_class(ctypes_object)

        result = map_cls()
        for i in range(0, ctypes_object.length << 1, 2):
            k = AnyDataObject.to_python(getattr(ctypes_object, f'element_{i}'), **kwargs)
            v = AnyDataObject.to_python(getattr(ctypes_object, f'element_{i + 1}'), **kwargs)
            result[k] = v
        return result

    @classmethod
    async def _to_python_async(cls, ctypes_object, **kwargs):
        map_cls = cls.__get_map_class(ctypes_object)

        kv_pairs_coro = [
            asyncio.gather(
                AnyDataObject.to_python_async(
                    getattr(ctypes_object, f'element_{i}'), **kwargs
                ),
                AnyDataObject.to_python_async(
                    getattr(ctypes_object, f'element_{i + 1}'), **kwargs
                )
            ) for i in range(0, ctypes_object.length << 1, 2)
        ]

        return map_cls(await asyncio.gather(*kv_pairs_coro))

    @classmethod
    def __get_map_class(cls, ctypes_object):
        map_type = getattr(ctypes_object, 'type', cls.HASH_MAP)
        return OrderedDict if map_type == cls.LINKED_HASH_MAP else dict

    @classmethod
    def _from_python(cls, stream, value, type_id=None):
        cls._write_header(stream, type_id, len(value))
        for k, v in value.items():
            infer_from_python(stream, k)
            infer_from_python(stream, v)

    @classmethod
    async def _from_python_async(cls, stream, value, type_id):
        cls._write_header(stream, type_id, len(value))
        for k, v in value.items():
            await infer_from_python_async(stream, k)
            await infer_from_python_async(stream, v)

    @classmethod
    def _write_header(cls, stream, type_id, length):
        raise NotImplementedError


class Map(IgniteDataType, _MapBase):
    """
    Dictionary type, payload-only.

    Ignite does not track the order of key-value pairs in its caches, hence
    the ordinary Python dict type, not the collections.OrderedDict.
    """
    _type_name = NAME_MAP
    _type_id = TYPE_MAP

    @classmethod
    def parse(cls, stream):
        return cls._parse(stream)

    @classmethod
    async def parse_async(cls, stream):
        return await cls._parse_async(stream)

    @classmethod
    def _parse_header(cls, stream):
        int_sz = ctypes.sizeof(ctypes.c_int)
        length = int.from_bytes(
            stream.slice(stream.tell(), int_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        stream.seek(int_sz, SEEK_CUR)
        return [('length', ctypes.c_int)], length

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        return cls._to_python(ctypes_object, **kwargs)

    @classmethod
    async def to_python_async(cls, ctypes_object, **kwargs):
        return await cls._to_python_async(ctypes_object, **kwargs)

    @classmethod
    def from_python(cls, stream, value, type_id=None):
        return cls._from_python(stream, value, type_id)

    @classmethod
    async def from_python_async(cls, stream, value, type_id=None):
        return await cls._from_python_async(stream, value, type_id)

    @classmethod
    def _write_header(cls, stream, type_id, length):
        stream.write(length.to_bytes(
            length=ctypes.sizeof(ctypes.c_int),
            byteorder=PROTOCOL_BYTE_ORDER
        ))


class MapObject(Nullable, _MapBase):
    """
    This is a dictionary type.

    Represented as tuple(type_id, value).

    Type ID can be a :py:attr:`~HASH_MAP` (corresponds to an ordinary `dict`
    in Python) or a :py:attr:`~LINKED_HASH_MAP` (`collections.OrderedDict`).
    """
    _type_name = NAME_MAP
    _type_id = TYPE_MAP
    type_code = TC_MAP

    @classmethod
    def parse_not_null(cls, stream):
        return cls._parse(stream)

    @classmethod
    async def parse_not_null_async(cls, stream):
        return await cls._parse_async(stream)

    @classmethod
    def _parse_header(cls, stream):
        int_sz, b_sz = ctypes.sizeof(ctypes.c_int), ctypes.sizeof(ctypes.c_byte)
        length = int.from_bytes(
            stream.slice(stream.tell() + b_sz, int_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        stream.seek(int_sz + 2 * b_sz, SEEK_CUR)
        fields = [('type_code', ctypes.c_byte), ('length', ctypes.c_int), ('type', ctypes.c_byte)]
        return fields, length

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return ctypes_object.type, cls._to_python(ctypes_object, **kwargs)

    @classmethod
    async def to_python_not_null_async(cls, ctypes_object, **kwargs):
        return ctypes_object.type, await cls._to_python_async(ctypes_object, **kwargs)

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        type_id, value = value
        if value is None:
            Null.from_python(stream)
        else:
            cls._from_python(stream, value, type_id)

    @classmethod
    async def from_python_not_null_async(cls, stream, value, **kwargs):
        type_id, value = value
        if value is None:
            Null.from_python(stream)
        else:
            await cls._from_python_async(stream, value, type_id)

    @classmethod
    def _write_header(cls, stream, type_id, length):
        stream.write(cls.type_code)
        stream.write(length.to_bytes(
            length=ctypes.sizeof(ctypes.c_int),
            byteorder=PROTOCOL_BYTE_ORDER)
        )
        stream.write(type_id.to_bytes(
            length=ctypes.sizeof(ctypes.c_byte),
            byteorder=PROTOCOL_BYTE_ORDER,
            signed=True)
        )


class BinaryObject(Nullable):
    _type_id = TYPE_BINARY_OBJ
    _header_class = None
    type_code = TC_COMPLEX_OBJECT

    USER_TYPE = 0x0001
    HAS_SCHEMA = 0x0002
    HAS_RAW_DATA = 0x0004
    OFFSET_ONE_BYTE = 0x0008
    OFFSET_TWO_BYTES = 0x0010
    COMPACT_FOOTER = 0x0020

    @classmethod
    def hashcode(cls, value: object, client: Optional['Client'] = None) -> int:
        # binary objects's hashcode implementation is special in the sense
        # that you need to fully serialize the object to calculate
        # its hashcode
        if not value._hashcode and client:
            with BinaryStream(client) as stream:
                value._from_python(stream, save_to_buf=True)

        return value._hashcode

    @classmethod
    async def hashcode_async(cls, value: object, client: Optional['AioClient'] = None) -> int:
        if not value._hashcode and client:
            with AioBinaryStream(client) as stream:
                await value._from_python_async(stream, save_to_buf=True)

        return value._hashcode

    @classmethod
    def get_header_class(cls):
        if not cls._header_class:
            cls._header_class = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                        ('version', ctypes.c_byte),
                        ('flags', ctypes.c_short),
                        ('type_id', ctypes.c_int),
                        ('hash_code', ctypes.c_int),
                        ('length', ctypes.c_int),
                        ('schema_id', ctypes.c_int),
                        ('schema_offset', ctypes.c_int),
                    ],
                }
            )
        return cls._header_class

    @classmethod
    def offset_c_type(cls, flags: int):
        if flags & cls.OFFSET_ONE_BYTE:
            return ctypes.c_ubyte
        if flags & cls.OFFSET_TWO_BYTES:
            return ctypes.c_uint16
        return ctypes.c_uint

    @classmethod
    def schema_type(cls, flags: int):
        if flags & cls.COMPACT_FOOTER:
            return cls.offset_c_type(flags)
        return type(
            'SchemaElement',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('field_id', ctypes.c_int),
                    ('offset', cls.offset_c_type(flags)),
                ],
            },
        )

    @classmethod
    def parse_not_null(cls, stream):
        header, header_class = cls.__parse_header(stream)

        # ignore full schema, always retrieve fields' types and order
        # from complex types registry
        data_class = stream.get_dataclass(header)
        object_fields_struct = cls.__build_object_fields_struct(data_class)
        object_fields = object_fields_struct.parse(stream)

        return cls.__build_final_class(stream, header, header_class, object_fields,
                                       len(object_fields_struct.fields))

    @classmethod
    async def parse_not_null_async(cls, stream):
        header, header_class = cls.__parse_header(stream)

        # ignore full schema, always retrieve fields' types and order
        # from complex types registry
        data_class = await stream.get_dataclass(header)
        object_fields_struct = cls.__build_object_fields_struct(data_class)
        object_fields = await object_fields_struct.parse_async(stream)

        return cls.__build_final_class(stream, header, header_class, object_fields,
                                       len(object_fields_struct.fields))

    @classmethod
    def __parse_header(cls, stream):
        header_class = cls.get_header_class()
        header = stream.read_ctype(header_class)
        stream.seek(ctypes.sizeof(header_class), SEEK_CUR)
        return header, header_class

    @staticmethod
    def __build_object_fields_struct(data_class):
        fields = data_class.schema.items()
        return Struct(fields)

    @classmethod
    def __build_final_class(cls, stream, header, header_class, object_fields, fields_len):
        final_class_fields = [('object_fields', object_fields)]
        if header.flags & cls.HAS_SCHEMA:
            schema = cls.schema_type(header.flags) * fields_len
            stream.seek(ctypes.sizeof(schema), SEEK_CUR)
            final_class_fields.append(('schema', schema))

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': final_class_fields,
            }
        )
        # register schema encoding approach
        stream.compact_footer = bool(header.flags & cls.COMPACT_FOOTER)
        return final_class

    @classmethod
    def to_python_not_null(cls, ctypes_object, client: 'Client' = None, **kwargs):
        type_id = ctypes_object.type_id
        if not client:
            raise ParseError(f'Can not query binary type {type_id}')

        data_class = client.query_binary_type(type_id, ctypes_object.schema_id)
        result = data_class()
        result.version = ctypes_object.version

        for field_name, field_type in data_class.schema.items():
            setattr(
                result, field_name, field_type.to_python(
                    getattr(ctypes_object.object_fields, field_name), client=client, **kwargs
                )
            )
        return result

    @classmethod
    async def to_python_not_null_async(cls, ctypes_object, client: 'AioClient' = None, **kwargs):
        type_id = ctypes_object.type_id
        if not client:
            raise ParseError(f'Can not query binary type {type_id}')

        data_class = await client.query_binary_type(type_id, ctypes_object.schema_id)
        result = data_class()
        result.version = ctypes_object.version

        field_values = await asyncio.gather(
            *[
                field_type.to_python_async(
                    getattr(ctypes_object.object_fields, field_name), client=client, **kwargs
                )
                for field_name, field_type in data_class.schema.items()
            ]
        )

        for i, field_name in enumerate(data_class.schema.keys()):
            setattr(result, field_name, field_values[i])

        return result

    @classmethod
    def __get_type_id(cls, ctypes_object, client):
        type_id = getattr(ctypes_object, "type_id", None)
        if type_id:
            if not client:
                raise ParseError(f'Can not query binary type {type_id}')
            return type_id
        return None

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        if cls.__write_fast_path(stream, value):
            stream.register_binary_type(value.__class__)
            value._from_python(stream)

    @classmethod
    async def from_python_not_null_async(cls, stream, value, **kwargs):
        if cls.__write_fast_path(stream, value):
            await stream.register_binary_type(value.__class__)
            await value._from_python_async(stream)

    @classmethod
    def __write_fast_path(cls, stream, value):
        if getattr(value, '_buffer', None):
            stream.write(value._buffer)
            return False
        return True
