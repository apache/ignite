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
import decimal
from datetime import date, datetime, timedelta
from io import SEEK_CUR
from typing import Any, Union, Callable, List
import uuid

import attr

from pyignite.constants import PROTOCOL_BYTE_ORDER
from pyignite.exceptions import ParseError
from pyignite.utils import is_binary, is_hinted, is_iterable
from .type_codes import *


__all__ = [
    'AnyDataArray', 'AnyDataObject', 'Struct', 'StructArray', 'tc_map', 'infer_from_python', 'infer_from_python_async'
]

from ..stream import READ_BACKWARD


_tc_map = {}


def tc_map(key: bytes):
    """
    Returns a default parser/generator class for the given type code.

    This mapping is used internally inside listed complex parser/generator
    classes, so it has to be a function. Local imports are used for the same
    reason.

    :param key: Ignite type code,
    :param _memo_map: do not use this parameter, it is for memoization
     of the “type code-type class” mapping,
    :return: parser/generator class for the type code.
    """
    global _tc_map
    if not _tc_map:
        from pyignite.datatypes import (
            Null, ByteObject, ShortObject, IntObject, LongObject, FloatObject,
            DoubleObject, CharObject, BoolObject, UUIDObject, DateObject,
            TimestampObject, TimeObject, EnumObject, BinaryEnumObject,
            ByteArrayObject, ShortArrayObject, IntArrayObject, LongArrayObject,
            FloatArrayObject, DoubleArrayObject, CharArrayObject,
            BoolArrayObject,
            UUIDArrayObject, DateArrayObject, TimestampArrayObject,
            TimeArrayObject, EnumArrayObject, String, StringArrayObject,
            DecimalObject, DecimalArrayObject, ObjectArrayObject,
            CollectionObject,
            MapObject, BinaryObject, WrappedDataObject,
        )

        _tc_map = {
            TC_NULL: Null,

            TC_BYTE: ByteObject,
            TC_SHORT: ShortObject,
            TC_INT: IntObject,
            TC_LONG: LongObject,
            TC_FLOAT: FloatObject,
            TC_DOUBLE: DoubleObject,
            TC_CHAR: CharObject,
            TC_BOOL: BoolObject,

            TC_UUID: UUIDObject,
            TC_DATE: DateObject,
            TC_TIMESTAMP: TimestampObject,
            TC_TIME: TimeObject,
            TC_ENUM: EnumObject,
            TC_BINARY_ENUM: BinaryEnumObject,

            TC_BYTE_ARRAY: ByteArrayObject,
            TC_SHORT_ARRAY: ShortArrayObject,
            TC_INT_ARRAY: IntArrayObject,
            TC_LONG_ARRAY: LongArrayObject,
            TC_FLOAT_ARRAY: FloatArrayObject,
            TC_DOUBLE_ARRAY: DoubleArrayObject,
            TC_CHAR_ARRAY: CharArrayObject,
            TC_BOOL_ARRAY: BoolArrayObject,

            TC_UUID_ARRAY: UUIDArrayObject,
            TC_DATE_ARRAY: DateArrayObject,
            TC_TIMESTAMP_ARRAY: TimestampArrayObject,
            TC_TIME_ARRAY: TimeArrayObject,
            TC_ENUM_ARRAY: EnumArrayObject,

            TC_STRING: String,
            TC_STRING_ARRAY: StringArrayObject,
            TC_DECIMAL: DecimalObject,
            TC_DECIMAL_ARRAY: DecimalArrayObject,

            TC_OBJECT_ARRAY: ObjectArrayObject,
            TC_COLLECTION: CollectionObject,
            TC_MAP: MapObject,

            TC_COMPLEX_OBJECT: BinaryObject,
            TC_ARRAY_WRAPPED_OBJECTS: WrappedDataObject,
        }
    return _tc_map[key]


class Conditional:
    def __init__(self, fields: List, predicate1: Callable[[any], bool],
                 predicate2: Callable[[any], bool], var1, var2):
        self.fields = fields
        self.predicate1 = predicate1
        self.predicate2 = predicate2
        self.var1 = var1
        self.var2 = var2

    def parse(self, stream, context):
        if self.predicate1(context):
            return self.var1.parse(stream)
        return self.var2.parse(stream)

    async def parse_async(self, stream, context):
        if self.predicate1(context):
            return await self.var1.parse_async(stream)
        return await self.var2.parse_async(stream)

    def to_python(self, ctypes_object, context, **kwargs):
        if self.predicate2(context):
            return self.var1.to_python(ctypes_object, **kwargs)
        return self.var2.to_python(ctypes_object, **kwargs)

    async def to_python_async(self, ctypes_object, context, **kwargs):
        if self.predicate2(context):
            return await self.var1.to_python_async(ctypes_object, **kwargs)
        return await self.var2.to_python_async(ctypes_object, **kwargs)


@attr.s
class StructArray:
    """ `counter_type` counter, followed by count*following structure. """
    following = attr.ib(type=list, factory=list)
    counter_type = attr.ib(default=ctypes.c_int)
    defaults = attr.ib(type=dict, default={})

    def parse(self, stream):
        fields, length = self.__parse_header(stream)

        for i in range(length):
            c_type = Struct(self.following).parse(stream)
            fields.append((f'element_{i}', c_type))

        return self.build_c_type(fields)

    async def parse_async(self, stream):
        fields, length = self.__parse_header(stream)

        for i in range(length):
            c_type = await Struct(self.following).parse_async(stream)
            fields.append((f'element_{i}', c_type))

        return self.build_c_type(fields)

    def __parse_header(self, stream):
        counter_sz = ctypes.sizeof(self.counter_type)
        length = int.from_bytes(
            stream.slice(offset=counter_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        stream.seek(counter_sz, SEEK_CUR)
        return [('length', self.counter_type)], length

    @staticmethod
    def build_c_type(fields):
        return type(
            'StructArray',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': fields,
            },
        )

    def to_python(self, ctypes_object, **kwargs):
        length = getattr(ctypes_object, 'length', 0)
        return [
            Struct(self.following, dict_type=dict).to_python(getattr(ctypes_object, f'element_{i}'), **kwargs)
            for i in range(length)
        ]

    async def to_python_async(self, ctypes_object, **kwargs):
        length = getattr(ctypes_object, 'length', 0)
        result_coro = [
            Struct(self.following, dict_type=dict).to_python_async(getattr(ctypes_object, f'element_{i}'), **kwargs)
            for i in range(length)
        ]
        return await asyncio.gather(*result_coro)

    def from_python(self, stream, value):
        self.__write_header(stream, len(value))

        for v in value:
            for default_key, default_value in self.defaults.items():
                v.setdefault(default_key, default_value)
            for name, el_class in self.following:
                el_class.from_python(stream, v[name])

    async def from_python_async(self, stream, value):
        self.__write_header(stream, len(value))

        for v in value:
            for default_key, default_value in self.defaults.items():
                v.setdefault(default_key, default_value)
            for name, el_class in self.following:
                await el_class.from_python_async(stream, v[name])

    def __write_header(self, stream, length):
        stream.write(
            length.to_bytes(ctypes.sizeof(self.counter_type),
                            byteorder=PROTOCOL_BYTE_ORDER)
        )


@attr.s
class Struct:
    """ Sequence of fields, including variable-sized and nested. """
    fields = attr.ib(type=list)
    dict_type = attr.ib(default=OrderedDict)
    defaults = attr.ib(type=dict, default={})

    def parse(self, stream):
        fields, ctx = [], self.__prepare_conditional_ctx()

        for name, c_type in self.fields:
            is_cond = isinstance(c_type, Conditional)
            c_type = c_type.parse(stream, ctx) if is_cond else c_type.parse(stream)
            fields.append((name, c_type))
            if name in ctx:
                ctx[name] = stream.read_ctype(c_type, direction=READ_BACKWARD)

        return self.build_c_type(fields)

    async def parse_async(self, stream):
        fields, ctx = [], self.__prepare_conditional_ctx()

        for name, c_type in self.fields:
            is_cond = isinstance(c_type, Conditional)
            c_type = await c_type.parse_async(stream, ctx) if is_cond else await c_type.parse_async(stream)
            fields.append((name, c_type))
            if name in ctx:
                ctx[name] = stream.read_ctype(c_type, direction=READ_BACKWARD)

        return self.build_c_type(fields)

    def __prepare_conditional_ctx(self):
        ctx = {}
        for _, c_type in self.fields:
            if isinstance(c_type, Conditional):
                for name in c_type.fields:
                    ctx[name] = None
        return ctx

    @staticmethod
    def build_c_type(fields):
        return type(
            'Struct',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': fields,
            },
        )

    def to_python(self, ctypes_object, **kwargs) -> Union[dict, OrderedDict]:
        result = self.dict_type()
        for name, c_type in self.fields:
            is_cond = isinstance(c_type, Conditional)
            result[name] = c_type.to_python(
                getattr(ctypes_object, name),
                result,
                **kwargs
            ) if is_cond else c_type.to_python(
                getattr(ctypes_object, name),
                **kwargs
            )
        return result

    async def to_python_async(self, ctypes_object, **kwargs) -> Union[dict, OrderedDict]:
        result = self.dict_type()
        for name, c_type in self.fields:
            is_cond = isinstance(c_type, Conditional)

            if is_cond:
                value = await c_type.to_python_async(
                    getattr(ctypes_object, name),
                    result,
                    **kwargs
                )
            else:
                value = await c_type.to_python_async(
                    getattr(ctypes_object, name),
                    **kwargs
                )
            result[name] = value
        return result

    def from_python(self, stream, value):
        self.__set_defaults(value)

        for name, el_class in self.fields:
            el_class.from_python(stream, value[name])

    async def from_python_async(self, stream, value):
        self.__set_defaults(value)

        for name, el_class in self.fields:
            await el_class.from_python_async(stream, value[name])

    def __set_defaults(self, value):
        for default_key, default_value in self.defaults.items():
            value.setdefault(default_key, default_value)


class AnyDataObject:
    """
    Not an actual Ignite type, but contains a guesswork
    on serializing Python data or parsing an unknown Ignite data object.
    """
    _python_map = None
    _python_array_map = None
    _map_obj_type = None
    _collection_obj_type = None
    _binary_obj_type = None

    @staticmethod
    def get_subtype(iterable, allow_none=False):
        # arrays of these types can contain Null objects
        object_array_python_types = [
            str,
            datetime,
            timedelta,
            decimal.Decimal,
            uuid.UUID,
        ]

        iterator = iter(iterable)
        type_first = type(None)
        try:
            while isinstance(None, type_first):
                type_first = type(next(iterator))
        except StopIteration:
            raise TypeError(
                'Can not represent an empty iterable '
                'or an iterable of `NoneType` in Ignite type.'
            )

        if type_first in object_array_python_types:
            allow_none = True

        # if an iterable contains items of more than one non-nullable type,
        # return None
        if all(isinstance(x, type_first) or ((x is None) and allow_none) for x in iterator):
            return type_first

    @classmethod
    def parse(cls, stream):
        data_class = cls.__data_class_parse(stream)
        return data_class.parse(stream)

    @classmethod
    async def parse_async(cls, stream):
        data_class = cls.__data_class_parse(stream)
        return await data_class.parse_async(stream)

    @classmethod
    def __data_class_parse(cls, stream):
        type_code = stream.slice(offset=ctypes.sizeof(ctypes.c_byte))
        try:
            return tc_map(type_code)
        except KeyError:
            raise ParseError('Unknown type code: `{}`'.format(type_code))

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        data_class = cls.__data_class_from_ctype(ctypes_object)
        return data_class.to_python(ctypes_object, **kwargs)

    @classmethod
    async def to_python_async(cls, ctypes_object, **kwargs):
        data_class = cls.__data_class_from_ctype(ctypes_object)
        return await data_class.to_python_async(ctypes_object, **kwargs)

    @classmethod
    def __data_class_from_ctype(cls, ctypes_object):
        type_code = ctypes_object.type_code.to_bytes(
            ctypes.sizeof(ctypes.c_byte),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        return tc_map(type_code)

    @classmethod
    def _init_python_mapping(cls):
        """
        Optimizes Python types→Ignite types map creation for speed.

        Local imports seem inevitable here.
        """
        from pyignite.datatypes import (
            LongObject, DoubleObject, String, BoolObject, Null, UUIDObject, DateObject, TimeObject,
            DecimalObject, ByteArrayObject, LongArrayObject, DoubleArrayObject, StringArrayObject,
            BoolArrayObject, UUIDArrayObject, DateArrayObject, TimeArrayObject, DecimalArrayObject,
            MapObject, CollectionObject, BinaryObject
        )

        cls._python_map = {
            int: LongObject,
            float: DoubleObject,
            str: String,
            bytes: ByteArrayObject,
            bytearray: ByteArrayObject,
            bool: BoolObject,
            type(None): Null,
            uuid.UUID: UUIDObject,
            datetime: DateObject,
            date: DateObject,
            timedelta: TimeObject,
            decimal.Decimal: DecimalObject,
        }

        cls._python_array_map = {
            int: LongArrayObject,
            float: DoubleArrayObject,
            str: StringArrayObject,
            bool: BoolArrayObject,
            uuid.UUID: UUIDArrayObject,
            datetime: DateArrayObject,
            date: DateArrayObject,
            timedelta: TimeArrayObject,
            decimal.Decimal: DecimalArrayObject,
        }

        cls._map_obj_type = MapObject
        cls._collection_obj_type = CollectionObject
        cls._binary_obj_type = BinaryObject

    @classmethod
    def map_python_type(cls, value):
        if cls._python_map is None or cls._python_array_map is None:
            cls._init_python_mapping()

        value_type = type(value)

        if value_type in cls._python_map:
            return cls._python_map[value_type]

        if is_iterable(value) and value_type not in (str, bytearray, bytes):
            value_subtype = cls.get_subtype(value)
            if value_subtype in cls._python_array_map:
                return cls._python_array_map[value_subtype]

            # a little heuristics (order is important)
            if all([
                value_subtype is None,
                len(value) == 2,
                isinstance(value[0], int),
                isinstance(value[1], dict),
            ]):
                return cls._map_obj_type

            if all([
                value_subtype is None,
                len(value) == 2,
                isinstance(value[0], int),
                is_iterable(value[1]),
            ]):
                return cls._collection_obj_type

            # no default for ObjectArrayObject, sorry

            raise TypeError(
                'Type `array of {}` is invalid'.format(value_subtype)
            )

        if is_binary(value):
            return cls._binary_obj_type

        raise TypeError(
            'Type `{}` is invalid.'.format(value_type)
        )

    @classmethod
    def from_python(cls, stream, value):
        p_type = cls.map_python_type(value)
        p_type.from_python(stream, value)

    @classmethod
    async def from_python_async(cls, stream, value):
        p_type = cls.map_python_type(value)
        await p_type.from_python_async(stream, value)


def infer_from_python(stream, value: Any):
    """
    Convert pythonic value to ctypes buffer, type hint-aware.

    :param value: pythonic value or (value, type_hint) tuple,
    :return: bytes.
    """
    value, data_type = __unpack_hinted(value)

    data_type.from_python(stream, value)


async def infer_from_python_async(stream, value: Any):
    """
    Async version of infer_from_python
    """
    value, data_type = __unpack_hinted(value)

    await data_type.from_python_async(stream, value)


def __unpack_hinted(value):
    if is_hinted(value):
        return value
    return value, AnyDataObject


@attr.s
class AnyDataArray(AnyDataObject):
    """
    Sequence of AnyDataObjects, payload-only.
    """
    counter_type = attr.ib(default=ctypes.c_int)

    def parse(self, stream):
        fields, length = self.__parse_header(stream)
        for i in range(length):
            c_type = super().parse(stream)
            fields.append((f'element_{i}', c_type))
        return self.build_c_type(fields)

    async def parse_async(self, stream):
        fields, length = self.__parse_header(stream)
        for i in range(length):
            c_type = await super().parse_async(stream)
            fields.append((f'element_{i}', c_type))
        return self.build_c_type(fields)

    def __parse_header(self, stream):
        cnt_sz = ctypes.sizeof(self.counter_type)
        length = int.from_bytes(
            stream.slice(stream.tell(), cnt_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        stream.seek(cnt_sz, SEEK_CUR)
        return [('length', self.counter_type)], length

    def build_c_type(self, fields):
        return type(
            self.__class__.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        length = getattr(ctypes_object, "length", 0)

        return [
            super().to_python(getattr(ctypes_object, f'element_{i}'), **kwargs)
            for i in range(length)
        ]

    @classmethod
    async def to_python_async(cls, ctypes_object, **kwargs):
        length = getattr(ctypes_object, "length", 0)

        values = asyncio.gather(
            *[
                super().to_python(
                    getattr(ctypes_object, f'element_{i}'),
                    *args, **kwargs
                ) for i in range(length)
            ]
        )
        return await values

    def from_python(self, stream, value):
        value = self.__write_header_and_process_value(stream, value)

        for x in value:
            infer_from_python(stream, x)

    async def from_python_async(self, stream, value):
        value = self.__write_header_and_process_value(stream, value)

        for x in value:
            await infer_from_python_async(stream, x)

    def __write_header_and_process_value(self, stream, value):
        try:
            length = len(value)
        except TypeError:
            value = [value]
            length = 1

        stream.write(length.to_bytes(
            ctypes.sizeof(self.counter_type),
            byteorder=PROTOCOL_BYTE_ORDER
        ))

        return value
