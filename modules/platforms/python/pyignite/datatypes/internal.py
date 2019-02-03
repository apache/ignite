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

from collections import OrderedDict
import ctypes
import decimal
from datetime import date, datetime, timedelta
from typing import Any, Tuple
import uuid

import attr

from pyignite.constants import *
from pyignite.exceptions import ParseError
from pyignite.utils import is_binary, is_hinted, is_iterable
from .type_codes import *


__all__ = ['AnyDataArray', 'AnyDataObject', 'Struct', 'StructArray', 'tc_map']


def tc_map(key: bytes, _memo_map: dict={}):
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
    if not _memo_map:
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

        _memo_map = {
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
    return _memo_map[key]


@attr.s
class StructArray:
    """ `counter_type` counter, followed by count*following structure. """
    following = attr.ib(type=list, factory=list)
    counter_type = attr.ib(default=ctypes.c_int)
    defaults = attr.ib(type=dict, default={})

    def build_header_class(self):
        return type(
            self.__class__.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', self.counter_type),
                ],
            },
        )

    def parse(self, client: 'Client'):
        buffer = client.recv(ctypes.sizeof(self.counter_type))
        length = int.from_bytes(buffer, byteorder=PROTOCOL_BYTE_ORDER)
        fields = []

        for i in range(length):
            c_type, buffer_fragment = Struct(self.following).parse(client)
            buffer += buffer_fragment
            fields.append(('element_{}'.format(i), c_type))

        data_class = type(
            'StructArray',
            (self.build_header_class(),),
            {
                '_pack_': 1,
                '_fields_': fields,
            },
        )

        return data_class, buffer

    def to_python(self, ctype_object, *args, **kwargs):
        result = []
        length = getattr(ctype_object, 'length', 0)
        for i in range(length):
            result.append(
                Struct(
                    self.following, dict_type=dict
                ).to_python(
                    getattr(ctype_object, 'element_{}'.format(i)),
                    *args, **kwargs
                )
            )
        return result

    def from_python(self, value):
        length = len(value)
        header_class = self.build_header_class()
        header = header_class()
        header.length = length
        buffer = bytes(header)

        for i, v in enumerate(value):
            for default_key, default_value in self.defaults.items():
                v.setdefault(default_key, default_value)
            for name, el_class in self.following:
                buffer += el_class.from_python(v[name])

        return buffer


@attr.s
class Struct:
    """ Sequence of fields, including variable-sized and nested. """
    fields = attr.ib(type=list)
    dict_type = attr.ib(default=OrderedDict)
    defaults = attr.ib(type=dict, default={})

    def parse(self, client: 'Client') -> Tuple[type, bytes]:
        buffer = b''
        fields = []

        for name, c_type in self.fields:
            c_type, buffer_fragment = c_type.parse(client)
            buffer += buffer_fragment

            fields.append((name, c_type))

        data_class = type(
            'Struct',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': fields,
            },
        )

        return data_class, buffer

    def to_python(self, ctype_object, *args, **kwargs) -> Any:
        result = self.dict_type()
        for name, c_type in self.fields:
            result[name] = c_type.to_python(
                getattr(ctype_object, name),
                *args, **kwargs
            )
        return result

    def from_python(self, value) -> bytes:
        buffer = b''

        for default_key, default_value in self.defaults.items():
            value.setdefault(default_key, default_value)

        for name, el_class in self.fields:
            buffer += el_class.from_python(value[name])

        return buffer


class AnyDataObject:
    """
    Not an actual Ignite type, but contains a guesswork
    on serializing Python data or parsing an unknown Ignite data object.
    """
    _python_map = None
    _python_array_map = None

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
        if all([
            isinstance(x, type_first)
            or ((x is None) and allow_none) for x in iterator
        ]):
            return type_first

    @classmethod
    def parse(cls, client: 'Client'):
        type_code = client.recv(ctypes.sizeof(ctypes.c_byte))
        try:
            data_class = tc_map(type_code)
        except KeyError:
            raise ParseError('Unknown type code: `{}`'.format(type_code))
        client.prefetch += type_code
        return data_class.parse(client)

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        type_code = ctype_object.type_code.to_bytes(
            ctypes.sizeof(ctypes.c_byte),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_class = tc_map(type_code)
        return data_class.to_python(ctype_object)

    @classmethod
    def _init_python_map(cls):
        """
        Optimizes Python types→Ignite types map creation for speed.

        Local imports seem inevitable here.
        """
        from pyignite.datatypes import (
            LongObject, DoubleObject, String, BoolObject, Null, UUIDObject,
            DateObject, TimeObject, DecimalObject,
        )

        cls._python_map = {
            int: LongObject,
            float: DoubleObject,
            str: String,
            bytes: String,
            bool: BoolObject,
            type(None): Null,
            uuid.UUID: UUIDObject,
            datetime: DateObject,
            date: DateObject,
            timedelta: TimeObject,
            decimal.Decimal: DecimalObject,
        }

    @classmethod
    def _init_python_array_map(cls):
        """
        Optimizes  Python types→Ignite array types map creation for speed.
        """
        from pyignite.datatypes import (
            LongArrayObject, DoubleArrayObject, StringArrayObject,
            BoolArrayObject, UUIDArrayObject, DateArrayObject, TimeArrayObject,
            DecimalArrayObject,
        )

        cls._python_array_map = {
            int: LongArrayObject,
            float: DoubleArrayObject,
            str: StringArrayObject,
            bytes: StringArrayObject,
            bool: BoolArrayObject,
            uuid.UUID: UUIDArrayObject,
            datetime: DateArrayObject,
            date: DateArrayObject,
            timedelta: TimeArrayObject,
            decimal.Decimal: DecimalArrayObject,
        }

    @classmethod
    def map_python_type(cls, value):
        from pyignite.datatypes import (
            MapObject, ObjectArrayObject, BinaryObject,
        )

        if cls._python_map is None:
            cls._init_python_map()
        if cls._python_array_map is None:
            cls._init_python_array_map()

        value_type = type(value)
        if is_iterable(value) and value_type is not str:
            value_subtype = cls.get_subtype(value)
            if value_subtype in cls._python_array_map:
                return cls._python_array_map[value_subtype]

            # a little heuristics (order may be important)
            if all([
                value_subtype is None,
                len(value) == 2,
                isinstance(value[0], int),
                isinstance(value[1], dict),
            ]):
                return MapObject

            if all([
                value_subtype is None,
                len(value) == 2,
                isinstance(value[0], int),
                is_iterable(value[1]),
            ]):
                return ObjectArrayObject

            raise TypeError(
                'Type `array of {}` is invalid'.format(value_subtype)
            )

        if is_binary(value):
            return BinaryObject

        if value_type in cls._python_map:
            return cls._python_map[value_type]
        raise TypeError(
            'Type `{}` is invalid.'.format(value_type)
        )

    @classmethod
    def from_python(cls, value):
        return cls.map_python_type(value).from_python(value)


def infer_from_python(value: Any):
    """
    Convert pythonic value to ctypes buffer, type hint-aware.

    :param value: pythonic value or (value, type_hint) tuple,
    :return: bytes.
    """
    if is_hinted(value):
        value, data_type = value
    else:
        data_type = AnyDataObject
    return data_type.from_python(value)


@attr.s
class AnyDataArray(AnyDataObject):
    """
    Sequence of AnyDataObjects, payload-only.
    """
    counter_type = attr.ib(default=ctypes.c_int)

    def build_header(self):
        return type(
            self.__class__.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', self.counter_type),
                ],
            }
        )

    def parse(self, client: 'Client'):
        header_class = self.build_header()
        buffer = client.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []

        for i in range(header.length):
            c_type, buffer_fragment = super().parse(client)
            buffer += buffer_fragment
            fields.append(('element_{}'.format(i), c_type))

        final_class = type(
            self.__class__.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )
        return final_class, buffer

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        result = []
        for i in range(ctype_object.length):
            result.append(
                super().to_python(
                    getattr(ctype_object, 'element_{}'.format(i)),
                    *args, **kwargs
                )
            )
        return result

    def from_python(self, value):
        header_class = self.build_header()
        header = header_class()

        try:
            length = len(value)
        except TypeError:
            value = [value]
            length = 1
        header.length = length
        buffer = bytes(header)

        for x in value:
            buffer += infer_from_python(x)
        return buffer
