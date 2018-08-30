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
from typing import Any

import attr

from pyignite.datatypes import *
from pyignite.exceptions import ParseError
from pyignite.utils import entity_id, schema_id


ALLOWED_FIELD_TYPES = [
    Null, ByteObject, ShortObject, IntObject, LongObject, FloatObject,
    DoubleObject, CharObject, BoolObject, UUIDObject, DateObject,
    TimestampObject, TimeObject, EnumObject, BinaryEnumObject,
    ByteArrayObject, ShortArrayObject, IntArrayObject, LongArrayObject,
    FloatArrayObject, DoubleArrayObject, CharArrayObject, BoolArrayObject,
    UUIDArrayObject, DateArrayObject, TimestampArrayObject,
    TimeArrayObject, EnumArrayObject, String, StringArrayObject,
    DecimalObject, DecimalArrayObject, ObjectArrayObject, CollectionObject,
    MapObject, BinaryObject, WrappedDataObject,
]


class GenericObjectPropsMixin:
    """
    This class is mixed both to metaclass and to resulting class to make class
    properties universally available. You should not subclass it directly.
    """
    @property
    def type_name(self) -> str:
        """ Binary object type name. """
        return self._type_name

    @property
    def type_id(self) -> int:
        """ Binary object type ID. """
        return entity_id(self._type_name)

    @property
    def schema(self) -> OrderedDict:
        """ Binary object schema. """
        return self._schema.copy()

    @property
    def schema_id(self) -> int:
        """ Binary object schema ID. """
        return schema_id(self._schema)

    def __new__(cls, *args, **kwargs) -> Any:
        # allow all items in Binary Object schema to be populated as optional
        # arguments to `__init__()` with sensible defaults.
        if cls is not GenericObjectMeta:
            attributes = {
                k: attr.ib(
                    type=getattr(v, 'pythonic', type(None)),
                    default=getattr(v, 'default', None),
                ) for k, v in cls.schema.items()
            }
            attributes.update({'version': attr.ib(type=int, default=1)})
            cls = attr.s(cls, these=attributes)
        # skip parameters
        return super().__new__(cls)


class GenericObjectMeta(type, GenericObjectPropsMixin):
    """
    Complex (or Binary) Object metaclass. It is aimed to help user create
    classes, which objects could serve as a pythonic representation of the
    :class:`~pyignite.datatypes.complex.BinaryObject` Ignite data type.
    """
    _schema = None
    _type_name = None
    version = None

    def __new__(
        mcs: Any, name: str, base_classes: tuple, namespace: dict, **kwargs
    ) -> Any:
        """ Sort out class creation arguments. """
        return super().__new__(
            mcs, name, (GenericObjectPropsMixin, )+base_classes, namespace
        )

    @staticmethod
    def _validate_schema(schema: dict):
        for field_type in schema.values():
            if field_type not in ALLOWED_FIELD_TYPES:
                raise ParseError(
                    'Wrong binary field type: {}'.format(field_type)
                )

    def __init__(
        cls, name: str, base_classes: tuple, namespace: dict,
        type_name: str=None, schema: OrderedDict=None, **kwargs
    ):
        """
        Initializes binary object class.

        :param type_name: (optional) binary object name. Defaults to class
         name,
        :param schema: (optional) a dict of field names: field types,
        :raise: ParseError if one or more binary field types
         did not recognized.
        """
        cls._type_name = type_name or cls.__name__
        cls._type_id = entity_id(cls._type_name)
        schema = schema or OrderedDict()
        cls._validate_schema(schema)
        cls._schema = schema
        super().__init__(name, base_classes, namespace)
