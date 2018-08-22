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

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

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

SENSIBLE_DEFAULTS = {
    ByteObject: 0,
    ShortObject: 0,
    IntObject: 0,
    LongObject: 0,
    FloatObject: 0.0,
    DoubleObject: 0.0,
    CharObject: ' ',
    BoolObject: False,
    DecimalObject: Decimal('0.00'),
    DateObject: datetime(1970, 1, 1),
    TimestampObject: (datetime(1970, 1, 1), 0),
    TimeObject: timedelta(),
    ByteArrayObject: [],
    ShortArrayObject: [],
    IntArrayObject: [],
    LongArrayObject: [],
    FloatArrayObject: [],
    DoubleArrayObject: [],
    CharArrayObject: [],
    BoolArrayObject: [],
    UUIDArrayObject: [],
    DateArrayObject: [],
    TimestampArrayObject: [],
    TimeArrayObject: [],
    DecimalArrayObject: [],
    StringArrayObject: [],
    CollectionObject: [],
    MapObject: {},
}


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
    def schema(self) -> dict:
        """ Binary object schema. """
        return self._schema

    @property
    def schema_id(self) -> int:
        """ Binary object schema ID. """
        return schema_id(self._schema)

    @property
    def version(self) -> int:
        """ Binary object version. """
        return self._version


class GenericObjectMeta(type, GenericObjectPropsMixin):
    """
    Complex (or Binary) Object metaclass. It is aimed to help user create
    classes, which objects could serve as a pythonic representation of the
    :class:`~pyignite.datatypes.complex.BinaryObject` Ignite data type.
    """
    _schema = None
    _type_name = None
    _version = None

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
        type_name: str=None, schema: dict=None, version: int=1, **kwargs
    ):
        """
        Initializes binary object class.

        :param type_name: (optional) binary object name. Defaults to class
         name,
        :param schema: (optional) a dict of field names: field types,
        :param version: (optional) binary object version number. Defaults to 1,
        :raise: ParseError if one or more binary field types
         did not recognized.
        """
        cls._type_name = type_name or cls.__name__
        cls._type_id = entity_id(cls._type_name)
        schema = schema or {}
        cls._validate_schema(schema)
        cls._schema = schema
        cls._version = version
        super().__init__(name, base_classes, namespace)

# {
#     'type_exists': True,
#     'is_enum': False,
#     'schema': {
#         -444625788: [-1180648841, -1146447612, -1146457406, 2058196079],
#         1709620731: [-1180648841, -1146447612, -1146457406, 255216804],
#         1270090500: [-1180648841, -1146447612, -1146457406]
#     },
#     'type_name': 'TestBinaryType',
#     'binary_fields': [
#         {'type_id': 8, 'field_id': -1180648841, 'field_name': 'TEST_BOOL'},
#         {'type_id': 9, 'field_id': -1146447612, 'field_name': 'TEST_STR'},
#         {'type_id': 3, 'field_id': -1146457406, 'field_name': 'TEST_INT'},
#         {'type_id': 30, 'field_id': 255216804, 'field_name': 'TEST_DECIMAL'},
#         {'type_id': 5, 'field_id': 2058196079, 'field_name': 'TEST_FLOAT'}
#     ],
#     'type_id': 708045005,
#     'affinity_key_field': None
# }
