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
from inspect import isclass

import attr

from connection import Connection
from constants import *
from .type_codes import *
from .null import null_class


def is_ctype_type(some_class: type):
    return (
        isclass(some_class)
        and issubclass(some_class, (ctypes._SimpleCData, ctypes.Structure))
    )


class PString:
    """ Pascal-style string: `c_int` counter, followed by count*bytes. """

    @staticmethod
    def parse(conn: Connection):
        tc_type = conn.recv(ctypes.sizeof(ctypes.c_byte))
        # String or Null
        if tc_type == TC_NULL:
            return null_class(), tc_type

        buffer = tc_type + conn.recv(ctypes.sizeof(ctypes.c_int))
        length = int.from_bytes(buffer[1:], byteorder=PROTOCOL_BYTE_ORDER)

        data_type = type(
            'String',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                    ('data', ctypes.c_char * length),
                ],
            },
        )
        buffer += conn.recv(ctypes.sizeof(data_type) - len(buffer))

        return data_type, buffer

    @staticmethod
    def to_python(ctype_object):
        length = getattr(ctype_object, 'length', None)
        if length:
            return ctype_object.data.decode(PROTOCOL_STRING_ENCODING)
        else:
            return ''


@attr.s
class StructArray:
    """ `counter_type` counter, followed by count*following structure. """
    following = attr.ib(type=list)
    counter_type = attr.ib(default=ctypes.c_int)

    def parse(self, conn: Connection):
        buffer = conn.recv(ctypes.sizeof(self.counter_type))
        length = int.from_bytes(buffer, byteorder=PROTOCOL_BYTE_ORDER)
        fields = []

        for i in range(length):
            c_type, buffer_fragment = Struct(self.following).parse(conn)
            buffer += buffer_fragment
            fields.append(('element_{}'.format(i), c_type))

        data_class = type(
            'StructArray',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', self.counter_type),
                ] + fields,
            },
        )

        return data_class, buffer

    def to_python(self, ctype_object):
        result = []
        length = getattr(ctype_object, 'length', 0)
        for i in range(length):
            result.append(
                Struct(self.following).to_python(
                    getattr(ctype_object, 'element_{}'.format(i))
                )
            )
        return result


@attr.s
class Struct:
    """ Sequence of fields, including variable-sized and nested. """
    fields = attr.ib(type=list)

    def parse(self, conn: Connection):
        buffer = b''
        fields = []

        for name, c_type in self.fields:
            if is_ctype_type(c_type):
                buffer += conn.recv(ctypes.sizeof(c_type))
            else:
                c_type, buffer_fragment = c_type.parse(conn)
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

    def to_python(self, ctype_object):
        result = OrderedDict()
        for name, c_type in self.fields:
            if is_ctype_type(c_type):
                result[name] = getattr(ctype_object, name)
            else:
                result[name] = c_type.to_python(getattr(ctype_object, name))
        return result


cache_config_struct = Struct([
    ('length', ctypes.c_int),
    ('backups_number', ctypes.c_int),
    ('cache_mode', ctypes.c_int),
    ('mistery_parameter', ctypes.c_int),
    ('copy_on_read', ctypes.c_byte),

    ('data_region_name', PString),

    ('eager_ttl', ctypes.c_byte),
    ('statistics_enabled', ctypes.c_byte),

    ('group_name', PString),

    ('invalidate', ctypes.c_int),
    ('default_lock_timeout', ctypes.c_long),
    ('max_query_iterators', ctypes.c_int),

    ('name', PString),

    ('is_onheap_cache_enabled', ctypes.c_byte),
    ('partition_loss_policy', ctypes.c_int),
    ('query_detail_metric_size', ctypes.c_int),
    ('query_parallelism', ctypes.c_int),
    ('read_from_backup', ctypes.c_byte),
    ('rebalance_batch_size', ctypes.c_int),
    ('rebalance_batches_prefetch_count', ctypes.c_long),
    ('rebalance_delay', ctypes.c_long),
    ('rebalance_mode', ctypes.c_int),
    ('rebalance_order', ctypes.c_int),
    ('rebalance_throttle', ctypes.c_long),
    ('rebalance_timeout', ctypes.c_long),
    ('sql_escape_all', ctypes.c_byte),
    ('sql_index_inline_max_size', ctypes.c_int),

    ('sql_schema', PString),

    ('write_sync_mode', ctypes.c_int),

    ('cache_key_configuration', StructArray([
        ('name', PString),
        ('type_name', PString),
        ('is_key_field', ctypes.c_byte),
        ('is_notnull_constraint_field', ctypes.c_byte),
    ])),

    ('query_entity', StructArray([
        ('key_type_name', PString),
        ('value_type_name', PString),
        ('table_name', PString),
        ('key_field_name', PString),
        ('value_field_name', PString),

        ('query_fields', StructArray([
            ('name', PString),
            ('type_name', PString),
            ('is_key_field', ctypes.c_byte),
            ('is_notnull_constraint_field', ctypes.c_byte),
        ])),

        ('field_name_aliases', StructArray([
            ('field_name', PString),
            ('alias', PString),
        ])),

        ('query_indexes', StructArray([
            ('index_name', PString),
            ('index_type', ctypes.c_byte),
            ('inline_size', ctypes.c_int),
            ('fields', StructArray([
                ('name', PString),
                ('is_descending', ctypes.c_byte),
            ])),
        ])),
    ])),
])


class CacheConfigHeader:
    """ Enum fields. """
    CACHE_MODE_LOCAL = 0
    CACHE_MODE_REPLICATED = 1
    CACHE_MODE_PARTITIONED = 2

    PARTITION_LOSS_POLICY_READ_ONLY_SAFE = 0
    PARTITION_LOSS_POLICY_READ_ONLY_ALL = 1
    PARTITION_LOSS_POLICY_READ_WRITE_SAFE = 2
    PARTITION_LOSS_POLICY_READ_WRITE_ALL = 3
    PARTITION_LOSS_POLICY_IGNORE = 4

    REBALANCE_MODE_SYNC = 0
    REBALANCE_MODE_ASYNC = 1
    REBALANCE_MODE_NONE = 2

    WRITE_SYNC_MODE_FULL_SYNC = 0
    WRITE_SYNC_MODE_FULL_ASYNC = 1
    WRITE_SYNC_MODE_PRIMARY_SYNC = 2

    QUERY_INDEX_TYPE_SORTED = 0
    QUERY_INDEX_TYPE_FULLTEXT = 1
    QUERY_INDEX_TYPE_GEOSPATIAL = 2
