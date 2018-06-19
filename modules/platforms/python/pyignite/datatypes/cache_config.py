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

import attr

from pyignite.connection import Connection
from pyignite.constants import *
from .standard import String
from .primitive import *


__all__ = [
    'Struct', 'StructArray', 'cache_config_struct', 'CacheMode',
    'PartitionLossPolicy', 'RebalanceMode', 'WriteSynchronizationMode',
    'IndexType',
]


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
            result[name] = c_type.to_python(getattr(ctype_object, name))
        return result


class CacheMode(Int):
    LOCAL = 0
    REPLICATED = 1
    PARTITIONED = 2


class PartitionLossPolicy(Int):
    READ_ONLY_SAFE = 0
    READ_ONLY_ALL = 1
    READ_WRITE_SAFE = 2
    READ_WRITE_ALL = 3
    IGNORE = 4


class RebalanceMode(Int):
    SYNC = 0
    ASYNC = 1
    NONE = 2


class WriteSynchronizationMode(Int):
    FULL_SYNC = 0
    FULL_ASYNC = 1
    PRIMARY_SYNC = 2


class IndexType(Byte):
    SORTED = 0
    FULLTEXT = 1
    GEOSPATIAL = 2


cache_config_struct = Struct([
    ('length', Int),
    ('backups_number', Int),
    ('cache_mode', CacheMode),
    ('mistery_parameter', Int),
    ('copy_on_read', Bool),

    ('data_region_name', String),

    ('eager_ttl', Bool),
    ('statistics_enabled', Bool),

    ('group_name', String),

    ('invalidate', Int),
    ('default_lock_timeout', Long),
    ('max_query_iterators', Int),

    ('name', String),

    ('is_onheap_cache_enabled', Bool),
    ('partition_loss_policy', PartitionLossPolicy),
    ('query_detail_metric_size', Int),
    ('query_parallelism', Int),
    ('read_from_backup', Bool),
    ('rebalance_batch_size', Int),
    ('rebalance_batches_prefetch_count', Long),
    ('rebalance_delay', Long),
    ('rebalance_mode', RebalanceMode),
    ('rebalance_order', Int),
    ('rebalance_throttle', Long),
    ('rebalance_timeout', Long),
    ('sql_escape_all', Bool),
    ('sql_index_inline_max_size', Int),

    ('sql_schema', String),

    ('write_synchronization_mode', WriteSynchronizationMode),

    ('cache_key_configuration', StructArray([
        ('name', String),
        ('type_name', String),
        ('is_key_field', Bool),
        ('is_notnull_constraint_field', Bool),
    ])),

    ('query_entity', StructArray([
        ('key_type_name', String),
        ('value_type_name', String),
        ('table_name', String),
        ('key_field_name', String),
        ('value_field_name', String),

        ('query_fields', StructArray([
            ('name', String),
            ('type_name', String),
            ('is_key_field', Bool),
            ('is_notnull_constraint_field', Bool),
        ])),

        ('field_name_aliases', StructArray([
            ('field_name', String),
            ('alias', String),
        ])),

        ('query_indexes', StructArray([
            ('index_name', String),
            ('index_type', IndexType),
            ('inline_size', Int),
            ('fields', StructArray([
                ('name', String),
                ('is_descending', Bool),
            ])),
        ])),
    ])),
])
