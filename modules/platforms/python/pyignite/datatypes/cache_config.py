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
            (self.build_header_class(),),
            {
                '_pack_': 1,
                '_fields_': fields,
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

    def from_python(self, value):
        length = len(value)
        header_class = self.build_header_class()
        header = header_class()
        header.length = length
        buffer = bytes(header)

        for i, v in enumerate(value):
            for element in self.following:
                name, el_class = element
                buffer += el_class.from_python(v[name])

        return buffer


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

    def from_python(self, value):
        buffer = b''

        for name, el_class in self.fields:
            buffer += el_class.from_python(value[name])

        return buffer


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


class CacheAtomicityMode(Int):
    TRANSACTIONAL = 0
    ATOMIC = 1


QueryFields = StructArray([
    ('name', String),
    ('type_name', String),
    ('is_key_field', Bool),
    ('is_notnull_constraint_field', Bool),
])


FieldNameAliases = StructArray([
    ('field_name', String),
    ('alias', String),
])


Fields = StructArray([
    ('name', String),
    ('is_descending', Bool),
])


QueryIndexes = StructArray([
    ('index_name', String),
    ('index_type', IndexType),
    ('inline_size', Int),
    ('fields', Fields),
])


QueryEntities = StructArray([
    ('key_type_name', String),
    ('value_type_name', String),
    ('table_name', String),
    ('key_field_name', String),
    ('value_field_name', String),
    ('query_fields', QueryFields),
    ('field_name_aliases', FieldNameAliases),
    ('query_indexes', QueryIndexes),
])


CacheKeyConfiguration = StructArray([
    ('name', String),
    ('type_name', String),
    ('is_key_field', Bool),
    ('is_notnull_constraint_field', Bool),
])


cache_config_struct = Struct([
    ('length', Int),
    ('backups_number', Int),
    ('cache_mode', CacheMode),
    ('cache_atomicity_mode', CacheAtomicityMode),
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
    ('cache_key_configuration', CacheKeyConfiguration),
    ('query_entities', QueryEntities),
])
