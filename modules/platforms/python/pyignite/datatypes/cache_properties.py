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

from pyignite.connection import Connection
from .prop_codes import *
from .cache_config import (
    CacheMode, CacheAtomicityMode, PartitionLossPolicy, RebalanceMode,
    WriteSynchronizationMode, QueryEntities, CacheKeyConfiguration,
)
from .primitive import *
from .standard import *


__all__ = [
    'PropName', 'PropCacheMode', 'PropCacheAtomicityMode', 'PropBackups',
    'PropWriteSynchronizationMode', 'PropCopyOnRead', 'PropReadFromBackup',
    'PropDataRegionName', 'PropIsOnheapcacheEnabled', 'PropQueryEntities',
    'PropQueryParallelism', 'PropQueryDetailMetricsSize', 'PropSQLSchema',
    'PropSQLIndexInlineMaxSize', 'PropSqlEscapeAll', 'PropMaxQueryIterators',
    'PropRebalanceMode', 'PropRebalanceDelay', 'PropRebalanceTimeout',
    'PropRebalanceBatchSize', 'PropRebalanceBatchesPrefetchCount',
    'PropRebalanceOrder', 'PropRebalanceThrottle', 'PropGroupName',
    'PropCacheKeyConfiguration', 'PropDefaultLockTimeout',
    'PropMaxConcurrentAsyncOperation', 'PropPartitionLossPolicy',
    'PropEagerTTL', 'PropStatisticsEnabled', 'prop_map', 'AnyProperty',
]


def prop_map(code: int):
    return {
        PROP_NAME: PropName,
        PROP_CACHE_MODE: PropCacheMode,
        PROP_CACHE_ATOMICITY_MODE: PropCacheAtomicityMode,
        PROP_BACKUPS: PropBackups,
        PROP_WRITE_SYNCHRONIZATION_MODE: PropWriteSynchronizationMode,
        PROP_COPY_ON_READ: PropCopyOnRead,
        PROP_READ_FROM_BACKUP: PropReadFromBackup,
        PROP_DATA_REGION_NAME: PropDataRegionName,
        PROP_IS_ONHEAPCACHE_ENABLED: PropIsOnheapcacheEnabled,
        PROP_QUERY_ENTITIES: PropQueryEntities,
        PROP_QUERY_PARALLELISM: PropQueryParallelism,
        PROP_QUERY_DETAIL_METRICS_SIZE: PropQueryDetailMetricsSize,
        PROP_SQL_SCHEMA: PropSQLSchema,
        PROP_SQL_INDEX_INLINE_MAX_SIZE: PropSQLIndexInlineMaxSize,
        PROP_SQL_ESCAPE_ALL: PropSqlEscapeAll,
        PROP_MAX_QUERY_ITERATORS: PropMaxQueryIterators,
        PROP_REBALANCE_MODE: PropRebalanceMode,
        PROP_REBALANCE_DELAY: PropRebalanceDelay,
        PROP_REBALANCE_TIMEOUT: PropRebalanceTimeout,
        PROP_REBALANCE_BATCH_SIZE: PropRebalanceBatchSize,
        PROP_REBALANCE_BATCHES_PREFETCH_COUNT: PropRebalanceBatchesPrefetchCount,
        PROP_REBALANCE_ORDER: PropRebalanceOrder,
        PROP_REBALANCE_THROTTLE: PropRebalanceThrottle,
        PROP_GROUP_NAME: PropGroupName,
        PROP_CACHE_KEY_CONFIGURATION: PropCacheKeyConfiguration,
        PROP_DEFAULT_LOCK_TIMEOUT: PropDefaultLockTimeout,
        PROP_MAX_CONCURRENT_ASYNC_OPERATIONS: PropMaxConcurrentAsyncOperation,
        PROP_PARTITION_LOSS_POLICY: PartitionLossPolicy,
        PROP_EAGER_TTL: PropEagerTTL,
        PROP_STATISTICS_ENABLED: PropStatisticsEnabled,
    }[code]


class PropBase:
    prop_code = None
    prop_data_class = None

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('prop_code', ctypes.c_short),
                ],
            }
        )

    @classmethod
    def parse(cls, conn: Connection):
        header_class = cls.build_header()
        header_buffer = conn.recv(ctypes.sizeof(header_class))
        data_class, data_buffer = cls.prop_data_class.parse(conn)
        prop_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('data', data_class),
                ],
            }
        )
        return prop_class, header_buffer + data_buffer

    @classmethod
    def to_python(cls, ctype_object):
        return cls.prop_data_class.to_python(ctype_object.data)

    @classmethod
    def from_python(cls, value):
        header_class = cls.build_header()
        header = header_class()
        header.prop_code = cls.prop_code
        return bytes(header) + cls.prop_data_class.from_python(value)


class PropName(PropBase):
    prop_code = PROP_NAME
    prop_data_class = String


class PropCacheMode(PropBase):
    prop_code = PROP_CACHE_MODE
    prop_data_class = CacheMode


class PropCacheAtomicityMode(PropBase):
    prop_code = PROP_CACHE_ATOMICITY_MODE
    prop_data_class = CacheAtomicityMode


class PropBackups(PropBase):
    prop_code = PROP_BACKUPS
    prop_data_class = Int


class PropWriteSynchronizationMode(PropBase):
    prop_code = PROP_WRITE_SYNCHRONIZATION_MODE
    prop_data_class = WriteSynchronizationMode


class PropCopyOnRead(PropBase):
    prop_code = PROP_COPY_ON_READ
    prop_data_class = Bool


class PropReadFromBackup(PropBase):
    prop_code = PROP_READ_FROM_BACKUP
    prop_data_class = Bool


class PropDataRegionName(PropBase):
    prop_code = PROP_DATA_REGION_NAME
    prop_data_class = String


class PropIsOnheapcacheEnabled(PropBase):
    prop_code = PROP_IS_ONHEAPCACHE_ENABLED
    prop_data_class = Bool


class PropQueryEntities(PropBase):
    prop_code = PROP_QUERY_ENTITIES
    prop_data_class = QueryEntities


class PropQueryParallelism(PropBase):
    prop_code = PROP_QUERY_PARALLELISM
    prop_data_class = Int


class PropQueryDetailMetricsSize(PropBase):
    prop_code = PROP_QUERY_DETAIL_METRICS_SIZE
    prop_data_class = Int


class PropSQLSchema(PropBase):
    prop_code = PROP_SQL_SCHEMA
    prop_data_class = String


class PropSQLIndexInlineMaxSize(PropBase):
    prop_code = PROP_SQL_INDEX_INLINE_MAX_SIZE
    prop_data_class = Int


class PropSqlEscapeAll(PropBase):
    prop_code = PROP_SQL_ESCAPE_ALL
    prop_data_class = Bool


class PropMaxQueryIterators(PropBase):
    prop_code = PROP_MAX_QUERY_ITERATORS
    prop_data_class = Int


class PropRebalanceMode(PropBase):
    prop_code = PROP_REBALANCE_MODE
    prop_data_class = RebalanceMode


class PropRebalanceDelay(PropBase):
    prop_code = PROP_REBALANCE_DELAY
    prop_data_class = Long


class PropRebalanceTimeout(PropBase):
    prop_code = PROP_REBALANCE_TIMEOUT
    prop_data_class = Long


class PropRebalanceBatchSize(PropBase):
    prop_code = PROP_REBALANCE_BATCH_SIZE
    prop_data_class = Int


class PropRebalanceBatchesPrefetchCount(PropBase):
    prop_code = PROP_REBALANCE_BATCHES_PREFETCH_COUNT
    prop_data_class = Long


class PropRebalanceOrder(PropBase):
    prop_code = PROP_REBALANCE_ORDER
    prop_data_class = Int


class PropRebalanceThrottle(PropBase):
    prop_code = PROP_REBALANCE_THROTTLE
    prop_data_class = Long


class PropGroupName(PropBase):
    prop_code = PROP_GROUP_NAME
    prop_data_class = String


class PropCacheKeyConfiguration(PropBase):
    prop_code = PROP_CACHE_KEY_CONFIGURATION
    prop_data_class = CacheKeyConfiguration


class PropDefaultLockTimeout(PropBase):
    prop_code = PROP_DEFAULT_LOCK_TIMEOUT
    prop_data_class = Long


class PropMaxConcurrentAsyncOperation(PropBase):
    prop_code = PROP_MAX_CONCURRENT_ASYNC_OPERATIONS
    prop_data_class = Int


class PropPartitionLossPolicy(PropBase):
    prop_code = PROP_PARTITION_LOSS_POLICY
    prop_data_class = PartitionLossPolicy


class PropEagerTTL(PropBase):
    prop_code = PROP_EAGER_TTL
    prop_data_class = Bool


class PropStatisticsEnabled(PropBase):
    prop_code = PROP_STATISTICS_ENABLED
    prop_data_class = Bool


class AnyProperty(PropBase):

    @classmethod
    def from_python(cls, value):
        raise Exception(
            'You must choose a certain type '
            'for your cache configuration property'
        )

    @classmethod
    def to_python(cls, ctype_object):
        prop_data_class = prop_map(ctype_object.prop_code)
        return prop_data_class.to_python(ctype_object.data)
