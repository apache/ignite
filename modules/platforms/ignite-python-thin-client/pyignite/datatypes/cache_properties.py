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
import math
from typing import Union

from . import ExpiryPolicy
from .prop_codes import *
from .cache_config import (
    CacheMode, CacheAtomicityMode, PartitionLossPolicy, RebalanceMode,
    WriteSynchronizationMode, QueryEntities, CacheKeyConfiguration,
)
from .primitive import *
from .standard import *

__all__ = [
    'PropName', 'PropCacheMode', 'PropCacheAtomicityMode', 'PropBackupsNumber',
    'PropWriteSynchronizationMode', 'PropCopyOnRead', 'PropReadFromBackup',
    'PropDataRegionName', 'PropIsOnheapCacheEnabled', 'PropQueryEntities',
    'PropQueryParallelism', 'PropQueryDetailMetricSize', 'PropSQLSchema',
    'PropSQLIndexInlineMaxSize', 'PropSqlEscapeAll', 'PropMaxQueryIterators',
    'PropRebalanceMode', 'PropRebalanceDelay', 'PropRebalanceTimeout',
    'PropRebalanceBatchSize', 'PropRebalanceBatchesPrefetchCount',
    'PropRebalanceOrder', 'PropRebalanceThrottle', 'PropGroupName',
    'PropCacheKeyConfiguration', 'PropDefaultLockTimeout',
    'PropMaxConcurrentAsyncOperation', 'PropPartitionLossPolicy',
    'PropEagerTTL', 'PropStatisticsEnabled', 'PropExpiryPolicy', 'prop_map', 'AnyProperty',
]


def prop_map(code: int):
    return {
        PROP_NAME: PropName,
        PROP_CACHE_MODE: PropCacheMode,
        PROP_CACHE_ATOMICITY_MODE: PropCacheAtomicityMode,
        PROP_BACKUPS_NUMBER: PropBackupsNumber,
        PROP_WRITE_SYNCHRONIZATION_MODE: PropWriteSynchronizationMode,
        PROP_COPY_ON_READ: PropCopyOnRead,
        PROP_READ_FROM_BACKUP: PropReadFromBackup,
        PROP_DATA_REGION_NAME: PropDataRegionName,
        PROP_IS_ONHEAP_CACHE_ENABLED: PropIsOnheapCacheEnabled,
        PROP_QUERY_ENTITIES: PropQueryEntities,
        PROP_QUERY_PARALLELISM: PropQueryParallelism,
        PROP_QUERY_DETAIL_METRIC_SIZE: PropQueryDetailMetricSize,
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
        PROP_PARTITION_LOSS_POLICY: PropPartitionLossPolicy,
        PROP_EAGER_TTL: PropEagerTTL,
        PROP_STATISTICS_ENABLED: PropStatisticsEnabled,
        PROP_EXPIRY_POLICY: PropExpiryPolicy,
    }[code]


class PropBase:
    prop_code = None
    prop_data_class = None

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__ + 'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('prop_code', ctypes.c_short),
                ],
            }
        )

    @classmethod
    def parse(cls, stream):
        init_pos = stream.tell()
        header_class = cls.build_header()
        data_class = cls.prop_data_class.parse(stream)

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

        stream.seek(init_pos + ctypes.sizeof(prop_class))
        return prop_class

    @classmethod
    async def parse_async(cls, stream):
        return cls.parse(stream)

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        return cls.prop_data_class.to_python(ctypes_object.data, **kwargs)

    @classmethod
    async def to_python_async(cls, ctypes_object, **kwargs):
        return cls.to_python(ctypes_object, **kwargs)

    @classmethod
    def from_python(cls, stream, value):
        header_class = cls.build_header()
        header = header_class()
        header.prop_code = cls.prop_code
        stream.write(bytes(header))
        cls.prop_data_class.from_python(stream, value)

    @classmethod
    async def from_python_async(cls, stream, value):
        return cls.from_python(stream, value)


class TimeoutProp(PropBase):
    prop_data_class = Long

    @classmethod
    def from_python(cls, stream, value: int):
        if not isinstance(value, int) or value < 0:
            raise ValueError(f'Timeout value should be a positive integer, {value} passed instead')
        return super().from_python(stream, value)

    @classmethod
    async def from_python_async(cls, stream, value):
        return cls.from_python(stream, value)


class PropName(PropBase):
    prop_code = PROP_NAME
    prop_data_class = String


class PropCacheMode(PropBase):
    prop_code = PROP_CACHE_MODE
    prop_data_class = CacheMode


class PropCacheAtomicityMode(PropBase):
    prop_code = PROP_CACHE_ATOMICITY_MODE
    prop_data_class = CacheAtomicityMode


class PropBackupsNumber(PropBase):
    prop_code = PROP_BACKUPS_NUMBER
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


class PropIsOnheapCacheEnabled(PropBase):
    prop_code = PROP_IS_ONHEAP_CACHE_ENABLED
    prop_data_class = Bool


class PropQueryEntities(PropBase):
    prop_code = PROP_QUERY_ENTITIES
    prop_data_class = QueryEntities


class PropQueryParallelism(PropBase):
    prop_code = PROP_QUERY_PARALLELISM
    prop_data_class = Int


class PropQueryDetailMetricSize(PropBase):
    prop_code = PROP_QUERY_DETAIL_METRIC_SIZE
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


class PropRebalanceTimeout(TimeoutProp):
    prop_code = PROP_REBALANCE_TIMEOUT


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


class PropDefaultLockTimeout(TimeoutProp):
    prop_code = PROP_DEFAULT_LOCK_TIMEOUT


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


class PropExpiryPolicy(PropBase):
    prop_code = PROP_EXPIRY_POLICY
    prop_data_class = ExpiryPolicy


class AnyProperty(PropBase):

    @classmethod
    def from_python(cls, stream, value):
        raise Exception(
            'You must choose a certain type '
            'for your cache configuration property'
        )

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        prop_data_class = prop_map(ctypes_object.prop_code)
        return prop_data_class.to_python(ctypes_object.data, **kwargs)
