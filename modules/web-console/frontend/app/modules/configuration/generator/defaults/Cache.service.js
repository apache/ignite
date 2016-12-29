/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const DFLT_CACHE = {
    cacheMode: {
        clsName: 'org.apache.ignite.cache.CacheMode'
    },
    atomicityMode: {
        clsName: 'org.apache.ignite.cache.CacheAtomicityMode'
    },
    memoryMode: {
        clsName: 'org.apache.ignite.cache.CacheMemoryMode',
        value: 'ONHEAP_TIERED'
    },
    offHeapMaxMemory: -1,
    startSize: 1500000,
    swapEnabled: false,
    sqlOnheapRowCacheSize: 10240,
    longQueryWarningTimeout: 3000,
    snapshotableIndex: false,
    sqlEscapeAll: false,
    storeKeepBinary: false,
    loadPreviousValue: false,
    cacheStoreFactory: {
        CacheJdbcPojoStoreFactory: {
            batchSize: 512,
            maximumWriteAttempts: 2,
            parallelLoadCacheMinimumThreshold: 512,
            sqlEscapeAll: false
        }
    },
    readThrough: false,
    writeThrough: false,
    writeBehindEnabled: false,
    writeBehindBatchSize: 512,
    writeBehindFlushSize: 10240,
    writeBehindFlushFrequency: 5000,
    writeBehindFlushThreadCount: 1,
    maxConcurrentAsyncOperations: 500,
    defaultLockTimeout: 0,
    atomicWriteOrderMode: {
        clsName: 'org.apache.ignite.cache.CacheAtomicWriteOrderMode'
    },
    writeSynchronizationMode: {
        clsName: 'org.apache.ignite.cache.CacheWriteSynchronizationMode',
        value: 'PRIMARY_SYNC'
    },
    rebalanceMode: {
        clsName: 'org.apache.ignite.cache.CacheRebalanceMode',
        value: 'ASYNC'
    },
    rebalanceThreadPoolSize: 1,
    rebalanceBatchSize: 524288,
    rebalanceBatchesPrefetchCount: 2,
    rebalanceOrder: 0,
    rebalanceDelay: 0,
    rebalanceTimeout: 10000,
    rebalanceThrottle: 0,
    statisticsEnabled: false,
    managementEnabled: false,
    nearConfiguration: {
        nearStartSize: 375000
    },
    clientNearConfiguration: {
        nearStartSize: 375000
    },
    evictionPolicy: {
        LRU: {
            batchSize: 1,
            maxSize: 100000
        },
        FIFO: {
            batchSize: 1,
            maxSize: 100000
        },
        SORTED: {
            batchSize: 1,
            maxSize: 100000
        }
    },
    queryMetadata: 'Configuration',
    fields: {
        keyClsName: 'java.lang.String',
        valClsName: 'java.lang.String',
        valField: 'className',
        entries: []
    },
    aliases: {
        keyClsName: 'java.lang.String',
        valClsName: 'java.lang.String',
        keyField: 'field',
        valField: 'alias',
        entries: []
    },
    indexes: {
        indexType: {
            clsName: 'org.apache.ignite.cache.QueryIndexType'
        },
        fields: {
            keyClsName: 'java.lang.String',
            valClsName: 'java.lang.Boolean',
            valField: 'direction',
            entries: []
        }
    },
    typeField: {
        databaseFieldType: {
            clsName: 'java.sql.Types'
        }
    }
};

export default class IgniteCacheDefaults {
    constructor() {
        Object.assign(this, DFLT_CACHE);
    }
}
