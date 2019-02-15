/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

const DFLT_CACHE = {
    cacheMode: {
        clsName: 'org.apache.ignite.cache.CacheMode'
    },
    partitionLossPolicy: {
        clsName: 'org.apache.ignite.cache.PartitionLossPolicy',
        value: 'IGNORE'
    },
    atomicityMode: {
        clsName: 'org.apache.ignite.cache.CacheAtomicityMode'
    },
    memoryMode: {
        clsName: 'org.apache.ignite.cache.CacheMemoryMode',
        value: 'ONHEAP_TIERED'
    },
    onheapCacheEnabled: false,
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
    writeBehindCoalescing: true,
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
        batchSize: 1,
        maxSize: 100000
    },
    queryMetadata: 'Configuration',
    queryDetailMetricsSize: 0,
    queryParallelism: 1,
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
    },
    memoryPolicyName: 'default'
};

export default class IgniteCacheDefaults {
    constructor() {
        Object.assign(this, DFLT_CACHE);
    }
}
