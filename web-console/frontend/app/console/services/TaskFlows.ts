/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import get from 'lodash/get';
import omit from 'lodash/fp/omit';
import uuidv4 from 'uuid/v4';

import {CacheModes, AtomicityModes, ShortCache} from 'app/configuration/types';
import {Menu} from 'app/types';

export default class TaskFlows {
    static $inject = ['$http'];

    cacheModes: Menu<CacheModes> = [
        {value: 'LOCAL', label: 'LOCAL'},
        {value: 'REPLICATED', label: 'REPLICATED'},
        {value: 'PARTITIONED', label: 'PARTITIONED'}
    ];

    atomicityModes: Menu<AtomicityModes> = [
        {value: 'ATOMIC', label: 'ATOMIC'},
        {value: 'TRANSACTIONAL', label: 'TRANSACTIONAL'},
        {value: 'TRANSACTIONAL_SNAPSHOT', label: 'TRANSACTIONAL_SNAPSHOT'}
    ];

    constructor(private $http: ng.IHttpService) {}

    getBlankTaskFlow() {
        return {
            id: uuidv4(),
            name: '',
            sourceCluster: null,
            source: null,
            targetCluster: null,
            target: null,
            group: null,
            cacheMode: 'PARTITIONED',
            atomicityMode: 'ATOMIC',
            readFromBackup: true,
            copyOnRead: true,
            
            writeBehindCoalescing: true        
            
        };
    }

    toShortCache(cache: any): ShortCache {
        return {
            id: cache.id,
            name: cache.name,
            backups: cache.backups,
            cacheMode: cache.cacheMode,
            atomicityMode: cache.atomicityMode
        };
    }

    normalize = omit(['__v', 'space', 'clusters']);

    nodeFilterKinds = [
        {value: 'Custom', label: 'Custom'},
        {value: null, label: 'Not set'}
    ];

    memoryModes = [
        {value: 'ONHEAP_TIERED', label: 'ONHEAP_TIERED'},
        {value: 'OFFHEAP_TIERED', label: 'OFFHEAP_TIERED'},
        {value: 'OFFHEAP_VALUES', label: 'OFFHEAP_VALUES'}
    ];

    diskPageCompression = [
        {value: 'SKIP_GARBAGE', label: 'SKIP_GARBAGE'},
        {value: 'ZSTD', label: 'ZSTD'},
        {value: 'LZ4', label: 'LZ4'},
        {value: 'SNAPPY', label: 'SNAPPY'},
        {value: null, label: 'Disabled'}
    ];

    taskFlowStoreFactory = {
        kind: {
            default: 'Not set'
        },
        values: [
            {value: 'CacheJdbcPojoStoreFactory', label: 'JDBC POJO store factory'},
            {value: 'CacheJdbcBlobStoreFactory', label: 'JDBC BLOB store factory'},
            {value: 'CacheHibernateBlobStoreFactory', label: 'Hibernate BLOB store factory'},
            {value: null, label: 'Not set'}
        ],
        
        storeDisabledValueOff: (cache, value) => {
            return cache && cache.cacheStoreFactory.kind ? true : !value;
        },
        storeEnabledReadOrWriteOn: (cache) => {
            return cache && cache.cacheStoreFactory.kind ? (cache.readThrough || cache.writeThrough) : true;
        }
    };

    writeBehindFlush = {
        min: (cache) => {
            return cache.writeBehindFlushSize === 0 && cache.writeBehindFlushFrequency === 0
                ? 1
                : 0;
        }
    };

    getCacheBackupsCount(cache: ShortCache) {
        return this.shouldShowCacheBackupsCount(cache)
            ? (cache.backups || 0)
            : void 0;
    }

    shouldShowCacheBackupsCount(cache: ShortCache) {
        return cache && cache.cacheMode === 'PARTITIONED';
    }


    getTaskFlow(flowID: string) {
        return this.$http.get(`/api/v1/taskflow/${flowID}`)
            .then((data) => {
                return data;
            });
    }

    getTaskFlowOfGroup(clusterID: string) {
        return this.$http.get(`/api/v1/taskflow/group/${clusterID}`);
    }   

    getTaskFlowsOfSource(clusterID: string,cacheID: string) {
        return this.$http.get(`/api/v1/taskflow/group/${clusterID}?source=`+cacheID);
    }
    
    removeTaskFlowOfGroup(clusterID: string) {
        return this.$http.delete('/api/v1/taskflow/group/${clusterID}');
    }
    
    removeTaskFlow(clusterID: string,id: string) {
        return this.$http.delete('/api/v1/taskflow/${id}');
    }

    saveBasic(changedItems) {
        return this.$http.put('/api/v1/taskflow', changedItems);
    }

    saveAdvanced(changedItems) {
        return this.$http.put('/api/v1/taskflow/advanced/', changedItems);
    }
}
