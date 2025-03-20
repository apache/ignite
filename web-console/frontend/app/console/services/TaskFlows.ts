

import get from 'lodash/get';
import omit from 'lodash/fp/omit';
import uuidv4 from 'uuid/v4';

import {CacheModes, AtomicityModes, ShortCache} from 'app/configuration/types';
import {Menu} from 'app/types';

export default class TaskFlows {
    static $inject = ['$http'];

    cacheModes: Menu<CacheModes> = [       
        {value: 'REPLICATED', label: 'REPLICATED'},
        {value: 'PARTITIONED', label: 'PARTITIONED'}
    ];

    atomicityModes: Menu<AtomicityModes> = [
        {value: 'ATOMIC', label: 'ATOMIC'},
        {value: 'TRANSACTIONAL', label: 'TRANSACTIONAL'}       
    ];
    
    updateModes: Menu<String> = [
        {value: 'SKIP_EXISTING', label: 'Do not update existing nodes'},
        {value: 'REPLACE_EXISTING', label: 'Replace existing nodes'},
        {value: 'UPDATE_EXISTING', label: 'Update existing nodes'},
        {value: 'MERGE_EXISTING', label: 'Only merge existing nodes'}
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
            existingMode: 'REPLACE_EXISTING',
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
    
    getTaskFlows(clusterID: string,targetCache: string,sourceCache: string) {
        return this.$http.get(`/api/v1/taskflow/group/${clusterID}?source=`+sourceCache+'&target='+targetCache);
    }

    getTaskFlowsOfSource(clusterID: string,cacheID: string) {
        return this.$http.get(`/api/v1/taskflow/group/*?source=`+cacheID+'&sourceCluster='+clusterID);
    }
    
    getTaskFlowsOfTarget(clusterID: string,cacheID: string) {
        return this.$http.get(`/api/v1/taskflow/cluster/${clusterID}?target=`+cacheID);
    }
    
    removeTaskFlowOfGroup(clusterID: string) {
        return this.$http.delete(`/api/v1/taskflow/group/${clusterID}`);
    }
    
    removeTaskFlow(clusterID: string, id: string) {
        return this.$http.delete(`/api/v1/taskflow/${id}`);
    }

    saveBasic(changedItems) {
        return this.$http.put('/api/v1/taskflow', changedItems, {responseType:'text'});
    }

    saveAdvanced(changedItems) {
        return this.$http.put('/api/v1/taskflow/advance/', changedItems, {responseType:'text'});
    }
}
