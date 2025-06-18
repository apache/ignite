

import get from 'lodash/get';
import omit from 'lodash/fp/omit';
import uuidv4 from 'uuid/v4';

import {CacheModes, AtomicityModes, ShortCache} from '../types';
import {Menu} from 'app/types';

const JDBC_LINKS = {
    Oracle: 'https://docs.oracle.com/javase/tutorial/jdbc/basics/connecting.html',
    Dremio: 'https://www.dremio.com/platform/sql-query-engine/'
}

export default class Caches {
    static $inject = ['$http'];

    cacheModes: Menu<CacheModes> = [       
        {value: 'REPLICATED', label: 'REPLICATED'},
        {value: 'PARTITIONED', label: 'PARTITIONED'}
    ];

    atomicityModes: Menu<AtomicityModes> = [
        {value: 'ATOMIC', label: 'ATOMIC'},
        {value: 'TRANSACTIONAL', label: 'TRANSACTIONAL'}
    ];

    constructor(private $http: ng.IHttpService) {}

    getCache(cacheID: string) {
        return this.$http.get(`/api/v1/configuration/caches/${cacheID}`);
    }

    getBlankCache() {
        return {
            id: uuidv4(),
            evictionPolicy: {},
            cacheMode: 'PARTITIONED',
            atomicityMode: 'ATOMIC',
            readFromBackup: true,
            copyOnRead: true,
            cacheStoreFactory: {
                CacheJdbcBlobStoreFactory: {
                    connectVia: 'DataSource'
                },
                CacheHibernateBlobStoreFactory: {
                    hibernateProperties: []
                }
            },
            writeBehindCoalescing: true,
            nearConfiguration: {},
            sqlFunctionClasses: [],
            domains: [],
            eagerTtl: true
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
        {value: 'IGFS', label: 'IGFS'},
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

    offHeapMode = {
        _val(cache) {
            return (cache.offHeapMode === null || cache.offHeapMode === void 0) ? -1 : cache.offHeapMode;
        },
        onChange: (cache) => {
            const offHeapMode = this.offHeapMode._val(cache);
            switch (offHeapMode) {
                case 1:
                    return cache.offHeapMaxMemory = cache.offHeapMaxMemory > 0 ? cache.offHeapMaxMemory : null;
                case 0:
                case -1:
                    return cache.offHeapMaxMemory = cache.offHeapMode;
                default: break;
            }
        },
        required: (cache) => cache.memoryMode === 'OFFHEAP_TIERED',
        offheapDisabled: (cache) => !(cache.memoryMode === 'OFFHEAP_TIERED' && this.offHeapMode._val(cache) === -1),
        default: 'Disabled'
    };

    offHeapModes = [
        {value: -1, label: 'Disabled'},
        {value: 1, label: 'Limited'},
        {value: 0, label: 'Unlimited'}
    ];

    offHeapMaxMemory = {
        min: 1
    };

    memoryMode = {
        default: 'ONHEAP_TIERED',
        offheapAndDomains: (cache) => {
            return !(cache.memoryMode === 'OFFHEAP_VALUES' && cache.domains.length);
        }
    };

    evictionPolicy = {
        required: (cache) => {
            return (cache.memoryMode || this.memoryMode.default) === 'ONHEAP_TIERED'
                && cache.offHeapMaxMemory > 0
                && !cache.evictionPolicy.kind;
        },
        values: [
            {value: 'LRU', label: 'LRU'},
            {value: 'FIFO', label: 'FIFO'},
            {value: 'SORTED', label: 'Sorted'},
            {value: null, label: 'Not set'}
        ],
        kind: {
            default: 'Not set'
        },
        maxMemorySize: {
            min: (evictionPolicy) => {
                const policy = evictionPolicy[evictionPolicy.kind];

                if (!policy)
                    return true;

                const maxSize = policy.maxSize === null || policy.maxSize === void 0
                    ? this.evictionPolicy.maxSize.default
                    : policy.maxSize;

                return maxSize ? 0 : 1;
            },
            default: 0
        },
        maxSize: {
            min: (evictionPolicy) => {
                const policy = evictionPolicy[evictionPolicy.kind];

                if (!policy)
                    return true;

                const maxMemorySize = policy.maxMemorySize === null || policy.maxMemorySize === void 0
                    ? this.evictionPolicy.maxMemorySize.default
                    : policy.maxMemorySize;

                return maxMemorySize ? 0 : 1;
            },
            default: 100000
        }
    };

    cacheStoreFactory = {
        kind: {
            default: 'Not set'
        },        
        values: [
            {value: 'DocumentLoadOnlyStoreFactory', label: 'Local Document POJO loader only store factory'},
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

    jdbcDriverURL(storeFactory) {       
        return JDBC_LINKS[get(storeFactory, 'dialect')];
    }

    requiresProprietaryDrivers(storeFactory) {
        return !!this.jdbcDriverURL(storeFactory);
    }
}
