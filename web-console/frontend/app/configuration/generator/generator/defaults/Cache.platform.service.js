

import _ from 'lodash';

const enumValueMapper = (val) => _.capitalize(val);

const DFLT_CACHE = {
    cacheMode: {
        clsName: 'Apache.Ignite.Core.Cache.Configuration.CacheMode',
        mapper: enumValueMapper
    },
    atomicityMode: {
        clsName: 'Apache.Ignite.Core.Cache.Configuration.CacheAtomicityMode',
        mapper: enumValueMapper
    },
    memoryMode: {
        clsName: 'Apache.Ignite.Core.Cache.Configuration.CacheMemoryMode',
        value: 'ONHEAP_TIERED',
        mapper: enumValueMapper
    },
    atomicWriteOrderMode: {
        clsName: 'org.apache.ignite.cache.CacheAtomicWriteOrderMode',
        mapper: enumValueMapper
    },
    writeSynchronizationMode: {
        clsName: 'org.apache.ignite.cache.CacheWriteSynchronizationMode',
        value: 'PRIMARY_SYNC',
        mapper: enumValueMapper
    },
    rebalanceMode: {
        clsName: 'org.apache.ignite.cache.CacheRebalanceMode',
        value: 'ASYNC',
        mapper: enumValueMapper
    }
};

export default class IgniteCachePlatformDefaults {
    constructor() {
        Object.assign(this, DFLT_CACHE);
    }
}
