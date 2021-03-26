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
