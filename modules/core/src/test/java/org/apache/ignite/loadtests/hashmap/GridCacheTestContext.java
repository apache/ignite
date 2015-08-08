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

package org.apache.ignite.loadtests.hashmap;

import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.datastructures.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.jta.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.query.continuous.*;
import org.apache.ignite.internal.processors.cache.store.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.plugin.*;
import org.apache.ignite.testframework.junits.*;

import java.util.*;

import static org.apache.ignite.testframework.junits.GridAbstractTest.*;

/**
 * Cache test context.
 */
public class GridCacheTestContext<K, V> extends GridCacheContext<K, V> {
    /**
     * @param ctx Context.
     * @throws Exception If failed.
     */
    @SuppressWarnings("NullableProblems")
    public GridCacheTestContext(GridTestKernalContext ctx) throws Exception {
        super(
            ctx,
            new GridCacheSharedContext<>(
                ctx,
                new IgniteTxManager(),
                new GridCacheVersionManager(),
                new GridCacheMvccManager(),
                new GridCacheDeploymentManager<K, V>(),
                new GridCachePartitionExchangeManager<K, V>(),
                new GridCacheIoManager(),
                new CacheNoopJtaManager(),
                null
            ),
            defaultCacheConfiguration(),
            CacheType.USER,
            true,
            true,
            new GridCacheEventManager(),
            new GridCacheSwapManager(false),
            new CacheOsStoreManager(null, new CacheConfiguration()),
            new GridCacheEvictionManager(),
            new GridCacheLocalQueryManager<K, V>(),
            new CacheContinuousQueryManager(),
            new GridCacheAffinityManager(),
            new CacheDataStructuresManager(),
            new GridCacheTtlManager(),
            new GridOsCacheDrManager(),
            new CacheOsConflictResolutionManager<K, V>(),
            new CachePluginManager(ctx, new CacheConfiguration())
        );

        store().initialize(null, new IdentityHashMap<CacheStore, ThreadLocal>());
    }
}
