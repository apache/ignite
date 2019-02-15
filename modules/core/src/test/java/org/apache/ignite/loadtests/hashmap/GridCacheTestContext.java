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

package org.apache.ignite.loadtests.hashmap;

import java.util.IdentityHashMap;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinitySharedManager;
import org.apache.ignite.internal.processors.cache.CacheOsConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeploymentManager;
import org.apache.ignite.internal.processors.cache.GridCacheEventManager;
import org.apache.ignite.internal.processors.cache.GridCacheEvictionManager;
import org.apache.ignite.internal.processors.cache.GridCacheIoManager;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedTtlCleanupManager;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManager;
import org.apache.ignite.internal.processors.cache.WalStateManager;
import org.apache.ignite.internal.processors.cache.datastructures.CacheDataStructuresManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionsEvictManager;
import org.apache.ignite.internal.processors.cache.dr.GridOsCacheDrManager;
import org.apache.ignite.internal.processors.cache.jta.CacheNoopJtaManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheLocalQueryManager;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.processors.cache.store.CacheOsStoreManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.plugin.CachePluginManager;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.GridTestKernalContext;

import static org.apache.ignite.testframework.junits.GridAbstractTest.defaultCacheConfiguration;

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
                null,
                null,
                new WalStateManager(null),
                new IgniteCacheDatabaseSharedManager(),
                new IgniteCacheSnapshotManager(),
                new GridCacheDeploymentManager<K, V>(),
                new GridCachePartitionExchangeManager<K, V>(),
                new CacheAffinitySharedManager<K, V>(),
                new GridCacheIoManager(),
                new GridCacheSharedTtlCleanupManager(),
                new PartitionsEvictManager(),
                new CacheNoopJtaManager(),
                null,
                null,
                null
            ),
            defaultCacheConfiguration(),
            null,
            CacheType.USER,
            AffinityTopologyVersion.ZERO,
            IgniteUuid.randomUuid(),
            true,
            true,
            false,
            false,
            new GridCacheEventManager(),
            new CacheOsStoreManager(null, new CacheConfiguration()),
            new GridCacheEvictionManager(),
            new GridCacheLocalQueryManager<K, V>(),
            new CacheContinuousQueryManager(),
            new CacheDataStructuresManager(),
            new GridCacheTtlManager(),
            new GridOsCacheDrManager(),
            new CacheOsConflictResolutionManager<K, V>(),
            new CachePluginManager(ctx, new CacheConfiguration()),
            new GridCacheAffinityManager()
        );

        store().initialize(null, new IdentityHashMap<CacheStore, ThreadLocal>());
    }
}
