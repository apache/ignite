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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.TestDependencyResolver;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Tests that {@link CacheRebalanceMode#SYNC} caches are evicted at first.
 */
public class PartitionEvictionOrderTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        CacheConfiguration<Long, Long> atomicCcfg = new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(atomicCcfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that {@link CacheRebalanceMode#SYNC} caches are evicted at first.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_EVICTION_PERMITS, value = "1")
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "500_000")
    public void testSyncCachesEvictedAtFirst() throws Exception {
        IgniteEx node0 = startGrid(0);

        node0.cluster().state(ACTIVE);

        IgniteEx node1 = startGrid(1);

        node0.cluster().setBaselineTopology(node1.cluster().topologyVersion());

        GridCacheAdapter<Object, Object> utilCache0 = grid(0).context().cache().internalCache(CU.UTILITY_CACHE_NAME);

        IgniteCache<Object, Object> cache = node0.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1000; i++) {
            utilCache0.put(i, i);

            cache.put(i, i);
        }

        awaitPartitionMapExchange();

        stopGrid(0);

        GridCacheAdapter<Object, Object> utilCache1 = grid(1).context().cache().internalCache(CU.UTILITY_CACHE_NAME);

        IgniteInternalCache<Object, Object> cache2 = grid(1).context().cache().cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 2000; i++) {
            try {
                cache2.put(i, i + 1);

                utilCache1.put(i, i + 1);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        }

        List<T2<Integer, Integer>> evictionOrder = Collections.synchronizedList(new ArrayList<>());

        TestDependencyResolver rslvr = new TestDependencyResolver(new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridDhtPartitionTopologyImpl) {
                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)instance;

                    top.partitionFactory((ctx, grp, id, recovery) -> new GridDhtLocalPartition(ctx, grp, id, recovery) {
                        @Override public long clearAll(EvictionContext evictionCtx) throws NodeStoppingException {
                            evictionOrder.add(new T2<>(grp.groupId(), id));

                            return super.clearAll(evictionCtx);
                        }
                    });
                }

                return instance;
            }
        });

        startGrid(0, rslvr);

        awaitPartitionMapExchange(true, true, null);

        assertEquals(utilCache0.affinity().partitions() + grid(0).cachex(DEFAULT_CACHE_NAME).affinity().partitions(),
            evictionOrder.size());

        for (int i = 0; i < utilCache0.affinity().partitions(); i++)
            assertEquals(CU.UTILITY_CACHE_GROUP_ID, evictionOrder.get(i).get1().intValue());
    }
}
