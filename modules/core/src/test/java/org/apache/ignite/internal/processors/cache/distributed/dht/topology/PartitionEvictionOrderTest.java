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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests that {@link CacheRebalanceMode#SYNC} caches are evicted at first.
 *
 * Note that this test is based on logging, for more correct version check the same class in actual master branch.
 * Correct version could be merged here only when https://ggsystems.atlassian.net/browse/GG-17431 has been backported.
 */
public class PartitionEvictionOrderTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Condition check. */
    static volatile boolean condCheck;

    /** */
    static AtomicBoolean check = new AtomicBoolean(false);

    /** */
    static ReentrantLock lock = new ReentrantLock();

    /** */
    static List<Integer> evictionOrder = Collections.synchronizedList(new ArrayList<>());

    /** */
    static int sysCachePartNum;

    /** */
    static int asyncCachePartNum;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        CacheConfiguration<Long, Long> atomicCcfg = new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setCacheMode(REPLICATED);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(atomicCcfg);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that {@link CacheRebalanceMode#SYNC} caches are evicted at first.
     */
    public void testSyncCachesEvictedAtFirst() throws Exception {
        withSystemProperty(IgniteSystemProperties.IGNITE_EVICTION_PERMITS, "1");

        withSystemProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "500_000");

        withSystemProperty("SHOW_EVICTION_PROGRESS_FREQ", "0");

        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);

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

        sysCachePartNum = utilCache1.affinity().partitions();

        asyncCachePartNum = cache2.affinity().partitions();

        TestProvider.enabled = true;

        startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        assertTrue(condCheck);

        for (int i : evictionOrder)
            assertEquals(CU.UTILITY_CACHE_GROUP_ID, i);
    }

    /**
     * Plugin that changes
     */
    public static class TestProvider implements PluginProvider, IgnitePlugin {
        /** */
        private GridKernalContext igniteCtx;

        /** */
        public static volatile boolean enabled;

        /** {@inheritDoc} */
        @Override public String name() {
            return "PartitionEvictionOrderTestPlugin";
        }

        /** {@inheritDoc} */
        @Override public String version() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public String copyright() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            igniteCtx = ((IgniteKernal)ctx.grid()).context();
        }

        /** {@inheritDoc} */
        @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void start(PluginContext pluginContext) throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public void stop(boolean cancel) throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStart() throws IgniteCheckedException {
        }

        /** {@inheritDoc} */
        @Override public void onIgniteStop(boolean cancel) {

        }

        /** {@inheritDoc} */
        @Override public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {

        }

        /** {@inheritDoc} */
        @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {

        }

        /** {@inheritDoc} */
        @Nullable @Override public Object createComponent(PluginContext pluginContext, Class cls) {
            if (enabled && CacheConflictResolutionManager.class.equals(cls)) {
                igniteCtx.cache().cacheGroups().forEach(g -> ((GridDhtPartitionTopologyImpl)g.topology()).partitionFactory(
                    (ctx, grp, id, recovery) -> new GridDhtLocalPartition(ctx, grp, id, recovery) {
                        @Override public boolean tryClear(EvictionContext evictionCtx) throws NodeStoppingException {
                            if (!check.get()) {
                                lock.lock();

                                try {
                                    if (!check.get()) {
                                        PriorityBlockingQueue<PartitionsEvictManager.PartitionEvictionTask> queue =
                                            (PriorityBlockingQueue<PartitionsEvictManager.PartitionEvictionTask>)
                                                ctx.cache().context().evict().evictionQueue.buckets[0];

                                        // Expected size of partition eviction queue. -1 because the first partition
                                        // has already been removed from queue.
                                        int expEvictQueueSize = sysCachePartNum +
                                            asyncCachePartNum - 1;

                                        condCheck = GridTestUtils.waitForCondition(() ->
                                            queue.size() == expEvictQueueSize, 100_000);

                                        if (!condCheck) {
                                            check.set(true);

                                            return super.tryClear(evictionCtx);
                                        }

                                        PartitionsEvictManager.PartitionEvictionTask[] tasks =
                                            new PartitionsEvictManager.PartitionEvictionTask[queue.size()];

                                        queue.toArray(tasks);

                                        Arrays.sort(tasks, queue.comparator());

                                        //-1 because the first partition that we evict might be from sys cache.
                                        // We don't have invariant for partition eviction order for cache types.
                                        for (int i = 0; i < sysCachePartNum - 1; i++) {
                                            GridDhtLocalPartition part;

                                            part = U.field(tasks[i], "part");

                                            evictionOrder.add(part.group().groupId());
                                        }

                                        check.set(true);
                                    }
                                }
                                catch (IgniteInterruptedCheckedException e) {
                                    e.printStackTrace();
                                }
                                finally {
                                    lock.unlock();
                                }
                            }

                            return super.tryClear(evictionCtx);
                        }
                    }));
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public IgnitePlugin plugin() {
            return this;
        }
    }
}
