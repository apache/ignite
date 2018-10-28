/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Test partition preload for varios cache modes.
 */
public class IgnitePdsPartitionPreloadTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Test entry count. */
    public static final int ENTRY_CNT = 500;

    /** Grid count. */
    private static final int GRIDS_CNT = 3;

    /** */
    private static final String CLIENT_GRID_NAME = "client";

    /** */
    public static final String DEFAULT_REGION = "default";

    /** */
    private Supplier<CacheConfiguration> cfgFactory;

    /** */
    private static final String TEST_ATTR = "testId";

    /** */
    private static final String NO_CACHE_NODE = "node0";

    /** */
    private static final String PRIMARY_NODE = "node1";

    /** */
    private static final String BACKUP_NODE = "node2";

    /** */
    public static final String MEM = "mem";

    /** */
    public static final int MB = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(CLIENT_GRID_NAME.equals(gridName));

        if (!cfg.isClientMode()) {
            String val = "node" + getTestIgniteInstanceIndex(gridName);
            cfg.setUserAttributes(Collections.singletonMap(TEST_ATTR, val));
            cfg.setConsistentId(val);
        }

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDataRegionConfigurations(new DataRegionConfiguration().setName(MEM).setInitialSize(10 * MB))
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().
                    setMetricsEnabled(true).
                    setMaxSize(50L * MB).
                    setPersistenceEnabled(true).
                    setName(DEFAULT_REGION))
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(16 * MB)
            .setPageSize(1024)
            .setMetricsEnabled(true);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setCacheConfiguration(cfgFactory.get());

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /**
     * @param atomicityMode Atomicity mode.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setBackups(1);
        ccfg.setNodeFilter(new TestIgnitePredicate());
        ccfg.setAtomicityMode(atomicityMode);

        return ccfg;
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

    /** */
    public void testLocalPreloadPartitionClient() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL).setDataRegionName(MEM);

        startGridsMultiThreaded(GRIDS_CNT);

        IgniteEx client = startGrid("client");

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        assertFalse(client.cache(DEFAULT_CACHE_NAME).localPreloadPartition(0));
        assertFalse(grid(0).cache(DEFAULT_CACHE_NAME).localPreloadPartition(0));
    }

    /** */
    public void testLocalPreloadPartitionPrimary() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(
            () -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), PreloadMode.LOCAL);
    }

    /** */
    public void testLocalPreloadPartitionBackup() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(
            () -> G.allGrids().stream().filter(BackupNodePredicate.INSTANCE).findFirst().get(), PreloadMode.LOCAL);
    }

    /** */
    public void testPreloadPartitionInMemoryRemote() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL).setDataRegionName(MEM);

        startGridsMultiThreaded(GRIDS_CNT);

        IgniteEx client = startGrid("client");

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        try {
            client.cache(DEFAULT_CACHE_NAME).preloadPartition(0);

            fail("Exception is expected");
        }
        catch (Exception e) {
            log.error("Expected", e);
        }
    }

    /** */
    public void testPreloadPartitionInMemoryLocal() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL).setDataRegionName(MEM);

        startGridsMultiThreaded(GRIDS_CNT);

        int key = 0;

        Ignite prim = primaryNode(key, DEFAULT_CACHE_NAME);

        int part = prim.affinity(DEFAULT_CACHE_NAME).partition(key);

        try {
            prim.cache(DEFAULT_CACHE_NAME).preloadPartition(part);

            fail("Exception is expected");
        }
        catch (Exception e) {
            log.error("Expected", e);
        }
    }

    /** */
    public void testPreloadPartitionTransactionalClientSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> {
            try {
                return startGrid(CLIENT_GRID_NAME);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, PreloadMode.SYNC);
    }

    /** */
    public void testPreloadPartitionTransactionalClientAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> {
            try {
                return startGrid(CLIENT_GRID_NAME);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, PreloadMode.ASYNC);
    }

    /** */
    public void testPreloadPartitionTransactionalNodeFilteredSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> grid(0), PreloadMode.SYNC);
    }

    /** */
    public void testPreloadPartitionTransactionalNodeFilteredAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> grid(0), PreloadMode.ASYNC);
    }

    /** */
    public void testPreloadPartitionTransactionalPrimarySync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(
            () -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), PreloadMode.SYNC);
    }

    /** */
    public void testPreloadPartitionTransactionalPrimaryAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(
            () -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), PreloadMode.ASYNC);
    }

    /** */
    public void testPreloadPartitionTransactionalBackupSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(
            () -> G.allGrids().stream().filter(BackupNodePredicate.INSTANCE).findFirst().get(), PreloadMode.SYNC);
    }

    /** */
    public void testPreloadPartitionTransactionalBackupAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(
            () -> G.allGrids().stream().filter(BackupNodePredicate.INSTANCE).findFirst().get(), PreloadMode.ASYNC);
    }

    /** */
    public void testPreloadPartitionAtomicClientSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> {
            try {
                return startGrid(CLIENT_GRID_NAME);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, PreloadMode.SYNC);
    }

    /** */
    public void testPreloadPartitionAtomicClientAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> {
            try {
                return startGrid(CLIENT_GRID_NAME);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, PreloadMode.ASYNC);
    }

    /** */
    public void testPreloadPartitionAtomicNodeFilteredSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> grid(0), PreloadMode.SYNC);
    }

    /** */
    public void testPreloadPartitionAtomicNodeFilteredAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> grid(0), PreloadMode.ASYNC);
    }

    /** */
    public void testPreloadPartitionAtomicPrimarySync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(
            () -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), PreloadMode.SYNC);
    }

    /** */
    public void testPreloadPartitionAtomicPrimaryAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(
            () -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), PreloadMode.ASYNC);
    }

    /** */
    public void testPreloadPartitionAtomicBackupSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(
            () -> G.allGrids().stream().filter(BackupNodePredicate.INSTANCE).findFirst().get(), PreloadMode.SYNC);
    }

    /** */
    public void testPreloadPartitionAtomicBackupAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(
            () -> G.allGrids().stream().filter(BackupNodePredicate.INSTANCE).findFirst().get(), PreloadMode.ASYNC);
    }

    /** */
    public void testPreloadLocalTransactionalSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL).setCacheMode(LOCAL);

        preloadPartition(
            () -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), PreloadMode.SYNC);
    }

    /** */
    public void testPreloadLocalTransactionalAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL).setCacheMode(LOCAL);

        preloadPartition(
            () -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), PreloadMode.ASYNC);
    }

    /**
     * @param execNodeFactory Test node factory.
     * @param preloadMode Preload mode.
     */
    private void preloadPartition(Supplier<Ignite> execNodeFactory, PreloadMode preloadMode) throws Exception {
        Ignite crd = startGridsMultiThreaded(GRIDS_CNT);

        Ignite testNode = grid(1);

        Object consistentId = testNode.cluster().localNode().consistentId();

        assertEquals(PRIMARY_NODE, testNode.cluster().localNode().consistentId());

        boolean locCacheMode = testNode.cache(DEFAULT_CACHE_NAME).getConfiguration(CacheConfiguration.class).getCacheMode() == LOCAL;

        Integer key = primaryKey(testNode.cache(DEFAULT_CACHE_NAME));

        int preloadPart = crd.affinity(DEFAULT_CACHE_NAME).partition(key);

        int cnt = 0;

        try (IgniteDataStreamer<Integer, Integer> streamer = testNode.dataStreamer(DEFAULT_CACHE_NAME)) {
            int k = 0;

            while (cnt < ENTRY_CNT) {
                if (testNode.affinity(DEFAULT_CACHE_NAME).partition(k) == preloadPart) {
                    streamer.addData(k, k);

                    cnt++;
                }

                k++;
            }
        }

        forceCheckpoint();

        stopAllGrids();

        startGridsMultiThreaded(GRIDS_CNT);

        testNode = G.allGrids().stream().
            filter(ignite -> PRIMARY_NODE.equals(ignite.cluster().localNode().consistentId())).findFirst().get();

        if (!locCacheMode)
            assertEquals(testNode, primaryNode(key, DEFAULT_CACHE_NAME));

        Ignite execNode = execNodeFactory.get();

        switch (preloadMode) {
            case SYNC:
                execNode.cache(DEFAULT_CACHE_NAME).preloadPartition(preloadPart);

                if (locCacheMode) {
                    testNode = G.allGrids().stream().filter(ignite ->
                        ignite.cluster().localNode().consistentId().equals(consistentId)).findFirst().get();
                }

                break;
            case ASYNC:
                execNode.cache(DEFAULT_CACHE_NAME).preloadPartitionAsync(preloadPart).get();

                if (locCacheMode) {
                    testNode = G.allGrids().stream().filter(ignite ->
                        ignite.cluster().localNode().consistentId().equals(consistentId)).findFirst().get();
                }

                break;
            case LOCAL:
                assertTrue(execNode.cache(DEFAULT_CACHE_NAME).localPreloadPartition(preloadPart));

                testNode = execNode; // For local preloading testNode == execNode

                break;
        }

        long c0 = testNode.dataRegionMetrics(DEFAULT_REGION).getPagesRead();

        // After partition preloading no pages should be read from store.
        List<Cache.Entry<Object, Object>> list = U.arrayList(testNode.cache(DEFAULT_CACHE_NAME).localEntries(), 1000);

        assertEquals(ENTRY_CNT, list.size());

        assertEquals("Read pages count must be same", c0, testNode.dataRegionMetrics(DEFAULT_REGION).getPagesRead());
    }

    /** */
    private static class TestIgnitePredicate implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !NO_CACHE_NODE.equals(node.attribute(TEST_ATTR));
        }
    }

    /** */
    private static class PrimaryNodePredicate implements Predicate<Ignite> {
        /** */
        private static final PrimaryNodePredicate INSTANCE = new PrimaryNodePredicate();

        /** {@inheritDoc} */
        @Override public boolean test(Ignite ignite) {
            return PRIMARY_NODE.equals(ignite.cluster().localNode().consistentId());
        }
    }

    /** */
    private static class BackupNodePredicate implements Predicate<Ignite> {
        /** */
        private static final BackupNodePredicate INSTANCE = new BackupNodePredicate();

        /** {@inheritDoc} */
        @Override public boolean test(Ignite ignite) {
            return BACKUP_NODE.equals(ignite.cluster().localNode().consistentId());
        }
    }

    /** */
    private enum PreloadMode {
        /** Sync. */ SYNC,
        /** Async. */ASYNC,
        /** Local. */LOCAL;
    }
}
