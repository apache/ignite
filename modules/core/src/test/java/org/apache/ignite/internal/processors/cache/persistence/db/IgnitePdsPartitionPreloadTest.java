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
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.G;
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
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().
                    setMetricsEnabled(true).
                    setMaxSize(50L * 1024 * 1024).
                    setPersistenceEnabled(true).
                    setName(DEFAULT_REGION))
            .setWalMode(WALMode.LOG_ONLY)
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
    public void testPreloadPartitionTransactionalClientSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> {
            try {
                return startGrid(CLIENT_GRID_NAME);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, false);
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
        }, true);
    }

    /** */
    public void testPreloadPartitionTransactionalNodeFilteredSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> grid(0), false);
    }

    /** */
    public void testPreloadPartitionTransactionalNodeFilteredAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> grid(0), true);
    }

    /** */
    public void testPreloadPartitionTransactionalPrimarySync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), false);
    }

    /** */
    public void testPreloadPartitionTransactionalPrimaryAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), true);
    }

    /** */
    public void testPreloadPartitionTransactionalBackupSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> G.allGrids().stream().filter(BackupNodePredicate.INSTANCE).findFirst().get(), false);
    }

    /** */
    public void testPreloadPartitionTransactionalBackupAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL);

        preloadPartition(() -> G.allGrids().stream().filter(BackupNodePredicate.INSTANCE).findFirst().get(), true);
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
        }, false);
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
        }, true);
    }

    /** */
    public void testPreloadPartitionAtomicNodeFilteredSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> grid(0), false);
    }

    /** */
    public void testPreloadPartitionAtomicNodeFilteredAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> grid(0), true);
    }

    /** */
    public void testPreloadPartitionAtomicPrimarySync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), false);
    }

    /** */
    public void testPreloadPartitionAtomicPrimaryAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), true);
    }

    /** */
    public void testPreloadPartitionAtomicBackupSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> G.allGrids().stream().filter(BackupNodePredicate.INSTANCE).findFirst().get(), false);
    }

    /** */
    public void testPreloadPartitionAtomicBackupAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(ATOMIC);

        preloadPartition(() -> G.allGrids().stream().filter(BackupNodePredicate.INSTANCE).findFirst().get(), true);
    }

    /** */
    public void testPreloadLocalTransactionalSync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL).setCacheMode(LOCAL);

        preloadPartition(() -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), false);
    }

    /** */
    public void testPreloadLocalTransactionalAsync() throws Exception {
        cfgFactory = () -> cacheConfiguration(TRANSACTIONAL).setCacheMode(LOCAL);

        preloadPartition(() -> G.allGrids().stream().filter(PrimaryNodePredicate.INSTANCE).findFirst().get(), true);
    }

    /**
     * @param testNodeFactory Test node factory.
     * @param async {@code True} for async preload.
     */
    private void preloadPartition(Supplier<Ignite> testNodeFactory, boolean async) throws Exception {
        Ignite crd = startGridsMultiThreaded(GRIDS_CNT);

        int cnt = 0;

        Ignite primary = grid(1);

        assertEquals(PRIMARY_NODE, primary.cluster().localNode().consistentId());

        Integer key = primaryKey(primary.cache(DEFAULT_CACHE_NAME));

        int preloadPart = crd.affinity(DEFAULT_CACHE_NAME).partition(key);

        try (IgniteDataStreamer<Integer, Integer> streamer = primary.dataStreamer(DEFAULT_CACHE_NAME)) {
            int k = 0;

            while (cnt < ENTRY_CNT) {
                if (primary.affinity(DEFAULT_CACHE_NAME).partition(k) == preloadPart) {
                    streamer.addData(k, k);

                    cnt++;
                }

                k++;
            }
        }

        forceCheckpoint();

        stopAllGrids();

        startGridsMultiThreaded(GRIDS_CNT);

        primary = G.allGrids().stream().
            filter(ignite -> PRIMARY_NODE.equals(ignite.cluster().localNode().consistentId())).findFirst().get();

        Ignite testNode = testNodeFactory.get();

        if (async)
            testNode.cache(DEFAULT_CACHE_NAME).preloadPartitionAsync(preloadPart).get();
        else
            testNode.cache(DEFAULT_CACHE_NAME).preloadPartition(preloadPart);

        long c0 = primary.dataRegionMetrics(DEFAULT_REGION).getPagesRead();

        // After partition preloading no pages should be read from store.
        testNode.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>()).getAll();

        assertEquals("Read pages count must be same", c0, primary.dataRegionMetrics(DEFAULT_REGION).getPagesRead());
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
}
