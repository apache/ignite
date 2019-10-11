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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplier;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

/**
 */
public class CacheRebalanceThreadPoolTest extends GridCommonAbstractTest {
    /** */
    private static final int REBALANCE_POOL_SIZE = 4;

    /** */
    private static final int PARTS_CNT = 32;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean persistenceEnabled;

    /** */
    private boolean delayDemandMsg = false;

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String CACHE3 = "cache3";

    /** */
    private static final String CACHE4 = "cache4";

    /** */
    public static final int CACHES_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setConsistentId(gridName);

        cfg.setActiveOnStart(false);

        TcpDiscoverySpi discoverySpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoverySpi.setIpFinder(ipFinder);

        cfg.setRebalanceThreadPoolSize(REBALANCE_POOL_SIZE);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        if (delayDemandMsg) {
            spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionDemandMessage &&
                blockCacheId(((GridDhtPartitionDemandMessage)msg).groupId()));

            delayDemandMsg = false;
        }

        cfg.setCommunicationSpi(spi);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE1)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)),
            new CacheConfiguration(CACHE2)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setBackups(2)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)),
            new CacheConfiguration(CACHE3)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setBackups(3)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)),
            new CacheConfiguration(CACHE4)
                .setCacheMode(CacheMode.REPLICATED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT)));

        long sz = 100 * 1024 * 1024;

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setPageSize(1024)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(persistenceEnabled).setInitialSize(sz).setMaxSize(sz))
            .setWalSegmentSize(8 * 1024 * 1024)
            .setWalHistorySize(1000)
            .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(24L * 60 * 60 * 1000);

        cfg.setDataStorageConfiguration(memCfg);

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

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testResourceUtilization_Volatile_ManyPartitions() throws Exception {
        doTestResourceUtilization(false, false, false, 20_000, false, false);
    }

    /** */
    @Test
    public void testResourceUtilization_Volatile_SinglePartition() throws Exception {
        doTestResourceUtilization(false, false, true, 20_000, false, false);
    }

    /** */
    @Test
    public void testResourceUtilization_Persistent_ManyPartitions() throws Exception {
        doTestResourceUtilization(true, false, false, 20_000, false, false);
    }

    /** */
    @Test
    public void testResourceUtilization_Persistent_SinglePartition() throws Exception {
        doTestResourceUtilization(true, false, true, 20_000, false, false);
    }

    /** */
    @Test
    public void testResourceUtilization_Historical_ManyPartitions() throws Exception {
        doTestResourceUtilization(true, true, false, 20_000, false, false);
    }

    /** */
    @Test
    public void testResourceUtilization_Historical_SinglePartition() throws Exception {
        doTestResourceUtilization(true, true, true, 20_000, false, false);
    }

    /** */
    @Test
    public void testUncaughtExceptionHandlingOnSupplier() throws Exception {
        doTestResourceUtilization(true, true, true, 100, true, false);
    }

    /** */
    @Test
    public void testUncaughtExceptionHandlingOnDemander() throws Exception {
        doTestResourceUtilization(true, true, true, 100, false, true);
    }

    /**
     */
    private void doTestResourceUtilization(boolean persistenceEnabled,
        boolean historical,
        boolean singlePart,
        int keys,
        boolean failSupplier,
        boolean failDemander
    ) throws Exception {
        if (historical)
            System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        try {
            this.persistenceEnabled = persistenceEnabled;

            IgniteEx ex = startGrids(1);

            ex.cluster().baselineAutoAdjustEnabled(false);
            ex.cluster().active(true);

            List<Integer> parts = movingKeysAfterJoin(ex, CACHE1, 1);

            startGrid(1);
            resetBaselineTopology();
            awaitPartitionMapExchange();

            // Non-empty partitions required for historical rebalance to start.
            if (persistenceEnabled) {
                for (int p = 0; p < PARTS_CNT; p++) {
                    ex.cache(CACHE1).put(p, p);
                    ex.cache(CACHE2).put(p, p);
                    ex.cache(CACHE3).put(p, p);
                    ex.cache(CACHE4).put(p, p);
                }

                forceCheckpoint();
            }

            stopGrid(1);

            ConcurrentSkipListSet<String> supplierThreads = new ConcurrentSkipListSet<>();

            for (int i = 1; i <= CACHES_CNT; i++) {
                GridDhtPreloader preloader = (GridDhtPreloader)ex.cachex(cacheName(i)).context().group().preloader();

                mockSupplier(preloader, new GridAbsClosure() {
                    @Override public void apply() {
                        if (failSupplier)
                            throw new Error();

                        supplierThreads.add(Thread.currentThread().getName());
                    }
                });
            }

            if (singlePart) {
                for (int i = 1; i <= CACHES_CNT; i++)
                    loadDataToPartition(parts.get(0), ex.name(), cacheName(i), keys, PARTS_CNT, 3);
            }
            else {
                for (int i = 1; i <= CACHES_CNT; i++) {
                    try (IgniteDataStreamer<Object, Object> streamer = ex.dataStreamer(cacheName(i))) {
                        for (int k = 0; k < keys; k++)
                            streamer.addData(k + PARTS_CNT, k + PARTS_CNT);
                    }
                }
            }

            ConcurrentSkipListSet<String> demanderThreads = new ConcurrentSkipListSet<>();

            delayDemandMsg = true;

            IgniteEx joining = startGrid(1);

            TestRecordingCommunicationSpi.spi(joining).waitForBlocked();

            for (int i = 1; i <= CACHES_CNT; i++) {
                GridDhtPreloader preloader = (GridDhtPreloader)joining.cachex(cacheName(i)).context().group().preloader();

                mockDemander(preloader, new GridAbsClosure() {
                    @Override public void apply() {
                        if (failDemander)
                            throw new Error();

                        demanderThreads.add(Thread.currentThread().getName());
                    }
                });
            }

            TestRecordingCommunicationSpi.spi(joining).stopBlock();

            if (failSupplier || failDemander) {
                // Wait until node is stopped by failure handler.
                waitForTopology(1);

                return;
            }

            // Test if rebalance was finished (all partitions are owned or other node failed).
            awaitPartitionMapExchange();

            // Tests partition consistency.
            for (int i = 1; i <= CACHES_CNT; i++)
                assertPartitionsSame(idleVerify(ex, cacheName(i)));

            // Test if rebalancing were done using expected thread pool.
            assertEquals(REBALANCE_POOL_SIZE, supplierThreads.size());
            assertEquals(REBALANCE_POOL_SIZE, demanderThreads.size());
            assertTrue(supplierThreads.stream().allMatch(s -> s.contains(ex.configuration().getIgniteInstanceName())));
            assertTrue(demanderThreads.stream().allMatch(s -> s.contains(joining.configuration().getIgniteInstanceName())));
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

            stopAllGrids();
        }
    }

    /**
     * @param preloader Preloader.
     * @param clo Closure to call before demand message processing.
     */
    private void mockSupplier(GridDhtPreloader preloader, GridAbsClosure clo) {
        GridDhtPartitionSupplier supplier = preloader.supplier();

        GridDhtPartitionSupplier mockedSupplier = Mockito.spy(supplier);

        Mockito.doAnswer(invocation -> {
            clo.run();

            invocation.callRealMethod();

            return null;
        }).when(mockedSupplier).handleDemandMessage(Mockito.anyInt(), Mockito.any(), Mockito.any());

        preloader.supplier(mockedSupplier);
    }

    /**
     * @param preloader Preloader.
     * @param clo Closure to call before supply message processing.
     */
    private void mockDemander(GridDhtPreloader preloader, GridAbsClosure clo) {
        GridDhtPartitionDemander demander = preloader.demander();

        GridDhtPartitionDemander mockedDemander = Mockito.spy(demander);

        Mockito.doAnswer(invocation -> {
            clo.run();

            invocation.callRealMethod();

            return null;
        }).when(mockedDemander).handleSupplyMessage(Mockito.any(), Mockito.any());

        preloader.demander(mockedDemander);
    }

    /**
     * @param idx Index.
     */
    private String cacheName(int idx) {
        return "cache" + idx;
    }

    /**
     * @param cacheId Group id.
     * @return {@code True} if message must be blocked.
     */
    private boolean blockCacheId(int cacheId) {
        for (int i = 1; i <= CACHES_CNT; i++) {
            if (cacheId == CU.cacheId(cacheName(i)))
                return true;
        }

        return false;
    }

}
