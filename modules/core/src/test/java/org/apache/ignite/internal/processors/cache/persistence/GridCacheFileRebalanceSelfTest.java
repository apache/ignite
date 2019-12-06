/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridPartitionFilePreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_FILE_REBALANCE_THRESHOLD;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
@RunWith(Parameterized.class)
public class GridCacheFileRebalanceSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int TEST_SIZE = GridTestUtils.SF.applyLB(100_000, 10_000);

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String DYNAMIC_CACHE = "dynamic-cache";

    @Parameterized.Parameter
    public CacheAtomicityMode cacheAtomicityMode;

    private CacheMode cacheMode = REPLICATED;

    private int parts = 16;

    private int backups = 0;

    private CacheWriteSynchronizationMode cacheWriteSyncMode = CacheWriteSynchronizationMode.PRIMARY_SYNC;

    /** */
    @Before
    public void setup() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** Parameters. */
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        List<Object[]> params = new ArrayList<>(2);

        params.add(new CacheAtomicityMode[] {CacheAtomicityMode.TRANSACTIONAL});
//        params.add(new CacheAtomicityMode[] {CacheAtomicityMode.ATOMIC});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 600_000;
    }

    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return 60_000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setInitialSize(10 * 1024 * 1024L)
                    .setMaxSize(4 * 1024L * 1024 * 1024)
                    .setPersistenceEnabled(true))
                .setDataRegionConfigurations(new DataRegionConfiguration()
                    .setInitialSize(10 * 1024 * 1024L)
                    .setMaxSize(4 * 1024 * 1024 * 1024L)
                    .setPersistenceEnabled(true)
                    .setName("someRegion"))
                .setWalMode(WALMode.LOG_ONLY)
                .setCheckpointFrequency(3_000)) // todo check with default timeout!
//                .setWalSegmentSize(4 * 1024 * 1024)
//                .setMaxWalArchiveSize(32 * 1024 * 1024 * 1024L))
            .setCacheConfiguration(cacheConfig(DEFAULT_CACHE_NAME).setDataRegionName("someRegion"), cacheConfig(CACHE1), cacheConfig(CACHE2));

        cfg.setSystemThreadPoolSize(56);

        cfg.setConsistentId(igniteInstanceName);
            //.setCacheConfiguration(cacheConfig(CACHE1));

//        if (getTestIgniteInstanceIndex(igniteInstanceName) == 2)
//            cfg.setGridLogger(new NullLogger());

        return cfg;
    }

    private CacheConfiguration cacheConfig(String name) {
        return new CacheConfiguration(name).setCacheMode(cacheMode)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setAtomicityMode(cacheAtomicityMode)
            .setWriteSynchronizationMode(cacheWriteSyncMode)
            //.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
//                .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(backups);
//            .setCommunicationSpi(new TestRecordingCommunicationSpi()
    }

//    @Test
//    public void testEvictReadOnlyPartition() {
//
//    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    public void testReadRemovePartitionEviction() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);
        ignite0.cluster().baselineAutoAdjustTimeout(0);

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        IgniteInternalCache<Object, Object> cache = ignite0.cachex(DEFAULT_CACHE_NAME);

        assert cache.size() == TEST_SIZE;

        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

//        int hash = DEFAULT_CACHE_NAME.hashCode();

        for (long i = 0; i < TEST_SIZE; i++) {
            assertTrue("key=" + i, cache.containsKey(i));

            assertEquals("key=" + i, generateValue(i, DEFAULT_CACHE_NAME), cache.localPeek(i, peekAll));
        }

        List<GridDhtLocalPartition> locParts = cache.context().topology().localPartitions();

        CountDownLatch allPartsCleared = new CountDownLatch(locParts.size());

        ignite0.context().cache().context().database().checkpointReadLock();

        CacheGroupContext grp = cache.context().group();

        System.out.println("Clearing partitions");

        try {
            for (GridDhtLocalPartition part : locParts) {
                part.moving();

                part.dataStore().readOnly(true);

                part.clearAsync();

                part.onClearFinished(f -> {

                    try {
                        PageMemoryEx memEx = (PageMemoryEx)grp.dataRegion().pageMemory();

                        int tag = memEx.invalidate(grp.groupId(), part.id());

                        ((FilePageStoreManager)ignite0.context().cache().context().pageStore()).getStore(grp.groupId(), part.id()).truncate(tag);
                        //PageMemoryEx memEx = (PageMemoryEx)region.pageMemory();

//                        memEx.clearAsync(
//                            (grp0, pageId) -> grp0 == grp.groupId() && part.id() == PageIdUtils.partId(pageId), true).get();

                        allPartsCleared.countDown();
                    } catch (IgniteCheckedException e) {
                        e.printStackTrace();
                    }
                    }
                );
            }
        } finally {
            ignite0.context().cache().context().database().checkpointReadUnlock();
        }

        System.out.println("Running standart partition eviction");

        for (GridDhtLocalPartition part : locParts) {
            part.dataStore().readOnly(false);

            part.clearAsync();
        }

        U.sleep(15_000);

//        allPartsCleared.await(20_000, TimeUnit.MILLISECONDS);
//
//        // Ensure twice that all entries evicted.
//        for (int i = 0; i < TEST_SIZE; i++)
//            assertNull(cache.localPeek(i, peekAll));
//
//        ignite0.context().cache().context().database().checkpointReadLock();
//
//        try {
//            for (GridDhtLocalPartition part : locParts) {
//                part.dataStore().readOnly(false);
//
//                part.own();
//            }
//        } finally {
//            ignite0.context().cache().context().database().checkpointReadUnlock();
//        }
//
//        for (int i = 0; i < TEST_SIZE; i++)
//            assertNull(cache.localPeek(i, peekAll));
//
//        cache.put(TEST_SIZE, TEST_SIZE);
//
//        assertEquals(TEST_SIZE, cache.get(TEST_SIZE));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    public void testBase() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);
        ignite0.cluster().baselineAutoAdjustTimeout(0);

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        forceCheckpoint();

        IgniteEx ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        verifyLocalCache(ignite0.cachex(DEFAULT_CACHE_NAME), ignite1.cachex(DEFAULT_CACHE_NAME));
    }


    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testBaseActivation() throws Exception {
        Ignite ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        loadData(ignite0, DEFAULT_CACHE_NAME, 100_000);

        startGridsMultiThreaded(1, 3);

        log.info("wait for activation");
        U.sleep(180_000);

        ignite0.cluster().setBaselineTopology(F.viewReadOnly(G.allGrids(), g -> g.cluster().localNode()));

//        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);
//
//        forceCheckpoint();
//
//        IgniteEx ignite1 = startGrid(1);
//
//        awaitPartitionMapExchange();
//
//        verifyLocalCache(ignite0.cachex(DEFAULT_CACHE_NAME), ignite1.cachex(DEFAULT_CACHE_NAME));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testUnderConstantLoad() throws Exception {
        cacheWriteSyncMode = FULL_SYNC;
        cacheMode = REPLICATED;
        backups = 0;

        boolean removes = false;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);
        ignite0.cluster().baselineAutoAdjustTimeout(0);

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        AtomicLong cntr = new AtomicLong(TEST_SIZE);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(DEFAULT_CACHE_NAME), cntr, removes, 8);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, 2, "thread");

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        awaitPartitionMapExchange(true, true, null, true);

        ldr.stop();

        ldrFut.get();

        U.sleep(1_000);

        verifyLocalCache(ignite0.cachex(DEFAULT_CACHE_NAME), ignite1.cachex(DEFAULT_CACHE_NAME));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testUnderConstantLoadPartitioned3nodes() throws Exception {
        cacheMode = PARTITIONED;
        backups = 0;

        List<ClusterNode> blt = new ArrayList<>();

        boolean removes = false;
        int threads = 1;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.cluster().localNode());

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        AtomicLong cntr = new AtomicLong(TEST_SIZE);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(DEFAULT_CACHE_NAME), cntr, removes, threads);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "thread");

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.cluster().localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange(true, true, null, true);

        ldr.pause();

        U.sleep(1_000);

        for (int i = 0; i < 2; i++) {
            IgniteInternalCache cache = grid(i).cachex(DEFAULT_CACHE_NAME);

            System.out.println("\nPartittions on " + grid(i).cluster().localNode().id());

            for (GridDhtLocalPartition part : cache.context().topology().currentLocalPartitions())
                System.out.println(part.id() + " state=" + part.state() + " size=" + part.fullSize());

            System.out.println();
        }

        verifyCacheContent(ignite0.cache(DEFAULT_CACHE_NAME), cntr.get(), removes);

        ldr.resume();

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.cluster().localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange(true, true, null, true);

        ldr.stop();

        ldrFut.get();

        U.sleep(1_000);

        for (int i = 0; i < 3; i++) {
            IgniteInternalCache cache = grid(i).cachex(DEFAULT_CACHE_NAME);

            System.out.println("\nPartittions on " + grid(i).cluster().localNode().id());

            for (GridDhtLocalPartition part : cache.context().topology().currentLocalPartitions())
                System.out.println(part.id() + " state=" + part.state() + " size=" + part.fullSize());

            System.out.println();
        }

        verifyCacheContent(ignite0.cache(DEFAULT_CACHE_NAME), cntr.get(), removes);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void checkEvictionOfReadonlyPartition() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);
        ignite0.cluster().baselineAutoAdjustTimeout(0);

        loadData(ignite0, CACHE1, TEST_SIZE);
        loadData(ignite0, CACHE2, TEST_SIZE);
//        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        IgniteInternalCache<Object, Object> cache1 = ignite1.cachex(CACHE1);
        IgniteInternalCache<Object, Object> cache2 = ignite1.cachex(CACHE2);

        AtomicInteger partsCntr = new AtomicInteger();

        CountDownLatch latch = new CountDownLatch(1);

        for (GridDhtLocalPartition part : cache1.context().topology().currentLocalPartitions()) {
            partsCntr.incrementAndGet();

            part.moving();

            part.readOnly(true);
        }

        for (GridDhtLocalPartition part : cache2.context().topology().currentLocalPartitions()) {
            partsCntr.incrementAndGet();

            part.moving();

            part.readOnly(true);
        }

        for (GridDhtLocalPartition part : cache1.context().topology().currentLocalPartitions()) {
            part.clearAsync();

            part.onClearFinished(c -> {
                int remain = partsCntr.decrementAndGet();

                log.info("Remain: " + remain);

                if (remain == 0)
                    latch.countDown();
            });
        }

        for (GridDhtLocalPartition part : cache2.context().topology().currentLocalPartitions()) {
            part.clearAsync();

            part.onClearFinished(c -> {
                int remain = partsCntr.decrementAndGet();

                log.info("Remain: " + remain);

                if (remain == 0)
                    latch.countDown();
            });
        }

        boolean success = latch.await(30, TimeUnit.SECONDS);

        assertTrue(success);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    public void testMultipleCaches() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);
        ignite0.cluster().baselineAutoAdjustTimeout(0);

        loadData(ignite0, CACHE1, TEST_SIZE);
        loadData(ignite0, CACHE2, TEST_SIZE);

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        U.sleep(1_000);

        awaitPartitionMapExchange();

        U.sleep(2_000);

        verifyLocalCache(ignite0.cachex(CACHE1), ignite1.cachex(CACHE1));
        verifyLocalCache(ignite0.cachex(CACHE2), ignite1.cachex(CACHE2));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesThreeNodesSequence() throws Exception {
        List<ClusterNode> blt = new ArrayList<>();

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        loadData(ignite0, CACHE1, TEST_SIZE);
        loadData(ignite0, CACHE2, TEST_SIZE);

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        U.sleep(2_000);

        verifyLocalCache(ignite0.cachex(CACHE1), ignite1.cachex(CACHE1));
        verifyLocalCache(ignite0.cachex(CACHE2), ignite1.cachex(CACHE2));

        verifyLocalCache(ignite0.cachex(CACHE1), ignite2.cachex(CACHE1));
        verifyLocalCache(ignite0.cachex(CACHE2), ignite2.cachex(CACHE2));
    }

    /** Check partitions moving with file rebalancing. */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesMultipleNodesSequencePartitioned() throws Exception {
        cacheMode = PARTITIONED;
        parts = 128;
        backups = 0;

        int nodesCnt = 5;

        List<ClusterNode> blt = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++) {
            IgniteEx ignite = startGrid(i);

            blt.add(ignite.localNode());

            if (i == 0) {
                ignite.cluster().active(true);

                loadData(ignite, CACHE1, TEST_SIZE);
                loadData(ignite, CACHE2, TEST_SIZE);
            }
            else
                ignite.cluster().setBaselineTopology(blt);

            awaitPartitionMapExchange();

            IgniteCache<Object, Object> cache1 = ignite.cache(CACHE1);
            IgniteCache<Object, Object> cache2 = ignite.cache(CACHE2);

            // todo should check partitions
            for (int k = 0; k < TEST_SIZE; k++) {
                assertEquals(generateValue(k, CACHE1), cache1.get(k));
                assertEquals(generateValue(k, CACHE2), cache2.get(k));
            }
        }
    }

    /** Check partitions moving with file rebalancing. */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesMultipleNodesStartStopStableTopologyPartitionedNoCoordinatorChange() throws Exception {
        cacheMode = PARTITIONED;
        parts = 128;
        backups = 1;

        int nodesCnt = 4;

        List<ClusterNode> blt = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++) {
            IgniteEx ignite = startGrid(i);

            blt.add(ignite.localNode());

            if (i == 0) {
                ignite.cluster().active(true);

                loadData(ignite, CACHE1, TEST_SIZE);
                loadData(ignite, CACHE2, TEST_SIZE);
            }
            else {
                ignite.cluster().setBaselineTopology(blt);

                awaitPartitionMapExchange();
            }
        }

        int maxNodeIdx = nodesCnt - 1;

        verifyCacheContent(grid(maxNodeIdx).cache(CACHE1), TEST_SIZE);
        verifyCacheContent(grid(maxNodeIdx).cache(CACHE2), TEST_SIZE);

        Ignite crd = grid(0);

        for (int i = maxNodeIdx; i > 0; i--) {
            IgniteEx stopNode = grid(i);

            blt.remove(stopNode.localNode());

            stopGrid(i);

            crd.cluster().setBaselineTopology(blt);

            awaitPartitionMapExchange();
        }

        verifyCacheContent(crd.cache(CACHE1), TEST_SIZE);
        verifyCacheContent(crd.cache(CACHE2), TEST_SIZE);
    }

    /** Check partitions moving with file rebalancing. */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesMultipleNodesStartStopStableTopologyPartitionedNoCoordinatorChangeWithConstantLoad() throws Exception {
        cacheMode = PARTITIONED;
        cacheWriteSyncMode = FULL_SYNC;
        parts = 16;
        backups = 1;

        int nodesCnt = 4;
        int threads = Runtime.getRuntime().availableProcessors();

        IgniteInternalFuture ldrFut = null;

        ConstantLoader ldr = null;

        AtomicLong cntr = new AtomicLong(TEST_SIZE);

        List<ClusterNode> blt = new ArrayList<>();

        for (int i = 0; i < nodesCnt; i++) {
            IgniteEx ignite = startGrid(i);

            blt.add(ignite.localNode());

            if (i == 0) {
                ignite.cluster().active(true);

                loadData(ignite, CACHE1, TEST_SIZE);
                loadData(ignite, CACHE2, TEST_SIZE);

                ldr = new ConstantLoader(ignite.cache(CACHE1), cntr, false, threads);

                ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "thread");
            }
            else {
                ignite.cluster().setBaselineTopology(blt);

                awaitPartitionMapExchange();
            }
        }

        int maxNodeIdx = nodesCnt - 1;

        ldr.pause();

        U.sleep(3_000);

        verifyCacheContent(grid(maxNodeIdx).cache(CACHE2), TEST_SIZE);
        verifyCacheContent(grid(maxNodeIdx).cache(CACHE1), cntr.get());

        ldr.resume();

        Ignite crd = grid(0);

        for (int i = maxNodeIdx; i > 0; i--) {
            IgniteEx stopNode = grid(i);

            blt.remove(stopNode.localNode());

            stopGrid(i);

            crd.cluster().setBaselineTopology(blt);

            awaitPartitionMapExchange();
        }

        ldr.stop();

        ldrFut.get();

        long size = cntr.get();

        U.sleep(3_000);

        verifyCacheContent(crd.cache(CACHE2), TEST_SIZE);
        verifyCacheContent(crd.cache(CACHE1), size);

    }


    /** Check partitions moving with file rebalancing. */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesMultipleNodesSequencePartitionedWithConstantLoad() throws Exception {
        cacheMode = PARTITIONED;
        parts = 128;
        backups = 0;
        cacheWriteSyncMode = PRIMARY_SYNC;

        int grids = 5;
        int threads = Runtime.getRuntime().availableProcessors();

        List<ClusterNode> blt = new ArrayList<>();

        IgniteInternalFuture ldrFut = null;

        ConstantLoader ldr = null;

        AtomicLong cntr = new AtomicLong(TEST_SIZE);

        for (int i = 0; i < grids; i++) {
            IgniteEx ignite = startGrid(i);

            blt.add(ignite.localNode());

            if (i == 0) {
                ignite.cluster().active(true);

                loadData(ignite, CACHE1, TEST_SIZE);
                loadData(ignite, CACHE2, TEST_SIZE);

                ldr = new ConstantLoader(ignite.cache(CACHE1), cntr, false, threads);

                ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "thread");
            }
            else
                ignite.cluster().setBaselineTopology(blt);

            awaitPartitionMapExchange(true, true, null, true);
        }

        ldr.stop();

        ldrFut.get();

        U.sleep(10_000);

        verifyCacheContent(grid(0).cache(CACHE1), cntr.get());

//        Ignite ignite = grid(grids - 1);
//
//        IgniteCache<Object, Object> cache1 = ignite.cache(CACHE1);
//        IgniteCache<Object, Object> cache2 = ignite.cache(CACHE2);
//
//        long size = cntr.get();
//
//        assertEquals(cache1.size(), size);

//        log.info("Data verification (size=" + size + ")");
//
//        // todo should check partitions
//        for (long k = 0; k < size; k++) {
//            // todo
//            if (k % 7 == 0)
//                continue;
//
//            assertEquals("k=" + k, generateValue(k, CACHE1), cache1.get(k));
//
//            if (k < TEST_SIZE)
//                assertEquals("k=" + k, generateValue(k, CACHE2), cache2.get(k));
//
//            if ((k + 1) % (size / 10) == 0)
//                log.info("Verified " + (k + 1) * 100 / size + "% entries");
//        }
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesCancelRebalance() throws Exception {
        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 400_000;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        loadData(ignite0, CACHE1, entriesCnt);
        loadData(ignite0, CACHE2, entriesCnt);

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(80);

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        U.sleep(2_000);

        verifyLocalCache(ignite0.cachex(CACHE1), ignite1.cachex(CACHE1));
        verifyLocalCache(ignite0.cachex(CACHE2), ignite1.cachex(CACHE2));

        verifyLocalCache(ignite0.cachex(CACHE1), ignite2.cachex(CACHE1));
        verifyLocalCache(ignite0.cachex(CACHE2), ignite2.cachex(CACHE2));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesCancelRebalancePartitioned() throws Exception {
        cacheMode = PARTITIONED;
        backups = 0;

        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        loadData(ignite0, CACHE1, entriesCnt);
        loadData(ignite0, CACHE2, entriesCnt);

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(80);

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        U.sleep(500);

        verifyCacheContent(ignite2.cache(CACHE1), entriesCnt);
        verifyCacheContent(ignite2.cache(CACHE2), entriesCnt);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesCancelRebalancePartitionedUnderConstantLoad() throws Exception {
        cacheMode = PARTITIONED;
        backups = 0;

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        loadData(ignite0, CACHE1, entriesCnt);
        loadData(ignite0, CACHE2, entriesCnt);

        forceCheckpoint(ignite0);

        AtomicLong cntr = new AtomicLong(entriesCnt);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(CACHE1), cntr, false, threads);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "loader");

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(80);

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        ldr.stop();

        ldrFut.get();

        verifyCacheContent(ignite2.cache(CACHE1), ldr.cntr.get());
        verifyCacheContent(ignite2.cache(CACHE2), entriesCnt);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
    public void testMultipleCachesCancelRebalancePartitionedUnderConstantLoadDynamicCacheStart() throws Exception {
        cacheMode = PARTITIONED;
        backups = 0;

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;
        int dynamicSize = 10_000;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        loadData(ignite0, CACHE1, entriesCnt);
        loadData(ignite0, CACHE2, entriesCnt);

        forceCheckpoint(ignite0);

        AtomicLong cntr = new AtomicLong(entriesCnt);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(CACHE1), cntr, false, threads);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "loader");

        CountDownLatch cacheStartLatch = new CountDownLatch(1);

        Random rnd = ThreadLocalRandom.current();

        IgniteInternalFuture dynamicCacheStartFut = GridTestUtils.runAsync(() -> {
            U.awaitQuiet(cacheStartLatch);

            IgniteCache<Long, Long> dynCache = ignite0.createCache(cacheConfig(DYNAMIC_CACHE));

            for (long i = 0; i < dynamicSize; i++)
                dynCache.put(i, generateValue(i, DYNAMIC_CACHE));
        }, "cache-starter");

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        U.sleep(rnd.nextInt(300));

        ignite0.cluster().setBaselineTopology(blt);

        cacheStartLatch.countDown();

        U.sleep(rnd.nextInt(300));

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        U.sleep(rnd.nextInt(300));

        ignite0.cluster().setBaselineTopology(blt);

        dynamicCacheStartFut.get();

        awaitPartitionMapExchange();

        ldr.stop();

        ldrFut.get();

        verifyCacheContent(ignite2.cache(CACHE1), ldr.cntr.get());
        verifyCacheContent(ignite2.cache(CACHE2), entriesCnt);
        verifyCacheContent(ignite2.cache(DYNAMIC_CACHE), dynamicSize);
    }

    /** todo */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesCancelRebalancePartitionedUnderConstantLoadUnstableTopology() throws Exception {
        cacheMode = PARTITIONED;
        backups = 3;

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;

        int timeout = 180_000;

        try {
            IgniteEx ignite0 = startGrid(0);

            ignite0.cluster().active(true);

            blt.add(ignite0.localNode());

            ignite0.cluster().setBaselineTopology(blt);

            loadData(ignite0, CACHE1, entriesCnt);
            loadData(ignite0, CACHE2, entriesCnt);

            forceCheckpoint(ignite0);

            AtomicLong cntr = new AtomicLong(entriesCnt);

            ConstantLoader ldr = new ConstantLoader(ignite0.cache(CACHE1), cntr, false, threads);

            IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "loader");

            long endTime = System.currentTimeMillis() + timeout;

            int nodes = 3;

            int started = 1;

            for (int i = 0; i < nodes; i++) {
                int time0 = ThreadLocalRandom.current().nextInt(1000);

                IgniteEx igniteX = startGrid(i + started);

                blt.add(igniteX.localNode());

                if (time0 % 2 == 0)
                    U.sleep(time0);

                ignite0.cluster().setBaselineTopology(blt);
            }

            do {
                for (int i = 0; i < nodes; i++) {
                    int time0 = ThreadLocalRandom.current().nextInt(2000);

                    U.sleep(time0);

                    stopGrid(i + started);
                }

                awaitPartitionMapExchange();

                for (int i = 0; i < nodes; i++) {
                    int time0 = ThreadLocalRandom.current().nextInt(1000);

                    if (time0 % 2 == 0)
                        U.sleep(time0);

                    startGrid(i + started);

                    //                blt.add(igniteX.localNode());;
                    //                ignite0.cluster().setBaselineTopology(blt);
                }

                awaitPartitionMapExchange();
            }
            while (U.currentTimeMillis() < endTime);

            awaitPartitionMapExchange();

            ldr.stop();

            ldrFut.get();

            for (Ignite g : G.allGrids()) {
                verifyCacheContent(g.cache(CACHE1), ldr.cntr.get());
                verifyCacheContent(g.cache(CACHE2), entriesCnt);
            }
        } catch (Error | RuntimeException | IgniteCheckedException e) {
            for (Ignite g : G.allGrids()) {
                GridPartitionFilePreloader filePreloader = ((IgniteEx)g).context().cache().context().filePreloader();

                synchronized (System.err) {
                    if (filePreloader != null)
                        filePreloader.printDiagnostic();
                }
            }

            throw e;
        }
    }

    /** todo */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesCancelRebalancePartitionedUnderConstantLoad2() throws Exception {
        cacheMode = PARTITIONED;
        backups = 3;

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;

        int timeout = 180_000;

        try {
            IgniteEx ignite0 = startGrid(0);

            ignite0.cluster().active(true);

            blt.add(ignite0.localNode());

            ignite0.cluster().setBaselineTopology(blt);

            loadData(ignite0, CACHE1, entriesCnt);
            loadData(ignite0, CACHE2, entriesCnt);

            forceCheckpoint(ignite0);

            AtomicLong cntr = new AtomicLong(entriesCnt);

            ConstantLoader ldr = new ConstantLoader(ignite0.cache(CACHE1), cntr, false, threads);

            IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "loader");

            long endTime = System.currentTimeMillis() + timeout;

            int nodes = 3;

            int started = 1;

            for (int i = 0; i < nodes; i++) {
                int time0 = ThreadLocalRandom.current().nextInt(1000);

                IgniteEx igniteX = startGrid(i + started);

                blt.add(igniteX.localNode());

                if (time0 % 2 == 0)
                    U.sleep(time0);

                ignite0.cluster().setBaselineTopology(blt);
            }

            for (int i = 0; i < nodes; i++) {
                int time0 = ThreadLocalRandom.current().nextInt(2000);

                U.sleep(time0);

                stopGrid(i + started);
            }

            U.sleep(3_000);


            for (int i = 0; i < nodes; i++) {
                int time0 = ThreadLocalRandom.current().nextInt(1000);

                if (time0 % 2 == 0)
                    U.sleep(time0);

                System.out.println("*******************************");
                System.out.println("  starting test killer " + (i + started));
                System.out.println("*******************************");

                startGrid(i + started);
            }


//            do {

//
//                awaitPartitionMapExchange();
//
//                for (int i = 0; i < nodes; i++) {
//                    int time0 = ThreadLocalRandom.current().nextInt(1000);
//
//                    if (time0 % 2 == 0)
//                        U.sleep(time0);
//
//                    startGrid(i + started);
//
//                    //                blt.add(igniteX.localNode());;
//
//
//
//                    //                ignite0.cluster().setBaselineTopology(blt);
//                }
//
//                awaitPartitionMapExchange();
//            }
//            while (U.currentTimeMillis() < endTime);

            awaitPartitionMapExchange();

            ldr.stop();

            ldrFut.get();

            for (Ignite g : G.allGrids()) {
                verifyCacheContent(g.cache(CACHE1), ldr.cntr.get());
                verifyCacheContent(g.cache(CACHE2), entriesCnt);
            }
        } catch (Error | RuntimeException | IgniteCheckedException e) {
            for (Ignite g : G.allGrids()) {
                GridPartitionFilePreloader filePreloader = ((IgniteEx)g).context().cache().context().filePreloader();

                synchronized (System.err) {
                    if (filePreloader != null)
                        filePreloader.printDiagnostic();
                }
            }

            throw e;
        }
    }

    /** todo */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
    public void testStartStopSingleNodeUnderLoad() throws Exception {
        cacheMode = PARTITIONED;
        backups = 3;

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 100_000;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        loadData(ignite0, CACHE1, entriesCnt);
        loadData(ignite0, CACHE2, entriesCnt);

        forceCheckpoint(ignite0);

        AtomicLong cntr = new AtomicLong(entriesCnt);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(CACHE1), cntr, false, threads);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "loader");

        int nodes = 1;

        int started = 1;

        for (int i = 0; i < nodes; i++) {
            IgniteEx igniteX = startGrid(i + started);

            blt.add(igniteX.localNode());

            System.out.println(">>> change baseline " + (i + started));

            ignite0.cluster().setBaselineTopology(blt);
        }

        awaitPartitionMapExchange();

        for (int i = 0; i < nodes; i++) {
            System.out.println(">>> stop node " + (i + started));

            stopGrid(i + started);
        }

        U.sleep(3_000);

        for (int i = 0; i < nodes; i++) {
            System.out.println(">>> start node " + (i + started));

            startGrid(i + started);
        }

        awaitPartitionMapExchange();

        ldr.stop();

        ldrFut.get();

        for (Ignite g : G.allGrids()) {
            verifyCacheContent(g.cache(CACHE1), ldr.cntr.get());
            verifyCacheContent(g.cache(CACHE2), entriesCnt);
        }
    }


    private void verifyCacheContent(IgniteCache<Object, Object> cache, long cnt) {
        verifyCacheContent(cache, cnt, false);
    }

    // todo should check partitions
    private void verifyCacheContent(IgniteCache<Object, Object> cache, long cnt, boolean removes) {
        log.info("Verifying cache contents [cache=" + cache.getName() + ", size=" + cnt + "]");

        StringBuilder buf = new StringBuilder();

        int fails = 0;

        long expSize = 0;

        for (long k = 0; k < cnt; k++) {
            if (removes && k % 10 == 0)
                continue;

            ++expSize;

            Long exp = generateValue(k, cache.getName());;
            Long actual = (Long)cache.get(k);

            if (!Objects.equals(exp, actual)) {
//                if (fails++ < 100)
                    buf.append("cache=").append(cache.getName()).append(", key=").append(k).append(", expect=").append(exp).append(", actual=").append(actual).append('\n');
//                else {
//                    buf.append("\n... and so on\n");

//                    break;
//                }
            }

            if ((k + 1) % (cnt / 10) == 0)
                log.info("Verification: " + (k + 1) * 100 / cnt + "%");
        }

        if (!removes && cnt != cache.size())
            buf.append("\ncache=").append(cache.getName()).append(" size mismatch [expect=").append(cnt).append(", actual=").append(cache.size()).append('\n');

        assertTrue(buf.toString(), buf.length() == 0);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesCancelRebalanceConstantLoad() throws Exception {
        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 400_000;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        int threads = Runtime.getRuntime().availableProcessors();

        loadData(ignite0, CACHE1, entriesCnt);
        loadData(ignite0, CACHE2, entriesCnt);

        AtomicLong cntr = new AtomicLong(entriesCnt);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(CACHE1), cntr, false, threads);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "thread");

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(80);

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        ldr.stop();

        ldrFut.get();

        U.sleep(500);

        verifyLocalCache(ignite0.cachex(CACHE1), ignite1.cachex(CACHE1));
        verifyLocalCache(ignite0.cachex(CACHE2), ignite1.cachex(CACHE2));

        verifyLocalCache(ignite0.cachex(CACHE1), ignite2.cachex(CACHE1));
        verifyLocalCache(ignite0.cachex(CACHE2), ignite2.cachex(CACHE2));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    @WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="1")
    public void testMultipleCachesCancelRebalanceConstantLoadPartitioned() throws Exception {
        cacheMode = PARTITIONED;
        parts = 64;
        backups = 3;

        List<ClusterNode> blt = new ArrayList<>();

        int entriesCnt = 400_000;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        int threads = Runtime.getRuntime().availableProcessors();

        loadData(ignite0, CACHE1, entriesCnt);
        loadData(ignite0, CACHE2, entriesCnt);

        AtomicLong cntr = new AtomicLong(entriesCnt);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(CACHE1), cntr, false, threads);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "thread");

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(80);

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        ldr.stop();

        ldrFut.get();

//        U.sleep(2_000);

        verifyCacheContent(ignite2.cache(CACHE1), cntr.get());
        verifyCacheContent(ignite2.cache(CACHE2), entriesCnt);
    }


    /** */
    @Test
    @Ignore
    @WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testManualCache() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(
            new CacheConfiguration<Integer, byte[]>("manual")
                .setCacheMode(CacheMode.PARTITIONED)
                .setRebalanceMode(CacheRebalanceMode.ASYNC)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setBackups(1)
                .setRebalanceDelay(-1)
                .setAffinity(new RendezvousAffinityFunction(false)
                    .setPartitions(8)));

        loadData(ignite0, "manual", TEST_SIZE);

        assertTrue(!ignite0.cluster().isBaselineAutoAdjustEnabled());

        IgniteEx ignite1 = startGrid(1);

        ignite1.cluster().setBaselineTopology(ignite1.cluster().nodes());

        printPartitionState("manual", 0);

        cache.put(TEST_SIZE, new byte[1000]);

        awaitPartitionMapExchange(true, true, Collections.singleton(ignite1.localNode()), true);
    }

    @Test
    public void testEvictions() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        GridCacheContext ctx = ignite0.cachex(DEFAULT_CACHE_NAME).context();

        for (GridDhtLocalPartition part : ctx.topology().currentLocalPartitions())
            part.dataStore().readOnly(true);

        PageMemoryEx memEx = (PageMemoryEx)ctx.dataRegion().pageMemory();

        final int groupId = ctx.groupId();

//        if (log.isDebugEnabled())
//            log.debug("Cleaning up region " + name);

        memEx.clearAsync(
            (grp, pageId) -> {
//                                if (isCancelled())
//                                    return false;
                return groupId == grp && PageIdUtils.partId(pageId) != 0;
            }, true)
            .listen(c1 -> {
                // todo misleading should be reformulate
//                            cctx.database().checkpointReadLock();
//                        cancelLock.lock();

                try {
//                            try {
//                    if (log.isDebugEnabled())
//                        log.debug("Off heap region cleared [node=" + cctx.localNodeId() + ", region=" + name + "]");

                    for (GridDhtLocalPartition part : ctx.topology().currentLocalPartitions()) {
                        //int grpId = gr;
                        //int partId = (int)partGrp;

                        CacheGroupContext grp = ctx.group();

                        int tag = ((PageMemoryEx)grp.dataRegion().pageMemory()).invalidate(groupId, part.id());

                        ((FilePageStoreManager)ctx.shared().pageStore()).getStore(groupId, part.id()).truncate(tag);

//                        if (log.isDebugEnabled())
//                            log.debug("Parition truncated [grp=" + cctx.cache().cacheGroup(grpId).cacheOrGroupName() + ", p=" + partId + "]");
                    }

//                    onDone();
                } catch (IgniteCheckedException e) {
                    e.printStackTrace();
//                    onDone(e);
//
//                    FileRebalanceFuture.this.onDone(e);
                }
//                finally {
////                            cancelLock.unlock();
//
////                                cctx.database().checkpointReadUnlock();
//                }
            });

        for (int i = 0; i < 1_000; i++)
            ctx.cache().put(i, i);

        forceCheckpoint();

        for (int i = 1_000; i < 2_000; i++)
            ctx.cache().put(i, i);

        for (GridDhtLocalPartition part : ctx.topology().currentLocalPartitions()) {
            part.updateSize(2);

            part.moving();

            part.readOnly(false);

            //log.info("p=" + part.id() + " size=" + part.publicSize(CU.cacheId(DEFAULT_CACHE_NAME)));

            part.rent(false);
        }

        U.sleep(5_000);

        log.info("cache size=" + ctx.cache().size());
    }

    /**
     * @param ignite Ignite instance to load.
     * @param name The cache name to add random data to.
     * @param size The total size of entries.
     */
    private void loadData(Ignite ignite, String name, int size) {
        try (IgniteDataStreamer<Long, Long> streamer = ignite.dataStreamer(name)) {
            streamer.allowOverwrite(true);

            for (long i = 0; i < size; i++) {
                if ((i + 1) % (size / 10) == 0)
                    log.info("Prepared " + (i + 1) * 100 / (size) + "% entries.");

                streamer.addData(i, generateValue(i, name));
            }
        }
    }

    /**
     * @param expCache Expected data cache.
     * @param actCache Actual data cache.

     * @throws IgniteCheckedException If failed.
     */
    private void verifyLocalCache(IgniteInternalCache<Integer, Integer> expCache,
        IgniteInternalCache<Integer, Integer> actCache) throws IgniteCheckedException {
        StringBuilder buf = new StringBuilder();

        buf.append(verifyLocalCacheContent(expCache, actCache));
        buf.append(verifyLocalCacheContent(actCache, expCache));

        for (GridDhtLocalPartition actPart : expCache.context().topology().currentLocalPartitions()) {
            GridDhtLocalPartition expPart = actCache.context().topology().localPartition(actPart.id());

            if (actPart.state() != expPart.state())
                buf.append("\n").append(expCache.context().localNodeId()).append(" vs ").append(actCache.context().localNodeId()).append(" state mismatch p=").append(actPart.id()).append(" exp=").append(expPart.state()).append(" act=").append(actPart.state());

            long expCntr = expPart.updateCounter();
            long actCntr = actPart.updateCounter();

            if (expCntr != actCntr)
                buf.append("\n").append("Counter not match p=").append(expPart.id()).append(", exp=").append(expCntr).append(", act=").append(actCntr);

            long expSize = expPart.fullSize();
            long actSize = actPart.fullSize();

            if (expSize != actSize)
                buf.append("\n").append("Size not match p=").append(expPart.id()).append(", exp=").append(expSize).append(", act=").append(actSize);
        }

        assertTrue(buf.toString(), buf.length() == 0);
    }

    /**
     * @param cache1 Expected data cache.
     * @param cache2 Actual data cache.

     * @throws IgniteCheckedException If failed.
     * @return Buffer with descriptions of found problems during verification.
     */
    private StringBuilder verifyLocalCacheContent(IgniteInternalCache<Integer, Integer> cache1,
        IgniteInternalCache<Integer, Integer> cache2) throws IgniteCheckedException {

        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

        UUID node1 = cache1.context().shared().localNodeId();
        UUID node2 = cache2.context().shared().localNodeId();

        StringBuilder buf = new StringBuilder();

        for (Cache.Entry<Integer, Integer> entry : cache1.localEntries(peekAll)) {
            Object expect = entry.getValue();
            Object actual = cache2.localPeek(entry.getKey(), peekAll);

            if (!Objects.equals(expect, actual))
                buf.append("\n").append(node1).append(" vs ").append(node2).append(", key=").append(entry.getKey()).append(", expected=").append(expect).append(", actual=").append(actual);

            if (buf.length() > 10 * 1024) {
                buf.append("\n").append("... and so on");

                break;
            }
        }

        return buf;
    }

    private static long generateValue(long num, String str) {
        return num + str.hashCode();
    }

    /** */
    private static class ConstantLoader implements Runnable {
        /** */
        private final AtomicLong cntr;

        /** */
        private final boolean enableRemove;

        /** */
        private final CyclicBarrier pauseBarrier;

        /** */
        private volatile boolean pause;

        /** */
        private volatile boolean paused;

        /** */
        private volatile boolean stop;

        /** */
        private final IgniteCache<Long, Long> cache;

        /** */
        public ConstantLoader(IgniteCache<Long, Long> cache, AtomicLong cntr, boolean enableRemove, int threadCnt) {
            this.cache = cache;
            this.cntr = cntr;
            this.enableRemove = enableRemove;
            this.pauseBarrier = new CyclicBarrier(threadCnt + 1); // +1 waiter
        }

        /** {@inheritDoc} */
        @Override public void run() {
            String cacheName = cache.getName();

            while (!stop && !Thread.currentThread().isInterrupted()) {
                if (pause) {
                    if (!paused) {
                        U.awaitQuiet(pauseBarrier);

                        log.info("Async loader paused.");

                        paused = true;
                    }

                    // Busy wait for resume.
                    try {
                        U.sleep(100);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        break;
                    }

                    continue;
                }

                long from = cntr.getAndAdd(100);

                for (long i = from; i < from + 100; i++)
                    cache.put(i, generateValue(i, cacheName));

                if (!enableRemove)
                    continue;

                for (long i = from; i < from + 100; i += 10)
                    cache.remove(i);
            }

            log.info("Async loader stopped.");
        }

        /**
         * Stop loader thread.
         */
        public void stop() {
            stop = true;
        }

        /**
         * Pause loading.
         */
        public void pause() {
            pause = true;

            log.info("Suspending loader threads: " + pauseBarrier.getParties());

            // Wait all workers came to barrier.
            U.awaitQuiet(pauseBarrier);

            log.info("Loader suspended");
        }

        /**
         * Resume loading.
         */
        public void resume() {
            paused = false;
            pause = false;

        }
    }
}
