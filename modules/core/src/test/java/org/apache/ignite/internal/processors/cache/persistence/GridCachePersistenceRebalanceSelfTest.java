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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
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
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERSISTENCE_REBALANCE_ENABLED;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
@RunWith(Parameterized.class)
public class GridCachePersistenceRebalanceSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_PART_COUNT = 16;

    /** */
    private static final int TEST_SIZE = GridTestUtils.SF.applyLB(100_000, 10_000);

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    @Parameterized.Parameter
    public CacheAtomicityMode cacheAtomicityMode;

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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(8 * 1024L * 1024 * 1024)
                    .setPersistenceEnabled(true))
                .setDataRegionConfigurations(new DataRegionConfiguration()
                    .setMaxSize(4*1024*1024*1024L)
                    .setPersistenceEnabled(true)
                    .setName("someRegion"))
                .setWalMode(WALMode.LOG_ONLY)
                .setCheckpointFrequency(3_000)) // todo check with default timeout!
//                .setWalSegmentSize(4 * 1024 * 1024)
//                .setMaxWalArchiveSize(32 * 1024 * 1024 * 1024L))
            .setCacheConfiguration(cacheConfig(DEFAULT_CACHE_NAME).setDataRegionName("someRegion"), cacheConfig(CACHE1), cacheConfig(CACHE2));
    }

    private CacheConfiguration cacheConfig(String name) {
        return new CacheConfiguration(name).setCacheMode(CacheMode.REPLICATED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setAtomicityMode(cacheAtomicityMode)
            //.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
//                .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, CACHE_PART_COUNT));
//            .setCommunicationSpi(new TestRecordingCommunicationSpi()
    }

    @Override protected long getPartitionMapExchangeTimeout() {
        return 60_000;
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    public void testReadRemovePartitionEviction() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);
        ignite0.cluster().baselineAutoAdjustTimeout(0);

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        IgniteInternalCache<Object, Object> cache = ignite0.cachex(DEFAULT_CACHE_NAME);

        CachePeekMode[] peekAll = new CachePeekMode[] {CachePeekMode.ALL};

        int hash = DEFAULT_CACHE_NAME.hashCode();

        for (int i = 0; i < TEST_SIZE; i++)
            assertEquals(i + hash, cache.localPeek(i, peekAll));

        List<GridDhtLocalPartition> locParts = cache.context().topology().localPartitions();

        CountDownLatch allPartsCleared = new CountDownLatch(locParts.size());

        ignite0.context().cache().context().database().checkpointReadLock();

        try {
            for (GridDhtLocalPartition part : locParts) {
                part.moving();

                part.dataStore().readOnly(true);

                part.clearAsync();

                part.onClearFinished(f -> {
                        allPartsCleared.countDown();
                    }
                );
            }
        } finally {
            ignite0.context().cache().context().database().checkpointReadUnlock();
        }

        System.out.println("Clearing partitions");

        allPartsCleared.await(20_000, TimeUnit.MILLISECONDS);

        // Ensure twice that all entries evicted.
        for (int i = 0; i < TEST_SIZE; i++)
            assertNull(cache.localPeek(i, peekAll));

        ignite0.context().cache().context().database().checkpointReadLock();

        try {
            for (GridDhtLocalPartition part : locParts) {
                part.dataStore().readOnly(false);

                part.own();
            }
        } finally {
            ignite0.context().cache().context().database().checkpointReadUnlock();
        }

        for (int i = 0; i < TEST_SIZE; i++)
            assertNull(cache.localPeek(i, peekAll));

        cache.put(TEST_SIZE, TEST_SIZE);

        assertEquals(TEST_SIZE, cache.get(TEST_SIZE));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    public void testPersistenceRebalanceBase() throws Exception {
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
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    public void testPersistenceRebalanceUnderConstantLoad() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);
        ignite0.cluster().baselineAutoAdjustTimeout(0);

        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);

        AtomicLong cntr = new AtomicLong(TEST_SIZE);

        ConstantLoader ldr = new ConstantLoader(ignite0.cache(DEFAULT_CACHE_NAME), cntr);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, 8, "thread");

        U.sleep(1_000);

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        U.sleep(1_000);

        awaitPartitionMapExchange();

        U.sleep(1_000);

        ldr.stop();

        ldrFut.get();

        U.sleep(1_000);

        verifyLocalCache(ignite0.cachex(DEFAULT_CACHE_NAME), ignite1.cachex(DEFAULT_CACHE_NAME));
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "true")
    public void testPersistenceRebalanceMultipleCaches() throws Exception {
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
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testPersistenceRebalanceMultipleCachesThreeNodesSequence() throws Exception {
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

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testPersistenceRebalanceMultipleCachesCancelRebalance() throws Exception {
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

        U.sleep(100);

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
    @Ignore
    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
    public void testPersistenceRebalanceManualCache() throws Exception {
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

//    /** */
//    @Test
//    @Ignore
//    @WithSystemProperty(key = IGNITE_JVM_PAUSE_DETECTOR_DISABLED, value = "true")
//    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "false")
//    @WithSystemProperty(key = IGNITE_PERSISTENCE_REBALANCE_ENABLED, value = "true")
//    @WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
//    public void testPersistenceRebalanceAsyncUpdates() throws Exception {
//        IgniteEx ignite0 = startGrid(0);
//
//        ignite0.cluster().active(true);
//
//        IgniteCache<Integer, byte[]> cache = ignite0.getOrCreateCache(
//            new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
//                .setCacheMode(CacheMode.PARTITIONED)
//                .setRebalanceMode(CacheRebalanceMode.ASYNC)
//                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
//                .setBackups(1)
//                .setAffinity(new RendezvousAffinityFunction(false)
//                    .setPartitions(8)));
//
//        loadData(ignite0, DEFAULT_CACHE_NAME, TEST_SIZE);
//
//        assertTrue(!ignite0.cluster().isBaselineAutoAdjustEnabled());
//
//        IgniteEx ignite1 = startGrid(1);
//
//        TestRecordingCommunicationSpi.spi(ignite1)
//            .blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
//                @Override public boolean apply(ClusterNode node, Message msg) {
//                    return msg instanceof GridPartitionBatchDemandMessage;
//                }
//            });
//
//        ignite1.cluster().setBaselineTopology(ignite1.cluster().nodes());
//
//        TestRecordingCommunicationSpi.spi(ignite1).waitForBlocked();
//
//        cache.put(TEST_SIZE, new byte[1000]);
//
//        awaitPartitionMapExchange(true, true, Collections.singleton(ignite1.localNode()), true);
//    }

    /**
     * @param ignite Ignite instance to load.
     * @param name The cache name to add random data to.
     * @param size The total size of entries.
     */
    private void loadData(Ignite ignite, String name, int size) {
        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(name)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < size; i++) {
                if ((i + 1) % (size / 10) == 0)
                    log.info("Prepared " + (i + 1) * 100 / (size) + "% entries.");

                streamer.addData(i, i + name.hashCode());
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
                buf.append("\n").append(expCache.context().localNodeId()).append(" vs ").append(actCache.context().localNodeId()).append(" state mismatch p=").append(actPart.id()).append(" exp=").append(expPart).append(" act=").append(actPart);

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
                buf.append("\n").append(node1).append(" vs ").append(node2).append(", expected=").append(expect).append(", actual=").append(actual);

            if (buf.length() > 10 * 1024) {
                buf.append("\n").append("... and so on");

                break;
            }
        }

        return buf;
    }

    /** */
    private static class ConstantLoader implements Runnable {
        /** */
        private final AtomicLong cntr;

        /** */
        private volatile boolean pause;

        /** */
        private volatile boolean paused;

        /** */
        private volatile boolean stop;

        /** */
        private final IgniteCache<Long, Long> cache;

        /** */
        public ConstantLoader(IgniteCache<Long, Long> cache, AtomicLong cntr) {
            this.cache = cache;
            this.cntr = cntr;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (!stop && !Thread.currentThread().isInterrupted()) {
                if (pause) {
                    if (!paused)
                        paused = true;

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
                    cache.put(i, i);

                for (long i = from; i < from + 100; i += 10)
                    cache.remove(i);
            }
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

            while (!paused) {
                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    break;
                }
            }
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
