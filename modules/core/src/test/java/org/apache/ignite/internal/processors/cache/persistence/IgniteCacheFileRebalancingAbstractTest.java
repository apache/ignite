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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRequestMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_FILE_REBALANCE_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;

/**
 * File rebalancing tests.
 */
@WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
@WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value = "0")
public abstract class IgniteCacheFileRebalancingAbstractTest extends IgnitePdsCacheRebalancingCommonAbstractTest {
    /** Initial entries count. */
    private static final int INITIAL_ENTRIES_COUNT = 100_000;

    /** */
    private static final long AWAIT_TIME_SECONDS = 15_000;

    /** */
    private static final int DFLT_LOADER_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);

    /** */
    private static final String SHARED_GROUP = "shared_group";

    /** */
    private static final String SHARED1 = "shared1";

    /** */
    private static final String SHARED2 = "shared2";

    /** */
    private final Function<Integer, TestValue> testValProducer = n -> new TestValue(n, n, n);

    /** */
    private final Set<Integer> requestedGroups = new GridConcurrentHashSet<>();

    /** */
    private boolean checkEvts;

    /** {@inheritDoc} */
    @Override protected long checkpointFrequency() {
        return 5_000;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        assertFalse("File rebalance hasn't been triggered.", requestedGroups.isEmpty());

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg,
                IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                if (((GridIoMessage)msg).message() instanceof SnapshotRequestMessage) {
                    SnapshotRequestMessage msg0 =  ((SnapshotRequestMessage)((GridIoMessage)msg).message());

                    requestedGroups.addAll(msg0.parts().keySet());
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        // Add 2 more cache (shared cache group).
        CacheConfiguration[] ccfgs = cfg.getCacheConfiguration();

        int len = ccfgs.length;
        int rebalanceOrder = 10;

        CacheConfiguration[] ccfgs0 = Arrays.copyOf(ccfgs, ccfgs.length + 2);

        ccfgs0[len] = cacheConfiguration(SHARED1).setGroupName(SHARED_GROUP).setRebalanceOrder(rebalanceOrder);
        ccfgs0[len + 1] = cacheConfiguration(SHARED2).setGroupName(SHARED_GROUP).setRebalanceOrder(rebalanceOrder);

        cfg.setCacheConfiguration(ccfgs0);

        if (checkEvts)
            cfg.setIncludeEventTypes(EVT_CACHE_REBALANCE_PART_LOADED);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleRebalancing() throws Exception {
        checkEvts = true;

        IgniteEx ignite0 = startGrid(0, true);

        LoadParameters<TestValue> idxCache =
            testValuesLoader(false, DFLT_LOADER_THREADS).loadData(ignite0);

        LoadParameters<Integer> replicatedCache = new DataLoader<>(
            grid(0).cache(CACHE),
            INITIAL_ENTRIES_COUNT,
            n -> n,
            true,
            DFLT_LOADER_THREADS
        ).loadData(ignite0);

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        AtomicInteger actualParts = new AtomicInteger();

        ignite1.events().localListen(evt -> {
            if (evt.shortDisplay().contains("cache=" + INDEXED_CACHE + ",") ||
                evt.shortDisplay().contains("cache=" + CACHE + ","))
                actualParts.incrementAndGet();

            return true;
        }, EVT_CACHE_REBALANCE_PART_LOADED);

        ignite0.cluster().setBaselineTopology(2);

        awaitPartitionMapExchange();

        int expParts = ignite0.cachex(INDEXED_CACHE).context().affinity().partitions() +
            ignite0.cachex(CACHE).context().affinity().partitions();

        assertEquals(expParts, actualParts.get());

        assertTrue(requestedGroups.contains(CU.cacheId(INDEXED_CACHE)));
        assertTrue(requestedGroups.contains(CU.cacheId(CACHE)));

        verifyCache(ignite1, idxCache);
        verifyCache(ignite1, replicatedCache);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
    public void testHistoricalStartsAfterFilesPreloading() throws Exception {
        assert backups() > 0 : backups();

        checkEvts = true;

        IgniteEx ignite0 = startGrid(0, true);

        DataLoader<TestValue> ldr = testValuesLoader(false, DFLT_LOADER_THREADS).loadData(ignite0);

        ldr.start();

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        CountDownLatch cacheStartLatch = new CountDownLatch(1);

        AtomicInteger loadedParts = new AtomicInteger();

        IgniteInternalCache<Object, Object> cache = ignite0.cachex(INDEXED_CACHE);

        int totalPartitions = cache.affinity().partitions();

        TestRecordingCommunicationSpi recCommSpi = TestRecordingCommunicationSpi.spi(ignite1);

        IgniteBiPredicate<ClusterNode, Message> msgPred =
            (node, msg) -> (msg instanceof GridDhtPartitionDemandMessage) &&
                ((GridCacheGroupIdMessage)msg).groupId() == cache.context().groupId() &&
                ((GridDhtPartitionDemandMessage)msg).partitions().historicalSet().size() == totalPartitions;

        // Recording all historical demand messages.
        recCommSpi.record(msgPred);

        ignite1.events().localListen(evt -> {
            if (evt.shortDisplay().contains("cache=" + INDEXED_CACHE + ",")) {
                if (loadedParts.incrementAndGet() == totalPartitions)
                    cacheStartLatch.countDown();
            }

            return true;
        }, EVT_CACHE_REBALANCE_PART_LOADED);

        recCommSpi.blockMessages(msgPred);

        // After baseline has changed.file rebalance should start.
        ignite0.cluster().setBaselineTopology(2);

        // Wait until partition files received.
        cacheStartLatch.await();

        // File rebalancing should request historiacal rebalance for loaded partitions.
        recCommSpi.waitForBlocked();

        // Changing minor topology version to interrupt rebalancing routine.
        ignite1.getOrCreateCache(new CacheConfiguration<>("tmp-cache"));

        // Current rebalance routine should be restarted.
        recCommSpi.stopBlock();

        awaitPartitionMapExchange();

        ldr.stop();

        verifyCache(ignite1, ldr);

        List<Object> msgs = recCommSpi.recordedMessages(true);

        assertEquals("Expecting specified count demand messages for historical rebalance.", 2, msgs.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
    public void testHistoricalNotStartsOnFilesPreloadingInterruption() throws Exception {
        assert backups() > 0 : backups();

        checkEvts = true;

        IgniteEx ignite0 = startGrid(0, true);

        DataLoader<TestValue> ldr = testValuesLoader(false, DFLT_LOADER_THREADS).loadData(ignite0);

        ldr.start();

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        CountDownLatch cacheStartLatch = new CountDownLatch(1);

        AtomicInteger loadedParts = new AtomicInteger();

        IgniteInternalCache<Object, Object> cache = ignite0.cachex(INDEXED_CACHE);

        int totalPartitions = cache.affinity().partitions();

        ignite1.events().localListen(evt -> {
            if (evt.shortDisplay().contains("cache=" + INDEXED_CACHE + ",")) {
                if (loadedParts.incrementAndGet() > totalPartitions / 4)
                    cacheStartLatch.countDown();
            }

            return true;
        }, EVT_CACHE_REBALANCE_PART_LOADED);

        // After baseline has changed.file rebalance should start.
        ignite0.cluster().setBaselineTopology(2);

        // Wait until some partition files received.
        cacheStartLatch.await();

        // Switching this partitions from read-only to normal mode.
        forceCheckpoint(ignite1);

        // Changing minor topology version to interrupt rebalancing routine.
        ignite1.getOrCreateCache(new CacheConfiguration<>("tmp-cache"));

        awaitPartitionMapExchange();

        ldr.stop();

        // If historical rebalancing starts after interrupting file preloading,
        // we'll get inconsistent indexes and cache verification should fail.
        verifyCache(ignite1, ldr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleRebalancingWithLoad() throws Exception {
        boolean checkRemoves = true;

        IgniteEx ignite0 = startGrid(0, true);

        DataLoader<TestValue> idxLdr = testValuesLoader(checkRemoves, DFLT_LOADER_THREADS).loadData(ignite0);

        DataLoader<Integer> cacheLdr = new DataLoader<>(
            grid(0).cache(CACHE),
            INITIAL_ENTRIES_COUNT,
            n -> n,
            true,
            DFLT_LOADER_THREADS
        ).loadData(ignite0);

        idxLdr.start();
        cacheLdr.start();

        forceCheckpoint(ignite0);

        U.sleep(1_000);

        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().setBaselineTopology(2);

        awaitPartitionMapExchange();

        idxLdr.stop();
        cacheLdr.stop();

        assertTrue(requestedGroups.contains(CU.cacheId(INDEXED_CACHE)));
        assertTrue(requestedGroups.contains(CU.cacheId(CACHE)));

        verifyCache(ignite1, idxLdr);
        verifyCache(ignite1, cacheLdr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleRebalancingSharedGroupOrdered() throws Exception {
        boolean checkRemoves = true;

        IgniteEx ignite0 = startGrid(0, true);

        DataLoader<TestValue> idxLdr = testValuesLoader(checkRemoves, 1).loadData(ignite0);

        DataLoader<Integer> sharedLdr1 = new DataLoader<>(
            grid(0).cache(SHARED1),
            INITIAL_ENTRIES_COUNT,
            n -> n,
            true,
            1
        ).loadData(ignite0);

        DataLoader<Integer> sharedLdr2 = new DataLoader<>(
            grid(0).cache(SHARED2),
            INITIAL_ENTRIES_COUNT,
            n -> n,
            true,
            1
        ).loadData(ignite0);

        idxLdr.start();
        sharedLdr1.start();
        sharedLdr2.start();

        forceCheckpoint(ignite0);

        U.sleep(1_000);

        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().setBaselineTopology(2);

        awaitPartitionMapExchange();

        idxLdr.stop();
        sharedLdr1.stop();
        sharedLdr2.stop();

        assertTrue(requestedGroups.contains(CU.cacheId(INDEXED_CACHE)));
        assertTrue(requestedGroups.contains(CU.cacheId(SHARED_GROUP)));

        verifyCache(ignite1, idxLdr);
        verifyCache(ignite1, sharedLdr1);
        verifyCache(ignite1, sharedLdr2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Ignore
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
    public void testHistoricalWithFileRebalancing() throws Exception {
        assert backups() >= 2;

        IgniteEx ignite0 = startGrid(0, false);
        IgniteEx ignite1 = startGrid(1, false);
        IgniteEx ignite2 = startGrid(2, true);

        ignite0.cluster().active(true);

        List<ClusterNode> baseline = new ArrayList<>(3);

        baseline.add(ignite0.localNode());
        baseline.add(ignite1.localNode());
        baseline.add(ignite2.localNode());

        for (int i = 0; i < 20_000; i++)
            ignite0.cache(INDEXED_CACHE).put(i, new TestValue(i, i, i));

        forceCheckpoint(ignite0);

        String inst2Name = ignite2.name();

        stopGrid(1);
        stopGrid(2);

        for (int i = 20_000; i < 25_000; i++)
            ignite0.cache(INDEXED_CACHE).put(i, new TestValue(i, i, i));

        cleanPersistenceDir(inst2Name);

        CountDownLatch startLatch = new CountDownLatch(1);

        IgniteInternalFuture fut1 = GridTestUtils.runAsync( () -> {
            startLatch.await(AWAIT_TIME_SECONDS, TimeUnit.SECONDS);

            startGrid(1);

            return null;
        });

        IgniteInternalFuture fut2 = GridTestUtils.runAsync( () -> {
            startLatch.await(AWAIT_TIME_SECONDS, TimeUnit.SECONDS);

            startGrid(2);

            return null;
        });

        startLatch.countDown();

        fut1.get();
        fut2.get();

        awaitPartitionMapExchange();
    }

    /**
     * Check file rebalancing when the coordinator joins the baseline.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorJoinsBaselineWithLoad() throws Exception {
        boolean checkRemoves = false;

        IgniteEx node = startGrid(0, true);

        IgniteEx crd = startGrid(1);

        stopGrid(0);

        node = startGrid(0);

        awaitPartitionMapExchange();

        Collection<Object> constIds =
            F.viewReadOnly(node.cluster().currentBaselineTopology(), BaselineNode::consistentId);

        // Ensure that coordinator node is not in baseline.
        assert U.oldest(crd.cluster().nodes(), null).equals(crd.localNode());
        assert !constIds.contains(crd.localNode().consistentId()) : constIds;
        assert constIds.contains(node.localNode().consistentId()) : constIds;

        DataLoader<TestValue> ldr = new DataLoader<>(
            node.cache(INDEXED_CACHE),
            INITIAL_ENTRIES_COUNT,
            testValProducer,
            checkRemoves,
            DFLT_LOADER_THREADS
        ).loadData(node);

        forceCheckpoint(node);

        ldr.start();

        List<ClusterNode> blt = new ArrayList<>();

        blt.add(node.localNode());
        blt.add(crd.localNode());

        node.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        ldr.stop();

        verifyCache(crd, ldr);
    }

    /**
     * Ensures that file rebalancing starts every time the baseline changes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousBaselineChangeWithLoad() throws Exception {
        boolean checkRemoves = false;

        IgniteEx crd = startGrid(0, true);

        DataLoader<TestValue> ldr = testValuesLoader(checkRemoves, DFLT_LOADER_THREADS).loadData(crd);

        ldr.start();

        Set<ClusterNode> blt = new GridConcurrentHashSet<>();

        blt.add(crd.localNode());

        long timeout = U.currentTimeMillis() + 30_000;

        AtomicInteger nodes = new AtomicInteger(1);

        int grids = 4;

        int backups = backups();

        assert backups > 0;

        BlockingQueue<Integer> queue = new ArrayBlockingQueue<>(4);

        forceCheckpoint();

        do {
            GridTestUtils.runMultiThreadedAsync( () -> {
                U.sleep(ThreadLocalRandom.current().nextLong(100));

                int n = nodes.incrementAndGet();

                Ignite node = startGrid(n);

                queue.add(n);

                blt.add(node.cluster().localNode());

                crd.cluster().setBaselineTopology(blt);

                return null;
            }, grids, "starter");

            int stopped = 0;

            do {
                Integer n = queue.poll(30, TimeUnit.SECONDS);

                assert n != null;

                ++stopped;

                ClusterNode node = grid(n).cluster().localNode();

                stopGrid(n);

                blt.remove(node);

                crd.cluster().setBaselineTopology(blt);

                if (stopped % backups == 0) {
                    awaitPartitionMapExchange();

                    if (stopped != grids) {
                        ldr.suspend();

                        for (Ignite g : G.allGrids()) {
                            if (!g.name().equals(crd.name())) {
                                verifyCache(crd, ldr);

                                break;
                            }
                        }

                        ldr.resume();
                    }
                }
            } while (stopped < grids);

            awaitPartitionMapExchange();
        } while (U.currentTimeMillis() < timeout);

        ldr.stop();

        verifyCache(crd, ldr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test4nodesRestartLastNodeWithLoad() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        List<ClusterNode> blt = new ArrayList<>();

        boolean checkRemoves = true;

        IgniteEx ignite0 = startGrid(0, true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        DataLoader<TestValue> ldr = testValuesLoader(checkRemoves, DFLT_LOADER_THREADS).loadData(ignite0);

        ldr.start();

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(rnd.nextLong(2000));

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(rnd.nextLong(2000));

        IgniteEx ignite3 = startGrid(3);

        ClusterNode node3 = ignite3.localNode();

        blt.add(ignite3.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(rnd.nextLong(2000));

        stopGrid(3);

        blt.remove(node3);

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(rnd.nextLong(2000));

        ignite3 = startGrid(3);

        blt.add(ignite3.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        awaitPartitionMapExchange();

        ldr.stop();

        verifyCache(ignite3, ldr);
    }

    /**
     * @param enableRemoves Enabled entries removes.
     * @param threadsCnt Threads count.
     * @return make loader for indexed cache.
     */
    private DataLoader<TestValue> testValuesLoader(boolean enableRemoves, int threadsCnt) {
        return new DataLoader<>(
            grid(0).cache(INDEXED_CACHE),
            INITIAL_ENTRIES_COUNT,
            testValProducer,
            enableRemoves,
            threadsCnt
        );
    }

    /**
     * @param node Target node.
     * @param cfg Testing paramters.
     * @param <V> Type of value.
     * @throws Exception If failed.
     */
    protected <V> void verifyCache(IgniteEx node, LoadParameters<V> cfg) throws Exception {
        String name = cfg.cacheName();
        int cnt = cfg.entriesCnt();
        boolean removes = cfg.checkRemoves();
        Function<Integer, V> valProducer = cfg.valueProducer();

        log.info("Verifying cache contents [node=" + node.cluster().localNode().id() +
            " cache=" + name + ", size=" + cnt + "]");

        IgniteCache<Integer, V> cache = node.cache(name);

        StringBuilder buf = new StringBuilder();

        int fails = 0;

        for (int k = 0; k < cnt; k++) {
            if (removes && k % 10 == 0)
                continue;

            V exp = valProducer.apply(k);
            V actual = cache.get(k);

            if (!Objects.equals(exp, actual)) {
                if (fails++ < 100) {
                    buf.append("cache=").append(cache.getName()).append(", key=").append(k).append(", expect=").
                        append(exp).append(", actual=").append(actual).append('\n');
                }
                else {
                    buf.append("\n... and so on\n");

                    break;
                }
            }

            if ((k + 1) % (cnt / 10) == 0)
                log.info("Verification: " + (k + 1) * 100 / cnt + "%");
        }

        if (!removes && cnt != cache.size()) {
            buf.append("\ncache=").append(cache.getName()).append(" size mismatch [expect=").append(cnt).
                append(", actual=").append(cache.size()).append('\n');
        }

        // todo remove next block
        if (buf.length() > 0) {
            long size = 0;

            for (Ignite g : G.allGrids()) {
                log.info("Partitions states [node=" + g.name() + ", cache=" + name + "]");

                CacheGroupContext ctx = ((IgniteEx)g).cachex(name).context().group();

                for (GridDhtLocalPartition part : ctx.topology().currentLocalPartitions())
                    log.info("\tp="+part.id() + " size=" + part.fullSize() + " init=" + part.initialUpdateCounter() + ", cntr=" + part.updateCounter() + ", state="+ part.state() + " mode=" + (part.primary(ctx.affinity().lastVersion()) ? "PRIMARY" : "BACKUP"));

                for (GridDhtLocalPartition part : ctx.topology().currentLocalPartitions()) {
                    boolean primary = part.primary(ctx.affinity().lastVersion());

                    if (primary)
                        size += part.fullSize();
                }
            }

            log.info("Expected size for " + name + " is " + size);
        }

        assertTrue(buf.toString(), buf.length() == 0);
    }

    /**
     * @param idx Node index.
     * @param activate Activate flag.
     */
    private IgniteEx startGrid(int idx, boolean activate) throws Exception {
        IgniteEx ignite = startGrid(idx);

        if (activate)
            ignite.cluster().state(ClusterState.ACTIVE);

        if (idx == 0)
            ignite.cluster().baselineAutoAdjustEnabled(false);

        return ignite;
    }

    /** */
    interface LoadParameters<V> {
        /** */
        public int entriesCnt();

        /** */
        public String cacheName();

        /** */
        public Function<Integer, V> valueProducer();

        /** */
        public boolean checkRemoves();
    }

    /** */
    protected static class DataLoader<V> implements Runnable, LoadParameters<V> {
        /** */
        private final AtomicInteger cntr;

        /** */
        private final boolean enableRmv;

        /** */
        private final CyclicBarrier pauseBarrier;

        /** */
        private volatile boolean pause;

        /** */
        private volatile boolean paused;

        /** */
        private volatile boolean stop;

        /** */
        private final IgniteCache<Integer, V> cache;

        /** */
        private final Function<Integer, V> valFunc;

        /** */
        private final int threadCnt;

        /** */
        private volatile IgniteInternalFuture ldrFut;

        /** */
        public DataLoader(IgniteCache<Integer, V> cache, int initCnt, Function<Integer, V> valFunc, boolean enableRmv, int threadCnt) {
            this.cache = cache;
            this.enableRmv = enableRmv;
            this.threadCnt = threadCnt;
            this.valFunc = valFunc;

            pauseBarrier = new CyclicBarrier(threadCnt + 1); // +1 waiter (suspend originator)
            cntr = new AtomicInteger(initCnt);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (!stop && !Thread.currentThread().isInterrupted()) {
                if (pause) {
                    if (!paused) {
                        U.awaitQuiet(pauseBarrier);

                        log.info("Async loader paused.");

                        paused = true;
                    }

                    // Busy wait for resume.
                    try {
                        U.sleep(300);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        break;
                    }

                    continue;
                }

                int from = cntr.getAndAdd(100);

                for (int i = from; i < from + 100; i++)
                    cache.put(i, valFunc.apply(i));

                if (!enableRmv)
                    continue;

                for (int i = from; i < from + 100; i += 10)
                    cache.remove(i);
            }

            log.info("Async loader stopped.");
        }

        /** */
        public void start() {
            ldrFut = GridTestUtils.runMultiThreadedAsync(this, threadCnt, "thread");
        }

        /**
         * Stop loader thread.
         */
        public void stop() throws IgniteCheckedException {
            stop = true;

            ldrFut.get(10_000);
        }

        /**
         * Pause loading.
         */
        public void suspend() {
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
            pause = paused = false;
        }

        /**
         * @param node Data originator.
         * @return Data loader instance.
         */
        public DataLoader<V> loadData(Ignite node) {
            int size = cntr.get();

            try (IgniteDataStreamer<Integer, V> streamer = node.dataStreamer(cache.getName())) {
                for (int i = 0; i < size; i++) {
                    if ((i + 1) % (size / 10) == 0)
                        log.info("Prepared " + (i + 1) * 100 / (size) + "% entries.");

                    streamer.addData(i, valFunc.apply(i));
                }
            }

            return this;
        }

        /** */
        @Override public int entriesCnt() {
            return cntr.get();
        }

        /** */
        @Override public String cacheName() {
            return cache.getName();
        }

        /** */
        @Override public Function<Integer, V> valueProducer() {
            return valFunc;
        }

        /** */
        @Override public boolean checkRemoves() {
            return enableRmv;
        }
    }
}
