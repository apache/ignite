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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_FILE_REBALANCE_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * File rebalancing tests.
 *
 * todo mixed rebalancing (file + historical)
 * todo mixed cache configuration (atomic+tx)
 * todo mixed data region configuration (pds+in-mem)
 * todo partition size change (start file rebalancing partition, cancel and then partition met)
 * todo [+/-] crd joins blt
 */
@WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
@WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
@WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value = "0")
public abstract class IgniteCacheFileRebalancingAbstractTest extends IgnitePdsCacheRebalancingCommonAbstractTest {
    /** Initial entries count. */
    private static final int INITIAL_ENTRIES_COUNT = 100_000;

    /** */
    private static final int DFLT_LOADER_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);

    /** */
    private final Function<Integer, TestValue> testValProducer = n -> new TestValue(n, n, n);

    /** {@inheritDoc} */
    @Override protected long checkpointFrequency() {
        return 3_000;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleRebalancingWithLoad() throws Exception {
        boolean checkRemoves = true;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        DataLoader<TestValue> ldr = testValuesLoader(checkRemoves, DFLT_LOADER_THREADS).loadData(ignite0);

        ldr.start();

        forceCheckpoint(ignite0);

        U.sleep(1_000);

        startGrid(1);

        ignite0.cluster().setBaselineTopology(2);

        awaitPartitionMapExchange();

        ldr.stop();

        verifyCache(ignite0, ldr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
    public void testHistoricalWithFileRebalancing() throws Exception {
        boolean checkRemoves = false;

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        List<ClusterNode> baseline = new ArrayList<>(3);

        baseline.add(ignite0.localNode());
        baseline.add(ignite1.localNode());

        //DataLoader<TestValue> ldr = testValuesLoader(checkRemoves, DFLT_LOADER_THREADS).loadData(ignite0);
        for (int i = 0; i < 20_000; i++)
            ignite0.cache(INDEXED_CACHE).put(i, new TestValue(i, i, i));

        ignite1.close();

        for (int i = 20_000; i < 25_000; i++)
            ignite0.cache(INDEXED_CACHE).put(i, new TestValue(i, i, i));

//        U.sleep(1_000);
//        ldr.stop();

        forceCheckpoint(ignite0);

        IgniteEx ignite2 = startGrid(2);

        baseline.add(ignite2.localNode());

//        startGrid(1);

        // we need to start node1 and include node2 into baseline at the same exchange.
        GridTestUtils.runAsync(() -> startGrid(1));

        ignite0.cluster().setBaselineTopology(baseline);

        awaitPartitionMapExchange();

//        ignite0.cluster().setBaselineTopology(baseline);
//
//        awaitPartitionMapExchange();
    }

    /**
     * Check file rebalancing when the coordinator joins the baseline.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCoordinatorJoinsBaselineWithLoad() throws Exception {
        boolean checkRemoves = false;

        IgniteEx node = startGrid(0);

        node.cluster().active(true);

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

        IgniteEx crd = startGrid(0);

        crd.cluster().active(true);

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
    public void testIndexedCacheStartStopLastNodeWithLoad() throws Exception {
        List<ClusterNode> blt = new ArrayList<>();

        boolean checkRemoves = false;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        DataLoader<TestValue> ldr = testValuesLoader(checkRemoves, DFLT_LOADER_THREADS).loadData(ignite0);

        ldr.start();

        forceCheckpoint(ignite0);

        IgniteEx ignite1 = startGrid(1);

        blt.add(ignite1.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(80));

        IgniteEx ignite2 = startGrid(2);

        blt.add(ignite2.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(80));

        IgniteEx ignite3 = startGrid(3);

        ClusterNode node3 = ignite3.localNode();

        blt.add(ignite3.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(50));

        stopGrid(3);

        blt.remove(node3);

        ignite0.cluster().setBaselineTopology(blt);

        U.sleep(ThreadLocalRandom.current().nextLong(100));

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

        log.info("Verifying cache contents [node=" +
            node.cluster().localNode().id() + " cache=" + name + ", size=" + cnt + "]");

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

        assertTrue(buf.toString(), buf.length() == 0);
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
