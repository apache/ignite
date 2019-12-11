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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_FILE_REBALANCE_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_FILE_REBALANCE_THRESHOLD;

/**
 * File rebalancing tests.
 */
@WithSystemProperty(key = IGNITE_FILE_REBALANCE_ENABLED, value = "true")
@WithSystemProperty(key = IGNITE_BASELINE_AUTO_ADJUST_ENABLED, value = "false")
@WithSystemProperty(key = IGNITE_PDS_FILE_REBALANCE_THRESHOLD, value="0")
public abstract class IgnitePdsCacheFileRebalancingAbstractTest extends IgnitePdsCacheRebalancingCommonAbstractTest {
    /** Initial entries count. */
    private static final int INITIAL_ENTRIES_COUNT = 100_000;

    private static final int threas = Math.min(2, Runtime.getRuntime().availableProcessors() / 2);

    /** {@inheritDoc} */
    @Override protected long checkpointFrequency() {
        return 3_000;
    }

    @Test
    public void testSimpleRebalancingWithConstantLoad() throws Exception {
        boolean removes = true;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);
        ignite0.cluster().baselineAutoAdjustTimeout(0);

        loadData(ignite0, INDEXED_CACHE, INITIAL_ENTRIES_COUNT);

        AtomicInteger cntr = new AtomicInteger(INITIAL_ENTRIES_COUNT);

        DataLoader ldr = new DataLoader(ignite0.cache(INDEXED_CACHE), cntr, removes, threas);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, 2, "thread");

        forceCheckpoint(ignite0);

        startGrid(1);

        awaitPartitionMapExchange();

        ldr.stop();

        ldrFut.get();

        verifyCacheContent(ignite0, INDEXED_CACHE, cntr.get(), removes);
    }

    @Test
    public void testIndexedCacheStartStopLastNodeConstantLoadPartitioned() throws Exception {
        List<ClusterNode> blt = new ArrayList<>();

        boolean checkRemoves = false;

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        blt.add(ignite0.localNode());

        ignite0.cluster().setBaselineTopology(blt);

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        loadData(ignite0, INDEXED_CACHE, INITIAL_ENTRIES_COUNT);

        AtomicInteger cntr = new AtomicInteger(INITIAL_ENTRIES_COUNT);

        DataLoader ldr = new DataLoader(ignite0.cache(INDEXED_CACHE), cntr, checkRemoves, threads);

        IgniteInternalFuture ldrFut = GridTestUtils.runMultiThreadedAsync(ldr, threads, "thread");

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

        ldrFut.get();

        verifyCacheContent(ignite3, INDEXED_CACHE, cntr.get(), checkRemoves);
    }

    protected void verifyCacheContent(IgniteEx node, String cacheName, int entriesCnt, boolean removes) throws Exception {
        log.info("Verifying cache contents [node=" + node.cluster().localNode().id() + " cache=" + cacheName + ", size=" + entriesCnt + "]");

        IgniteCache cache = node.cache(cacheName);

        StringBuilder buf = new StringBuilder();

        int fails = 0;

        for (int k = 0; k < entriesCnt; k++) {
            if (removes && k % 10 == 0)
                continue;

            TestValue exp = new TestValue(k, k, k);;
            TestValue actual = (TestValue)cache.get(k);

            if (!Objects.equals(exp, actual)) {
                if (fails++ < 100)
                    buf.append("cache=").append(cache.getName()).append(", key=").append(k).append(", expect=").append(exp).append(", actual=").append(actual).append('\n');
                else {
                    buf.append("\n... and so on\n");

                    break;
                }
            }

            if ((k + 1) % (entriesCnt / 10) == 0)
                log.info("Verification: " + (k + 1) * 100 / entriesCnt + "%");
        }

        if (!removes && entriesCnt != cache.size())
            buf.append("\ncache=").append(cache.getName()).append(" size mismatch [expect=").append(entriesCnt).append(", actual=").append(cache.size()).append('\n');

        assertTrue(buf.toString(), buf.length() == 0);
    }

    /**
     * @param ignite Ignite instance to load.
     * @param name The cache name to add random data to.
     * @param size The total size of entries.
     */
    private void loadData(Ignite ignite, String name, int size) {
        try (IgniteDataStreamer<Integer, TestValue> streamer = ignite.dataStreamer(name)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < size; i++) {
                if ((i + 1) % (size / 10) == 0)
                    log.info("Prepared " + (i + 1) * 100 / (size) + "% entries.");

                streamer.addData(i, new TestValue(i, i, i));
            }
        }
    }

    /** */
    private static class DataLoader implements Runnable {
        /** */
        private final AtomicInteger cntr;

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
        private final IgniteCache<Integer, TestValue> cache;

        /** */
        public DataLoader(IgniteCache<Integer, TestValue> cache, AtomicInteger cntr, boolean enableRemove, int threadCnt) {
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

                int from = cntr.getAndAdd(100);

                for (int i = from; i < from + 100; i++)
                    cache.put(i, new TestValue(i, i, i));

                if (!enableRemove)
                    continue;

                for (int i = from; i < from + 100; i += 10)
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
