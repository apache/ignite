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
package org.apache.ignite.internal.processors.database;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.processors.cache.database.MemoryMetricsImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.lang.Thread.sleep;

/**
 *
 */
public class MemoryMetricsSelfTest extends GridCommonAbstractTest {
    /** */
    private MemoryMetricsImpl memMetrics;

    /** */
    private int threadsCnt = 1;

    /** */
    private Thread[] allocationThreads;

    /** */
    private Thread watcherThread;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MemoryPolicyConfiguration plcCfg = new MemoryPolicyConfiguration();

        memMetrics = new MemoryMetricsImpl(plcCfg);

        memMetrics.enableMetrics();
    }

    /**
     * Test for allocationRate metric in single-threaded mode.
     * @throws Exception if any happens during test.
     */
    public void testAllocationRateSingleThreaded() throws Exception {
        threadsCnt = 1;
        memMetrics.rateTimeInterval(10);

        CountDownLatch startLatch = new CountDownLatch(1);

        startAllocationThreads(startLatch, 340, 50);
        AllocationRateWatcher watcher = startWatcherThread(startLatch, 20);

        startLatch.countDown();

        joinAllThreads();

        assertEquals(4, watcher.rateDropsCntr);
    }

    /**
     * Test for allocationRate metric in multi-threaded mode with short silent period in the middle of the test.
     * @throws Exception if any happens during test.
     */
    public void testAllocationRateMultiThreaded() throws Exception {
        threadsCnt = 4;
        memMetrics.rateTimeInterval(5);

        CountDownLatch startLatch = new CountDownLatch(1);

        startAllocationThreads(startLatch, 7_800, 1);

        AllocationRateWatcher watcher = startWatcherThread(startLatch, 20);

        startLatch.countDown();

        joinAllocationThreads();

        assertEquals(4, watcher.rateDropsCntr);

        sleep(3);

        threadsCnt = 8;

        CountDownLatch restartLatch = new CountDownLatch(1);

        startAllocationThreads(restartLatch, 8_000, 1);

        restartLatch.countDown();

        joinAllThreads();

        assertTrue(watcher.rateDropsCntr > 4);
    }

    /**
     * Test verifies that allocationRate calculation algorithm survives setting new values to rateTimeInterval parameter.
     * @throws Exception if any happens during test.
     */
    public void testAllocationRateTimeIntervalConcurrentChange() throws Exception {
        threadsCnt = 5;
        memMetrics.rateTimeInterval(5);

        CountDownLatch startLatch = new CountDownLatch(1);

        startAllocationThreads(startLatch, 10_000, 1);

        AllocationRateWatcher watcher = startWatcherThread(startLatch, 20);

        startLatch.countDown();

        for (int i = 0; i < 10; i++) {
            Thread.sleep(25);

            memMetrics.rateTimeInterval((2 + i * 5) % 3 + 1);
        }

        joinAllThreads();

        assertTrue(watcher.rateDropsCntr > 4);
    }

    /**
     *
     * @throws Exception if any happens during test.
     */
    public void testAllocationRateSubintervalsConcurrentChange() throws Exception {
        threadsCnt = 5;
        memMetrics.rateTimeInterval(5);

        CountDownLatch startLatch = new CountDownLatch(1);

        startAllocationThreads(startLatch, 10_000, 1);

        AllocationRateWatcher watcher = startWatcherThread(startLatch, 20);

        startLatch.countDown();

        for (int i = 0; i < 10; i++) {
            Thread.sleep(25);

            memMetrics.subIntervals((2 + i * 5) % 3 + 1);
        }

        joinAllThreads();

        assertTrue(watcher.rateDropsCntr > 4);
    }

    /**
     * @param startLatch Start latch.
     * @param watchingDelay Watching delay.
     */
    private AllocationRateWatcher startWatcherThread(CountDownLatch startLatch, int watchingDelay) {
        AllocationRateWatcher watcher = new AllocationRateWatcher(startLatch, memMetrics, watchingDelay);

        watcherThread = new Thread(watcher);

        watcherThread.start();

        return watcher;
    }

    /**
     * @param startLatch Start latch.
     * @param iterationsCnt Iterations count.
     * @param allocationsDelay Allocations delay.
     */
    private void startAllocationThreads(CountDownLatch startLatch, int iterationsCnt, int allocationsDelay) {
        assert threadsCnt > 0;

        allocationThreads = new Thread[threadsCnt];

        for (int i = 0; i < threadsCnt; i++) {
            AllocationsIncrementer inc = new AllocationsIncrementer(startLatch, memMetrics, iterationsCnt, allocationsDelay);

            Thread incThread = new Thread(inc);
            incThread.start();

            allocationThreads[i] = incThread;
        }
    }

    /**
     *
     */
    private void joinAllThreads() throws Exception {
        joinAllocationThreads();

        watcherThread.interrupt();
        watcherThread.join();
    }

    /**
     *
     */
    private void joinAllocationThreads() throws Exception {
        assert allocationThreads != null;
        assert allocationThreads.length > 0;

        for (Thread allocationThread : allocationThreads)
            allocationThread.join();
    }

    /**
     *
     */
    private static class AllocationsIncrementer implements Runnable {
        /** */
        private final CountDownLatch startLatch;

        /** */
        private final MemoryMetricsImpl memMetrics;

        /** */
        private final int iterationsCnt;

        /** */
        private final int delay;

        /**
         * @param startLatch Start latch.
         * @param memMetrics Mem metrics.
         * @param iterationsCnt Iterations count.
         * @param delay Delay.
         */
        private AllocationsIncrementer(CountDownLatch startLatch, MemoryMetricsImpl memMetrics, int iterationsCnt, int delay) {
            this.startLatch = startLatch;
            this.memMetrics = memMetrics;
            this.iterationsCnt = iterationsCnt;
            this.delay = delay;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                startLatch.await();

                for (int i = 0; i < iterationsCnt; i++) {
                    memMetrics.incrementTotalAllocatedPages();

                    sleep(delay);
                }
            }
            catch (InterruptedException ignore) {
                // No-op.
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     */
    private static class AllocationRateWatcher implements Runnable {
        /** */
        private volatile int rateDropsCntr;

        /** */
        private final CountDownLatch startLatch;

        /** */
        private final MemoryMetrics memMetrics;

        /** */
        private final int delay;

        /**
         * @param startLatch Start latch.
         * @param memMetrics Mem metrics.
         * @param delay Delay.
         */
        private AllocationRateWatcher(CountDownLatch startLatch, MemoryMetrics memMetrics, int delay) {
            this.startLatch = startLatch;
            this.memMetrics = memMetrics;
            this.delay = delay;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                startLatch.await();

                float prevRate = 0;

                while (!Thread.currentThread().isInterrupted()) {
                    if (prevRate > memMetrics.getAllocationRate())
                        rateDropsCntr++;

                    prevRate = memMetrics.getAllocationRate();

                    sleep(delay);
                }
            }
            catch (InterruptedException ignore) {
                // No-op.
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
