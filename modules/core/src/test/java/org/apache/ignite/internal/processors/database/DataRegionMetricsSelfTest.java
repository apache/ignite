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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataRegionMetricsProvider;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.Test;

import static java.lang.Thread.sleep;

/**
 *
 */
public class DataRegionMetricsSelfTest extends GridCommonAbstractTest {
    /** For test purposes only. */
    public static final DataRegionMetricsProvider NO_OP_METRICS = new DataRegionMetricsProvider() {
        /** {@inheritDoc} */
        @Override public long partiallyFilledPagesFreeSpace() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long emptyDataPages() {
            return 0;
        }
    };

    /** */
    private DataRegionMetricsImpl memMetrics;

    /** */
    private int threadsCnt = 1;

    /** */
    private Thread[] allocationThreads;

    /** */
    private Thread watcherThread;

    /** */
    private static final int RATE_TIME_INTERVAL_1 = 5_000;

    /** */
    private static final int RATE_TIME_INTERVAL_2 = 10_000;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration();

        IgniteConfiguration cfg = new IgniteConfiguration().setMetricExporterSpi(new NoopMetricExporterSpi());

        memMetrics = new DataRegionMetricsImpl(plcCfg,
            new GridMetricManager(new GridTestKernalContext(new GridTestLog4jLogger(), cfg)),
            NO_OP_METRICS);

        memMetrics.enableMetrics();
    }

    /**
     * Test for allocationRate metric in single-threaded mode.
     * @throws Exception if any happens during test.
     */
    @Test
    public void testAllocationRateSingleThreaded() throws Exception {
        threadsCnt = 1;
        memMetrics.rateTimeInterval(RATE_TIME_INTERVAL_2);

        CountDownLatch startLatch = new CountDownLatch(1);

        startAllocationThreads(startLatch, 340, 50);
        AllocationRateWatcher watcher = startWatcherThread(startLatch, 20);

        alignWithTimeInterval(RATE_TIME_INTERVAL_2, 5);

        startLatch.countDown();

        joinAllThreads();

        assertTrue("Expected rate drops count > 3 and < 6 but actual is " + watcher.rateDropsCntr.get(),
            watcher.rateDropsCntr.get() > 3 && watcher.rateDropsCntr.get() < 6);
    }

    /**
     * Test for allocationRate metric in multi-threaded mode with short silent period in the middle of the test.
     * @throws Exception if any happens during test.
     */
    @Test
    public void testAllocationRateMultiThreaded() throws Exception {
        threadsCnt = 4;
        memMetrics.rateTimeInterval(RATE_TIME_INTERVAL_1);

        CountDownLatch startLatch = new CountDownLatch(1);

        startAllocationThreads(startLatch, 7_800, 1);

        AllocationRateWatcher watcher = startWatcherThread(startLatch, 20);

        alignWithTimeInterval(RATE_TIME_INTERVAL_1, 5);

        startLatch.countDown();

        joinAllocationThreads();

        assertTrue("4 or 5 rate drops must be observed: " + watcher.rateDropsCntr,
            watcher.rateDropsCntr.get() == 4 || watcher.rateDropsCntr.get() == 5);

        sleep(3);

        threadsCnt = 8;

        CountDownLatch restartLatch = new CountDownLatch(1);

        startAllocationThreads(restartLatch, 8_000, 1);

        restartLatch.countDown();

        joinAllThreads();

        assertTrue("Expected rate drops count > 4 but actual is " + watcher.rateDropsCntr.get(),
            watcher.rateDropsCntr.get() > 4);
    }

    /**
     * Test verifies that allocationRate calculation algorithm survives setting new values to rateTimeInterval parameter.
     * @throws Exception if any happens during test.
     */
    @Test
    public void testAllocationRateTimeIntervalConcurrentChange() throws Exception {
        threadsCnt = 5;
        memMetrics.rateTimeInterval(RATE_TIME_INTERVAL_1);

        CountDownLatch startLatch = new CountDownLatch(1);

        startAllocationThreads(startLatch, 10_000, 1);

        AllocationRateWatcher watcher = startWatcherThread(startLatch, 20);

        alignWithTimeInterval(RATE_TIME_INTERVAL_1, 5);

        startLatch.countDown();

        for (int i = 0; i < 10; i++) {
            Thread.sleep(25);

            memMetrics.rateTimeInterval(((2 + i * 5) % 3 + 1) * 1000);
        }

        joinAllThreads();

        assertTrue("Expected rate drops count > 4 but actual is " + watcher.rateDropsCntr.get(),
            watcher.rateDropsCntr.get() > 4);
    }

    /**
     *
     * @throws Exception if any happens during test.
     */
    @Test
    public void testAllocationRateSubintervalsConcurrentChange() throws Exception {
        threadsCnt = 5;
        memMetrics.rateTimeInterval(RATE_TIME_INTERVAL_1);

        CountDownLatch startLatch = new CountDownLatch(1);

        startAllocationThreads(startLatch, 10_000, 1);

        AllocationRateWatcher watcher = startWatcherThread(startLatch, 20);

        alignWithTimeInterval(RATE_TIME_INTERVAL_1, 5);

        startLatch.countDown();

        for (int i = 0; i < 10; i++) {
            Thread.sleep(25);

            memMetrics.subIntervals((2 + i * 5) % 3 + 2);
        }

        joinAllThreads();

        assertTrue("Expected rate drops count > 4 but actual is " + watcher.rateDropsCntr.get(),
            watcher.rateDropsCntr.get() > 4);
    }

    /**
     * As rate metrics {@link HitRateMetric implementation} is tied to absolute time ticks
     * (not related to the first hit) all tests need to align start time with this sequence of ticks.
     *
     * @param rateTimeInterval Rate time interval.
     * @param size Size.
     */
    private void alignWithTimeInterval(int rateTimeInterval, int size) throws InterruptedException {
        int subIntervalLength = rateTimeInterval / size;

        long subIntCurTime = System.currentTimeMillis() % subIntervalLength;

        Thread.sleep(subIntervalLength - subIntCurTime);
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
        private final DataRegionMetricsImpl memMetrics;

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
        private AllocationsIncrementer(CountDownLatch startLatch, DataRegionMetricsImpl memMetrics, int iterationsCnt, int delay) {
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
                    memMetrics.totalAllocatedPages().increment();

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
        private final AtomicInteger rateDropsCntr = new AtomicInteger();

        /** */
        private final CountDownLatch startLatch;

        /** */
        private final DataRegionMetrics memMetrics;

        /** */
        private final int delay;

        /**
         * @param startLatch Start latch.
         * @param memMetrics Mem metrics.
         * @param delay Delay.
         */
        private AllocationRateWatcher(CountDownLatch startLatch, DataRegionMetrics memMetrics, int delay) {
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
                        rateDropsCntr.incrementAndGet();

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
