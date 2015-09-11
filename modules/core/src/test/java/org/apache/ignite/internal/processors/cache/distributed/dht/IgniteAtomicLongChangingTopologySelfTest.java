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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class IgniteAtomicLongChangingTopologySelfTest extends GridCommonAbstractTest {
    /**
     * Grid count.
     */
    private static final int GRID_CNT = 5;

    /**
     * Restart count.
     */
    private static final int RESTART_CNT = 15;

    /**
     * Atomic long name.
     */
    private static final String ATOMIC_LONG_NAME = "test-atomic-long";

    /**
     * Queue.
     */
    private final Queue<Long> queue = new ConcurrentLinkedQueue<>();

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();
        atomicCfg.setCacheMode(CacheMode.PARTITIONED);
        atomicCfg.setBackups(1);

        cfg.setAtomicConfiguration(atomicCfg);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();

        queue.clear();
    }

    /**
     *
     */
    public void testQueueCreateNodesJoin() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(GRID_CNT);
        final AtomicBoolean run = new AtomicBoolean(true);

        Collection<IgniteInternalFuture<?>> futs = new ArrayList<>();

        for (int i = 0; i < GRID_CNT; i++)
            futs.add(startNodeAndCreaterThread(i, startLatch, run));

        startLatch.await();

        info("All nodes started.");

        Thread.sleep(10_000);

        run.set(false);

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();

        info("Increments: " + queue.size());

        assert !queue.isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementConsistency() throws Exception {
        startGrids(GRID_CNT);

        final AtomicBoolean run = new AtomicBoolean(true);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            /** {@inheritDoc} */
            @Override
            public Void call() throws Exception {
                IgniteAtomicLong cntr = ignite(0).atomicLong(ATOMIC_LONG_NAME, 0, true);

                while (run.get())
                    queue.add(cntr.getAndIncrement());

                return null;
            }
        }, 4, "increment-runner");

        for (int i = 0; i < RESTART_CNT; i++) {
            int restartIdx = ThreadLocalRandom.current().nextInt(GRID_CNT - 1) + 1;

            stopGrid(restartIdx);

            U.sleep(500);

            startGrid(restartIdx);
        }

        run.set(false);

        fut.get();

        info("Increments: " + queue.size());

        checkQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueueClose() throws Exception {
        startGrids(GRID_CNT);

        int threads = 4;

        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicInteger idx = new AtomicInteger();
        final AtomicReferenceArray<Exception> arr = new AtomicReferenceArray<>(threads);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            /** {@inheritDoc} */
            @Override
            public Void call() throws Exception {
                int base = idx.getAndIncrement();

                try {
                    int delta = 0;

                    while (run.get()) {
                        IgniteAtomicLong cntr = ignite(0).atomicLong(ATOMIC_LONG_NAME + "-" + base + "-" + delta, 0, true);

                        for (int i = 0; i < 5; i++)
                            queue.add(cntr.getAndIncrement());

                        cntr.close();

                        delta++;
                    }
                }
                catch (Exception e) {
                    arr.set(base, e);

                    throw e;
                }
                finally {
                    info("RUNNER THREAD IS STOPPING");
                }

                return null;
            }
        }, threads, "increment-runner");

        for (int i = 0; i < RESTART_CNT; i++) {
            int restartIdx = ThreadLocalRandom.current().nextInt(GRID_CNT - 1) + 1;

            stopGrid(restartIdx);

            U.sleep(500);

            startGrid(restartIdx);
        }

        run.set(false);

        fut.get();

        for (int i = 0; i < threads; i++) {
            Exception err = arr.get(i);

            if (err != null)
                throw err;
        }
    }

    /**
     *
     */
    private void checkQueue() {
        List<Long> list = new ArrayList<>(queue);

        Collections.sort(list);

        boolean failed = false;

        int delta = 0;

        for (int i = 0; i < list.size(); i++) {
            Long exp = (long)(i + delta);

            Long actual = list.get(i);

            if (!exp.equals(actual)) {
                failed = true;

                delta++;

                info(">>> Expected " + exp + ", actual " + actual);
            }
        }

        assertFalse(failed);
    }

    /**
     * @param i Node index.
     */
    private IgniteInternalFuture<?> startNodeAndCreaterThread(final int i, final CountDownLatch startLatch, final AtomicBoolean run)
        throws Exception {
        return multithreadedAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    Ignite ignite = startGrid(i);

                    startLatch.countDown();

                    while (run.get()) {
                        IgniteAtomicLong cntr = ignite.atomicLong(ATOMIC_LONG_NAME, 0, true);

                        queue.add(cntr.getAndIncrement());
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 1, "grunner-" + i);
    }
}