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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteAtomicLongChangingTopologySelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 5;

    /** Restart count. */
    private static final int RESTART_CNT = 15;

    /** Atomic long name. */
    private static final String ATOMIC_LONG_NAME = "test-atomic-long";

    /** Queue. */
    private final Queue<Long> queue = new ConcurrentLinkedQueue<>();

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi).setNetworkTimeout(30_000);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();
        atomicCfg.setCacheMode(PARTITIONED);
        atomicCfg.setBackups(1);

        cfg.setAtomicConfiguration(atomicCfg);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        queue.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientAtomicLongCreateCloseFailover() throws Exception {
        testFailoverWithClient(new IgniteInClosure<Ignite>() {
            @Override public void apply(Ignite ignite) {
                for (int i = 0; i < 100; i++) {
                    IgniteAtomicLong l = ignite.atomicLong("long-" + 1, 0, true);

                    l.close();
                }
            }
        });
    }

    /**
     * @param c Test iteration closure.
     * @throws Exception If failed.
     */
    private void testFailoverWithClient(IgniteInClosure<Ignite> c) throws Exception {
        startGridsMultiThreaded(GRID_CNT, false);

        client = true;

        Ignite ignite = startGrid(GRID_CNT);

        assertTrue(ignite.configuration().isClientMode());

        client = false;

        final AtomicBoolean finished = new AtomicBoolean();

        IgniteInternalFuture<?> fut = restartThread(finished);

        long stop = System.currentTimeMillis() + 60_000;

        try {
            int iter = 0;

            while (System.currentTimeMillis() < stop) {
                log.info("Iteration: " + iter++);

                try {
                    c.apply(ignite);
                }
                catch (IgniteClientDisconnectedException e) {
                    e.reconnectFuture().get();
                }
            }

            finished.set(true);

            fut.get();
        }
        finally {
            finished.set(true);
        }
    }

    /**
     * @param finished Finished flag.
     * @return Future.
     */
    private IgniteInternalFuture<?> restartThread(final AtomicBoolean finished) {
        return GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    for (int i = 0; i < GRID_CNT; i++) {
                        log.info("Stop node: " + i);

                        stopGrid(i);

                        U.sleep(500);

                        log.info("Start node: " + i);

                        startGrid(i);

                        if (finished.get())
                            break;
                    }
                }

                return null;
            }
        }, "restart-thread");
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
     * @param startLatch Thread start latch.
     * @param run Run flag.
     * @throws Exception If failed.
     * @return Threads future.
     */
    private IgniteInternalFuture<?> startNodeAndCreaterThread(final int i,
        final CountDownLatch startLatch,
        final AtomicBoolean run)
        throws Exception {
        return multithreadedAsync(new Runnable() {
            @Override public void run() {
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
