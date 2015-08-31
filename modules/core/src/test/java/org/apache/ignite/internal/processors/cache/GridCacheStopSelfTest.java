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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests correct cache stopping.
 */
public class GridCacheStopSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-257");
    }

    /** */
    private static final String EXPECTED_MSG = "Grid is in invalid state to perform this operation. " +
        "It either not started yet or has already being or have stopped";

    /** */
    private boolean atomic;

    /** */
    private boolean replicated;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disc = new TcpDiscoverySpi();

        disc.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disc);

        CacheConfiguration ccfg  = new CacheConfiguration();

        ccfg.setCacheMode(replicated ? REPLICATED : PARTITIONED);

        ccfg.setAtomicityMode(atomic ? ATOMIC : TRANSACTIONAL);

        ccfg.setSwapEnabled(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopExplicitTransactions() throws Exception {
        testStop(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopImplicitTransactions() throws Exception {
        testStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopExplicitTransactionsReplicated() throws Exception {
        replicated = true;

        testStop(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopImplicitTransactionsReplicated() throws Exception {
        replicated = true;

        testStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopAtomic() throws Exception {
        atomic = true;

        testStop(false);
    }

    /**
     * @param startTx If {@code true} starts transactions.
     * @throws Exception If failed.
     */
    private void testStop(final boolean startTx) throws Exception {
        for (int i = 0; i < 10; i++) {
            startGrid(0);

            final int PUT_THREADS = 50;

            final CountDownLatch stopLatch = new CountDownLatch(1);

            final CountDownLatch readyLatch = new CountDownLatch(PUT_THREADS);

            final IgniteCache<Integer, Integer> cache = grid(0).cache(null);

            assertNotNull(cache);

            assertEquals(atomic ? ATOMIC : TRANSACTIONAL, cache.getConfiguration(CacheConfiguration.class).getAtomicityMode());
            assertEquals(replicated ? REPLICATED : PARTITIONED, cache.getConfiguration(CacheConfiguration.class).getCacheMode());

            Collection<IgniteInternalFuture<?>> putFuts = new ArrayList<>();

            for (int j = 0; j < PUT_THREADS; j++) {
                final int key = j;

                putFuts.add(GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        if (startTx) {
                            try (Transaction tx = grid(0).transactions().txStart()) {
                                cache.put(key, key);

                                readyLatch.countDown();

                                stopLatch.await();

                                tx.commit();
                            }
                        }
                        else {
                            readyLatch.countDown();

                            stopLatch.await();

                            cache.put(key, key);
                        }

                        return null;
                    }
                }));
            }

            readyLatch.await();

            stopLatch.countDown();

            stopGrid(0);

            for (IgniteInternalFuture<?> fut : putFuts) {
                try {
                    fut.get();
                }
                catch (IgniteCheckedException e) {
                    if (!e.getMessage().startsWith(EXPECTED_MSG))
                        e.printStackTrace();

                    assertTrue("Unexpected error message: " + e.getMessage(), e.getMessage().startsWith(EXPECTED_MSG));
                }
            }

            try {
                cache.put(1, 1);
            }
            catch (IllegalStateException e) {
                if (!e.getMessage().startsWith(EXPECTED_MSG))
                    e.printStackTrace();

                assertTrue("Unexpected error message: " + e.getMessage(), e.getMessage().startsWith(EXPECTED_MSG));
            }
        }
    }
}