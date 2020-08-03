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

package org.apache.ignite.spi.discovery.tcp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Client reconnect test in multi threaded mode while cache operations are in progress.
 */
public class IgniteClientReconnectMassiveShutdownTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 14;

    /** */
    private static final int CLIENT_GRID_CNT = 14;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(5_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    public void testMassiveServersShutdown1() throws Exception {
        massiveServersShutdown(StopType.FAIL_EVENT);
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    public void testMassiveServersShutdown2() throws Exception {
        massiveServersShutdown(StopType.SIMULATE_FAIL);
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    public void testMassiveServersShutdown3() throws Exception {
        massiveServersShutdown(StopType.CLOSE);
    }

    /**
     * @param stopType How to stop node.
     * @throws Exception If any error occurs.
     */
    private void massiveServersShutdown(final StopType stopType) throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        startClientGridsMultiThreaded(GRID_CNT, CLIENT_GRID_CNT);

        final AtomicBoolean done = new AtomicBoolean();

        // Starting a cache dynamically.
        Ignite client = grid(GRID_CNT);

        assertTrue(client.configuration().isClientMode());

        final CacheConfiguration<String, Integer> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setBackups(2);

        IgniteCache<String, Integer> cache = client.getOrCreateCache(cfg);

        assertNotNull(cache);

        HashMap<String, Integer> put = new HashMap<>();

        // Load some data.
        for (int i = 0; i < 10_000; i++)
            put.put(String.valueOf(i), i);

        cache.putAll(put);

        // Preparing client nodes and starting cache operations from them.
        final BlockingQueue<Integer> clientIdx = new LinkedBlockingQueue<>();

        for (int i = GRID_CNT; i < GRID_CNT + CLIENT_GRID_CNT; i++)
            clientIdx.add(i);

        final CountDownLatch latch = new CountDownLatch(CLIENT_GRID_CNT);

        IgniteInternalFuture<?> clientsFut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try {
                        int idx = clientIdx.take();

                        Ignite ignite = grid(idx);

                        Thread.currentThread().setName("client-thread-" + ignite.name());

                        assertTrue(ignite.configuration().isClientMode());

                        IgniteCache<String, Integer> cache = ignite.getOrCreateCache(cfg);

                        assertNotNull(cache);

                        IgniteTransactions txs = ignite.transactions();

                        Random rand = new Random();

                        latch.countDown();

                        IgniteFuture<?> retryFut = new IgniteFinishedFutureImpl<>();

                        while (!done.get()) {
                            try {
                                retryFut.get();
                            }
                            catch (IgniteException | CacheException e) {
                                retryFut = getRetryFuture(e);

                                continue;
                            }

                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                cache.put(String.valueOf(rand.nextInt(10_000)), rand.nextInt(50_000));

                                tx.commit();
                            }
                            catch (IgniteException | CacheException e) {
                                retryFut = getRetryFuture(e);
                            }
                        }

                        return null;
                    }
                    catch (Throwable e) {
                        log.error("Unexpected error: " + e, e);

                        throw e;
                    }
                }
            },
            CLIENT_GRID_CNT, "client-thread");

        try {
            if (!latch.await(30, SECONDS)) {
                log.warning("Failed to wait for for clients start.");

                U.dumpThreads(log);

                fail("Failed to wait for for clients start.");
            }

            // Killing a half of server nodes.
            final int srvsToKill = GRID_CNT / 2;

            final BlockingQueue<Integer> victims = new LinkedBlockingQueue<>();

            for (int i = 0; i < srvsToKill; i++)
                victims.add(i);

            final BlockingQueue<Integer> assassins = new LinkedBlockingQueue<>();

            for (int i = srvsToKill; i < GRID_CNT; i++)
                assassins.add(i);

            IgniteInternalFuture<?> srvsShutdownFut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        Thread.sleep(5_000);

                        Ignite assassin = grid(assassins.take());

                        assertFalse(assassin.configuration().isClientMode());

                        Ignite victim = grid(victims.take());

                        assertFalse(victim.configuration().isClientMode());

                        log.info("Kill node [node=" + victim.name() + ", from=" + assassin.name() + ']');

                        switch (stopType) {
                            case CLOSE:
                                victim.close();

                                break;

                            case FAIL_EVENT:
                                UUID nodeId = victim.cluster().localNode().id();

                                assassin.configuration().getDiscoverySpi().failNode(nodeId, null);

                                break;

                            case SIMULATE_FAIL:
                                ((TcpDiscoverySpi)victim.configuration().getDiscoverySpi()).simulateNodeFailure();

                                break;

                            default:
                                fail();
                        }

                        return null;
                    }
                },
                assassins.size(), "kill-thread");

            srvsShutdownFut.get();

            Thread.sleep(15_000);

            done.set(true);

            clientsFut.get();

            // Checks that failing servers was stopped after segmentation policy applying.
            if (stopType == StopType.FAIL_EVENT) {
                assertTrue("Servers was not stopped.", GridTestUtils.waitForCondition(() -> {
                    for (int i = 0; i < srvsToKill; i++) {
                        try {
                            grid(i);

                            return false;
                        }
                        catch (IgniteIllegalStateException ignored) {
                            // No-op.
                        }
                    }

                    return true;
                }, 15_000));
            }

            // Clean up ignite instance from static map in IgnitionEx.grids
            if (stopType == StopType.SIMULATE_FAIL) {
                for (int i = 0; i < srvsToKill; i++) {
                    grid(i).close();
                }
            }

            awaitPartitionMapExchange();

            List<Object> values = new ArrayList<>(GRID_CNT - srvsToKill);

            for (int k = 0; k < 10_000; k++) {
                String key = String.valueOf(k);

                Object val = cache.get(key);

                for (int i = srvsToKill; i < GRID_CNT; i++)
                    values.add(ignite(i).cache(DEFAULT_CACHE_NAME).get(key));

                for (Object val0 : values) {
                    if (val == null && val0 == null)
                        continue;

                    if (!val.equals(val0)) {
                        SB sb = new SB();

                        sb.a("\nExp:").a(val);

                        sb.a("\nActual:");

                        for (int i = 0; i < values.size(); i++) {
                            sb.a("[grid=")
                                .a(grid(srvsToKill + i).name())
                                .a(" val=")
                                .a(values.get(i))
                                .a("]\n");
                        }

                        fail(sb.toString());
                    }
                }

                values.clear();
            }
        }
        finally {
            done.set(true);
        }
    }

    /**
     * Gets retry or reconnect future if passed in {@code 'Exception'} has corresponding class in {@code 'cause'}
     * hierarchy.
     *
     * @param e {@code Exception}.
     * @return Internal future.
     * @throws Exception If unable to find retry or reconnect future.
     */
    private IgniteFuture<?> getRetryFuture(Exception e) throws Exception {
        if (X.hasCause(e, IgniteClientDisconnectedException.class)) {
            IgniteClientDisconnectedException cause = X.cause(e,
                IgniteClientDisconnectedException.class);

            assertNotNull(cause);

            return cause.reconnectFuture();
        }
        else if (X.hasCause(e, ClusterTopologyException.class)) {
            ClusterTopologyException cause = X.cause(e, ClusterTopologyException.class);

            assertNotNull(cause);

            return cause.retryReadyFuture();
        }
        else
            throw e;
    }

    /**
     *
     */
    enum StopType {
        /** */
        CLOSE,

        /** */
        SIMULATE_FAIL,

        /** */
        FAIL_EVENT
    }
}
