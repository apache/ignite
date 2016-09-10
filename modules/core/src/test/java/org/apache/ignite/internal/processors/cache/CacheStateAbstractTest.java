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
 *
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
@SuppressWarnings("TooBroadScope")
public abstract class CacheStateAbstractTest extends GridCommonAbstractTest {
    /** Entry count. */
    public static final int ENTRY_CNT = 5000;

    /** */
    public static final int GRID_CNT = 4;

    /** */
    private static final Collection<Class> forbidden = new GridConcurrentHashSet<>();

    /** */
    private boolean activeOnStart = true;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setActiveOnStart(activeOnStart);

        cfg.setCacheConfiguration(cacheConfiguration(null));

        if (client)
            cfg.setClientMode(true);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration(String cacheName);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        forbidden.clear();
    }

    /**
     * @throws Exception if failed.
     */
    public void testStartStopSimple() throws Exception {
        activeOnStart = false;

        forbidden.add(GridDhtPartitionsSingleMessage.class);
        forbidden.add(GridDhtPartitionsFullMessage.class);
        forbidden.add(GridDhtPartitionSupplyMessageV2.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        startGrids(GRID_CNT);

        forbidden.clear();

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testNoPartitionMapExchange() throws Exception {
        activeOnStart = false;

        forbidden.add(GridDhtPartitionsSingleMessage.class);
        forbidden.add(GridDhtPartitionsFullMessage.class);
        forbidden.add(GridDhtPartitionSupplyMessageV2.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        startGrids(GRID_CNT);

        checkInactive(GRID_CNT);

        forbidden.clear();

        grid(0).active(true);

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> cache = grid(0).cache(null);

        for (int k = 0; k < ENTRY_CNT; k++)
            cache.put(k, k);

        for (int g = 0; g < GRID_CNT; g++) {
            // Tests that state changes are propagated to existing and new nodes.
            assertTrue(grid(g).active());

            IgniteCache<Object, Object> cache0 = grid(g).cache(null);

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(k,  cache0.get(k));
        }

        // Check that new node startup and shutdown works fine after activation.
        startGrid(GRID_CNT);
        startGrid(GRID_CNT + 1);

        for (int g = 0; g < GRID_CNT + 2; g++) {
            IgniteCache<Object, Object> cache0 = grid(g).cache(null);

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(k,  cache0.get(k));
        }

        stopGrid(GRID_CNT + 1);

        for (int g = 0; g < GRID_CNT + 1; g++)
            grid(g).cache(null).rebalance().get();

        stopGrid(GRID_CNT);

        for (int g = 0; g < GRID_CNT; g++) {
            IgniteCache<Object, Object> cache0 = grid(g).cache(null);

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(k,  cache0.get(k));
        }

        grid(0).active(false);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int g = 0; g < GRID_CNT; g++) {
                    if (grid(g).active())
                        return false;
                }

                return true;
            }
        }, 5000);

        checkInactive(GRID_CNT);

        forbidden.add(GridDhtPartitionsSingleMessage.class);
        forbidden.add(GridDhtPartitionsFullMessage.class);
        forbidden.add(GridDhtPartitionSupplyMessageV2.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        // Should stop without exchange.
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testActivationFromClient() throws Exception {
        forbidden.add(GridDhtPartitionsSingleMessage.class);
        forbidden.add(GridDhtPartitionsFullMessage.class);
        forbidden.add(GridDhtPartitionSupplyMessageV2.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        activeOnStart = false;

        startGrids(GRID_CNT);

        client = true;

        startGrid(GRID_CNT + 1);

        checkInactive(GRID_CNT + 1);

        Ignite cl = grid(GRID_CNT);

        forbidden.clear();

        cl.active(true);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = cl.cache(null);

        for (int k = 0; k < ENTRY_CNT; k++)
            cache.put(k, k);

        for (int g = 0; g < GRID_CNT + 1; g++) {
            // Tests that state changes are propagated to existing and new nodes.
            assertTrue(grid(g).active());

            IgniteCache<Object, Object> cache0 = grid(g).cache(null);

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(k,  cache0.get(k));
        }

        cl.active(false);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int g = 0; g < GRID_CNT + 1; g++) {
                    if (grid(g).active())
                        return false;
                }

                return true;
            }
        }, 5000);

        checkInactive(GRID_CNT + 1);
    }

    /**
     * Tests that state doesn't change until all acquired locks are released.
     *
     * @throws Exception If fails.
     */
    public void testDeactivationWithPendingLock() throws Exception {
        startGrids(GRID_CNT);

        Lock lock = grid(0).cache(null).lock(1);

        IgniteInternalFuture<?> fut;

        lock.lock();

        try {
            fut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    grid(1).active(false);
                }
            }, 1);

            U.sleep(2000);

            assert !fut.isDone();

            for (int g = 0; g < GRID_CNT; g++)
                assertTrue(grid(g).active());
        }
        finally {
            lock.unlock();
        }

        fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        checkInactive(GRID_CNT);
    }

    /**
     * Tests that state doesn't change until all pending transactions are finished.
     *
     * @throws Exception If fails.
     */
    public void testDeactivationWithPendingTransaction() throws Exception {
        startGrids(GRID_CNT);

        final Ignite ignite0 = grid(0);

        final IgniteCache<Object, Object> cache0 = ignite0.cache(null);

        IgniteInternalFuture<?> fut;

        try (Transaction tx = ignite0.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            cache0.put(1, 1);

            fut = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    ignite0.active(false);
                }
            }, 1);

            U.sleep(2000);

            assert !fut.isDone();

            for (int g = 0; g < GRID_CNT; g++)
                assertTrue(grid(g).active());

            cache0.put(2, 2);

            tx.commit();
        }

        fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        checkInactive(GRID_CNT);

        ignite0.active(true);

        for (int g = 0; g < GRID_CNT; g++)
            assertEquals(2, grid(g).cache(null).get(2));
    }

    /**
     *
     */
    private void checkInactive(int cnt) {
        for (int g = 0; g < cnt; g++) {
            assertFalse(grid(g).active());

            final IgniteCache<Object, Object> cache0 = grid(g).cache(null);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return cache0.get("testKey");
                }
            }, CacheException.class, null);
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            checkForbidden((GridIoMessage)msg);

            super.sendMessage(node, msg, ackC);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            checkForbidden((GridIoMessage)msg);

            super.sendMessage(node, msg);
        }

        /**
         * @param msg Message to check.
         */
        private void checkForbidden(GridIoMessage msg) {
            if (forbidden.contains(msg.message().getClass()))
                throw new IgniteSpiException("Message is forbidden for this test: " + msg.message());
        }
    }
}
