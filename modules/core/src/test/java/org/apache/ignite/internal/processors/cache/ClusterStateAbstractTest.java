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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 *
 */
public abstract class ClusterStateAbstractTest extends GridCommonAbstractTest {
    /** Entry count. */
    public static final int ENTRY_CNT = 5000;

    /** */
    public static final int GRID_CNT = 4;

    /** */
    private static final String CACHE_NAME = "cache1";

    /** */
    private static final Collection<Class> forbidden = new GridConcurrentHashSet<>();

    /** */
    private static AtomicReference<Exception> errEncountered = new AtomicReference<>();

    /** */
    private boolean activeOnStart = true;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setActiveOnStart(activeOnStart);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME));

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

        Exception err = errEncountered.getAndSet(null);

        if (err != null)
            throw err;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testDynamicCacheStart() throws Exception {
        activeOnStart = false;

        forbidden.add(GridDhtPartitionSupplyMessage.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        startGrids(GRID_CNT);

        checkInactive(GRID_CNT);

        forbidden.clear();

        grid(0).cluster().active(true);

        IgniteCache<Object, Object> cache2 = grid(0).createCache(new CacheConfiguration<>("cache2"));

        for (int k = 0; k < ENTRY_CNT; k++)
            cache2.put(k, k);

        grid(0).cluster().active(false);

        checkInactive(GRID_CNT);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testNoRebalancing() throws Exception {
        activeOnStart = false;

        forbidden.add(GridDhtPartitionSupplyMessage.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        startGrids(GRID_CNT);

        checkInactive(GRID_CNT);

        forbidden.clear();

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();

        final IgniteCache<Object, Object> cache = grid(0).cache(CACHE_NAME);

        for (int k = 0; k < ENTRY_CNT; k++)
            cache.put(k, k);

        for (int g = 0; g < GRID_CNT; g++) {
            // Tests that state changes are propagated to existing and new nodes.
            assertTrue(grid(g).cluster().active());

            IgniteCache<Object, Object> cache0 = grid(g).cache(CACHE_NAME);

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(k,  cache0.get(k));
        }

        // Check that new node startup and shutdown works fine after activation.
        startGrid(GRID_CNT);
        startGrid(GRID_CNT + 1);

        for (int g = 0; g < GRID_CNT + 2; g++) {
            IgniteCache<Object, Object> cache0 = grid(g).cache(CACHE_NAME);

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals("Failed for [grid=" + g + ", key=" + k + ']', k, cache0.get(k));
        }

        stopGrid(GRID_CNT + 1);

        for (int g = 0; g < GRID_CNT + 1; g++)
            grid(g).cache(CACHE_NAME).rebalance().get();

        stopGrid(GRID_CNT);

        for (int g = 0; g < GRID_CNT; g++) {
            IgniteCache<Object, Object> cache0 = grid(g).cache(CACHE_NAME);

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(k,  cache0.get(k));
        }

        grid(0).cluster().active(false);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int g = 0; g < GRID_CNT; g++) {
                    if (grid(g).cluster().active())
                        return false;
                }

                return true;
            }
        }, 5000);

        checkInactive(GRID_CNT);

        forbidden.add(GridDhtPartitionSupplyMessage.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        // Should stop without exchange.
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testActivationFromClient() throws Exception {
        forbidden.add(GridDhtPartitionSupplyMessage.class);
        forbidden.add(GridDhtPartitionDemandMessage.class);

        activeOnStart = false;

        startGrids(GRID_CNT);

        client = true;

        startGrid(GRID_CNT);

        checkInactive(GRID_CNT + 1);

        Ignite cl = grid(GRID_CNT);

        forbidden.clear();

        cl.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = cl.cache(CACHE_NAME);

        for (int k = 0; k < ENTRY_CNT; k++)
            cache.put(k, k);

        for (int g = 0; g < GRID_CNT + 1; g++) {
            // Tests that state changes are propagated to existing and new nodes.
            assertTrue(grid(g).cluster().active());

            IgniteCache<Object, Object> cache0 = grid(g).cache(CACHE_NAME);

            for (int k = 0; k < ENTRY_CNT; k++)
                assertEquals(k,  cache0.get(k));
        }

        cl.cluster().active(false);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int g = 0; g < GRID_CNT + 1; g++) {
                    if (grid(g).cluster().active())
                        return false;
                }

                return true;
            }
        }, 5000);

        checkInactive(GRID_CNT + 1);
    }

    /**
     * Tests that deactivation is prohibited if explicit lock is held in current thread.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testDeactivationWithPendingLock() throws Exception {
        startGrids(GRID_CNT);

        Lock lock = grid(0).cache(CACHE_NAME).lock(1);

        lock.lock();

        try {
            //noinspection ThrowableNotThrown
            GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                @Override public Object call() {
                    grid(0).cluster().active(false);

                    return null;
                }
            }, IgniteException.class,
                "Failed to deactivate cluster (must invoke the method outside of an active transaction).");
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Tests that deactivation is prohibited if transaction is active in current thread.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivationWithPendingTransaction() throws Exception {
        startGrids(GRID_CNT);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values())
                deactivateWithPendingTransaction(concurrency, isolation);
        }
    }

    /**
     */
    private void deactivateWithPendingTransaction(TransactionConcurrency concurrency,
        TransactionIsolation isolation) {
        final Ignite ignite0 = grid(0);

        final IgniteCache<Object, Object> cache0 = ignite0.cache(CACHE_NAME);

        try (Transaction ignore = ignite0.transactions().txStart(concurrency, isolation)) {
            cache0.put(1, "1");

            //noinspection ThrowableNotThrown
            GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                @Override public Object call() {
                    grid(0).cluster().active(false);

                    return null;
                }
            }, IgniteException.class,
                "Failed to deactivate cluster (must invoke the method outside of an active transaction).");
        }

        assertNull(cache0.get(1));
        assertNull(ignite0.transactions().tx());
    }

    /**
     *
     */
    private void checkInactive(int cnt) {
        for (int g = 0; g < cnt; g++)
            assertFalse(grid(g).cluster().active());
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
            if (forbidden.contains(msg.message().getClass())) {
                IgniteSpiException err = new IgniteSpiException("Message is forbidden for this test: " + msg.message());

                // Set error in case if this exception is not visible to the user code.
                errEncountered.compareAndSet(null, err);

                throw err;
            }
        }
    }
}
