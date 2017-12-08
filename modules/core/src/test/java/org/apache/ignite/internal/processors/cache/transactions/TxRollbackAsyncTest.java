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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static java.util.Collections.synchronizedList;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests an ability to async rollback near transactions.
 */
public class TxRollbackAsyncTest extends GridCommonAbstractTest {
    /** */
    public static final int ROLLBACK_TIMEOUT = 500;

    /** */
    private static final String CACHE_NAME = "test";

    /** */
    private static final String CACHE_NAME_2 = "test2";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    public static final int THREADS_CNT = Runtime.getRuntime().availableProcessors() * 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTransactionConfiguration(new TransactionConfiguration().
            setRollbackOnTopologyChangeTimeout(ROLLBACK_TIMEOUT));

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = "client".equals(igniteInstanceName);

        cfg.setClientMode(client);

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            if (nearCacheEnabled())
                ccfg.setNearConfiguration(new NearCacheConfiguration());

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(2);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * @return Near cache flag.
     */
    protected boolean nearCacheEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @return Started client.
     * @throws Exception If f nodeailed.
     */
    private Ignite startClient() throws Exception {
        Ignite client = startGrid("client");

        assertTrue(client.configuration().isClientMode());

        if (nearCacheEnabled())
            client.createNearCache(CACHE_NAME, new NearCacheConfiguration<>());
        else
            assertNotNull(client.cache(CACHE_NAME));

        return client;
    }

    /**
     *
     */
    public void testRollbackOnTopologyChange() throws Exception {
        final Ignite client = startClient();

        final AtomicInteger idx = new AtomicInteger();

        final CountDownLatch readStartLatch = new CountDownLatch(1);

        final CountDownLatch cacheStartLatch = new CountDownLatch(1);

        final IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                final int idx0 = idx.getAndIncrement();

                if (idx0 == 0) {
                    client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1);

                    client.cache(CACHE_NAME).put(0, 0); // Lock is owned.

                    readStartLatch.countDown();

                    U.awaitQuiet(cacheStartLatch);
                }
                else {
                    try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                        U.awaitQuiet(readStartLatch);

                        client.cache(CACHE_NAME).get(0); // Lock acquisition is queued.
                    }
                    catch (CacheException e) {
                        assertTrue(e.getMessage(), X.hasCause(e, TransactionTimeoutException.class));
                    }
                }
            }
        }, THREADS_CNT, "tx-async");

        final IgniteInternalFuture<?> fut2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(readStartLatch);

                // Trigger topology change event.
                final IgniteCache<Object, Object> cache = client.getOrCreateCache(new CacheConfiguration<>(CACHE_NAME_2));

                assertNotNull(cache);
            }
        }, 1, "top-change-async");

        fut2.get();

        cacheStartLatch.countDown();

        fut.get();

        assertNull(client.cache(CACHE_NAME).get(0));

        checkFutures();
    }

    /**
     *
     */
    public void testSimple() throws Exception {
        final Ignite client = startClient();

        testSimple0(client, 2);
    }

    /**
     *
     */
    private void testSimple0(final Ignite node, int threadsCnt) throws Exception {
        final AtomicInteger idx = new AtomicInteger();

        final CountDownLatch readStartLatch = new CountDownLatch(1);

        final CountDownLatch enqueueLatch = new CountDownLatch(threadsCnt - 1);

        final CountDownLatch commitLatch = new CountDownLatch(1);

        final List<Transaction> txs = synchronizedList(new ArrayList<Transaction>());

        final IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                final int idx0 = idx.getAndIncrement();

                if (idx0 == 0) {
                    try(Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)){
                        node.cache(CACHE_NAME).put(0, 0); // Lock is owned.

                        readStartLatch.countDown();

                        U.awaitQuiet(commitLatch);

                        tx.commit();
                    }
                }
                else {
                    try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                        txs.add(tx);

                        enqueueLatch.countDown();

                        U.awaitQuiet(readStartLatch);

                        node.cache(CACHE_NAME).get(0); // Lock acquisition is queued.
                    }
                    catch (CacheException e) {
                        assertTrue(e.getMessage(), X.hasCause(e, TransactionRollbackException.class));
                    }
                }
            }
        }, threadsCnt, "tx-async");

        U.awaitQuiet(enqueueLatch);

        final Transaction tx0 = txs.remove(0);

        tx0.rollback();

        commitLatch.countDown();

        fut.get();

        assertEquals(0, node.cache(CACHE_NAME).get(0));

        checkFutures();
    }

    public void testRollbackActiveTransactions() throws Exception {
        final Ignite client = startClient();

        final Collection<Transaction> transactions = client.transactions().localActiveTransactions();

        for (Transaction transaction : transactions)
            transaction.rollback();
    }

    /**
     * Checks if all tx futures are finished.
     */
    private void checkFutures() {
        for (Ignite ignite : G.allGrids()) {
            IgniteEx ig = (IgniteEx)ignite;

            final IgniteInternalFuture<?> f = ig.context().cache().context().
                partitionReleaseFuture(new AffinityTopologyVersion(G.allGrids().size() + 1, 0));

            assertTrue("Unexpected incomplete future: " + f, f.isDone());
        }
    }
}
