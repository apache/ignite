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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jsr166.LongAdder8;

import static java.lang.Thread.sleep;
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
    public void testNormalRollbacks() throws Exception {
        final Ignite client = startClient();

        // Normal rollback after put.
        Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1);

        client.cache(CACHE_NAME).put(0, 0); // Lock is owned.

        tx.rollback();

        assertNull(client.cache(CACHE_NAME).get(0));

        // Normal rollback before put.
        tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1);

        tx.rollback();

        client.cache(CACHE_NAME).put(0, 1);

        assertEquals(1, client.cache(CACHE_NAME).get(0));

        checkFutures();
    }

    /**
     *
     */
    public void testAsyncRollbacks() throws Exception {
        final Ignite client = startClient();

        testAsyncRollbacks0(client, 2);
    }

    /**
     *
     */
    public void testRollbackEmptyTx() throws Exception {
        final Ignite client = startClient();

        final int threadsCnt = 1;

        final CountDownLatch lockedLatch = new CountDownLatch(1);

        final CountDownLatch enqueueLatch = new CountDownLatch(threadsCnt);

        final CountDownLatch commitLatch = new CountDownLatch(1);

        final CountDownLatch rollbackLatch = new CountDownLatch(1);

        final List<Transaction> txs = synchronizedList(new ArrayList<Transaction>());

        IgniteInternalFuture<?> lockFut = startLockThread(client, lockedLatch, commitLatch, 0);

        final IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1);
                txs.add(tx);

                enqueueLatch.countDown();

                U.awaitQuiet(lockedLatch);
                U.awaitQuiet(rollbackLatch);

                try {
                    client.cache(CACHE_NAME).get(0);

                    fail("Op must fail");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage(), X.hasCause(e, TransactionRollbackException.class));
                }
            }
        }, threadsCnt, "tx-async");

        U.awaitQuiet(enqueueLatch);

        txs.get(0).rollback();

        rollbackLatch.countDown();

        commitLatch.countDown();

        lockFut.get();

        fut.get();

        checkFutures();
    }

    /**
     *
     */
    private void testAsyncRollbacks0(final Ignite node, int threadsCnt) throws Exception {
        final CountDownLatch readStartLatch = new CountDownLatch(1);

        final CountDownLatch enqueueLatch = new CountDownLatch(threadsCnt);

        final CountDownLatch commitLatch = new CountDownLatch(1);

        final List<Transaction> txs = synchronizedList(new ArrayList<Transaction>());

        IgniteInternalFuture<?> lockFut = startLockThread(node, readStartLatch, commitLatch, 0);

        final IgniteInternalFuture<?> testFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1);
                txs.add(tx);

                enqueueLatch.countDown();

                U.awaitQuiet(readStartLatch);

                try {
                    node.cache(CACHE_NAME).get(0); // Try own the lock.

                    fail("Op must rollback");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage(), X.hasCause(e, TransactionRollbackException.class));
                }

                try {
                    node.cache(CACHE_NAME).put(0, 1);

                    fail("Op must fail");
                }
                catch (Exception e) {
                    assertTrue(e.getMessage(), X.hasCause(e, TransactionRollbackException.class));
                }
            }
        }, threadsCnt, "tx-async");

        U.awaitQuiet(enqueueLatch);

        Thread.sleep(500);

        List<IgniteFuture<Void>> futs = new ArrayList<>();

        for (Transaction tx : txs)
            futs.add(tx.rollbackAsync());

        for (IgniteFuture<Void> future : futs)
            future.get();

        testFut.get();

        commitLatch.countDown();

        lockFut.get();

        assertEquals(0, node.cache(CACHE_NAME).get(0));

        checkFutures();
    }

    /**
     *
     */
    public void testMixedRollbackTypes() throws Exception {
        final Ignite client = startClient();

        final AtomicBoolean stop = new AtomicBoolean();

        final int threadsCnt = Runtime.getRuntime().availableProcessors() * 2;

        final int keysCnt = 100;

        final Random r = new Random();

        final TransactionConcurrency[] TC_VALS = TransactionConcurrency.values();
        final TransactionIsolation[] TI_VALS = TransactionIsolation.values();

        final LongAdder8 completed = new LongAdder8();
        final LongAdder8 failed = new LongAdder8();

        IgniteInternalFuture<?> txFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while(!stop.get()) {

                    int nodeId = r.nextInt(GRID_CNT + 1);

                    Ignite node = nodeId == GRID_CNT || nearCacheEnabled() ? client : grid(nodeId);

                    TransactionConcurrency conc = TC_VALS[r.nextInt(TC_VALS.length)];
                    TransactionIsolation isolation = TI_VALS[r.nextInt(TI_VALS.length)];

                    int k = r.nextInt(keysCnt);

                    long timeout = r.nextInt(200) + 50;

                    try (Transaction tx = node.transactions().txStart(conc, isolation, timeout, 1)) {
                        final Long v = (Long)node.cache(CACHE_NAME).get(k);

                        final int delay = r.nextInt(400);

                        if (delay > 0)
                            sleep(delay);

                        node.cache(CACHE_NAME).put(k, v + 1);

                        tx.commit();

                        completed.add(1);
                    }
                    catch (Exception e) {
                        failed.add(1);
                    }
                }
            }
        }, threadsCnt, "tx-thread");

        final AtomicInteger nodeIdx = new AtomicInteger();

        IgniteInternalFuture<?> rollbackFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int nodeId = nodeIdx.getAndIncrement();

                while (!stop.get()) {
                    try {
                        sleep(r.nextInt(350) + 50);
                    }
                    catch (InterruptedException e) {
                        return;
                    }

                    Collection<Transaction> transactions = grid(nodeId).transactions().localActiveTransactions();

                    for (Transaction transaction : transactions)
                        transaction.rollbackAsync();
                }
            }
        }, G.allGrids().size(), "rollback-thread");

        doSleep(10_000);

        stop.set(true);

        txFut.get();

        rollbackFut.get();

        log.info("Completed txs: " + completed.sum() + ", failed txs: " + failed.sum());

        checkFutures();
    }

    /**
     * Tests rollback of active local transactions.
     */
    public void testRollbackActiveTransactions() throws Exception {
        final Ignite client = startClient();

        final Collection<Transaction> transactions = client.transactions().localActiveTransactions();

        for (Transaction transaction : transactions)
            transaction.rollback();

        assertTrue(client.transactions().localActiveTransactions().isEmpty());
    }

    /**
     * @param node Node.
     * @param lockedLatch Locked latch.
     * @param commitLatch Commit latch.
     * @param timeout Timeout.
     */
    private IgniteInternalFuture<?> startLockThread(final Ignite node, final CountDownLatch lockedLatch,
        final CountDownLatch commitLatch, final int timeout) throws Exception {
        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 1)){
                    node.cache(CACHE_NAME).put(0, 0); // Own the lock.

                    lockedLatch.countDown();

                    U.awaitQuiet(commitLatch);

                    tx.commit();
                }
            }
        }, 1, "tx-lock-thread");
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
