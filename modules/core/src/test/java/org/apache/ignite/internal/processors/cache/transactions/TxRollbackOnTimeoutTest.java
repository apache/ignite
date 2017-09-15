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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jsr166.ThreadLocalRandom8;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests an ability to eagerly rollback timed out transactions.
 */
public class TxRollbackOnTimeoutTest extends GridCommonAbstractTest {
    /** */
    private static final long TX_MIN_TIMEOUT = 1;

    /** */
    private static final long TX_TIMEOUT = 300;

    /** */
    private static final long TX_DEFAULT_TIMEOUT = 3_000;

    /** */
    private static final String CACHE_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    private final CountDownLatch blocked = new CountDownLatch(1);

    /** */
    private final CountDownLatch unblocked = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        TransactionConfiguration txCfg = new TransactionConfiguration();
        txCfg.setDefaultTxTimeout(TX_DEFAULT_TIMEOUT);

        cfg.setTransactionConfiguration(txCfg);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
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

    /** */
    protected void validateException(Exception e) {
        assertEquals("Deadlock report is expected",
            TransactionDeadlockException.class, e.getCause().getCause().getClass());
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout1() throws Exception {
        testWaitingTxUnblockedOnTimeout0(grid(0), grid(0));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout2() throws Exception {
        testWaitingTxUnblockedOnTimeout0(grid(0), grid(1));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout3() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnTimeout0(grid(0), client);
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout4() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnTimeout0(client, grid(0));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout5() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnTimeout0(client, client);
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout6() throws Exception {
        testWaitingTxUnblockedOnThreadDeath0(grid(0), grid(0));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout7() throws Exception {
        testWaitingTxUnblockedOnThreadDeath0(grid(0), grid(1));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout8() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnThreadDeath0(grid(0), client);
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout9() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnThreadDeath0(client, grid(0));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     *
     * @throws Exception If failed.
     */
    public void testWaitingTxUnblockedOnTimeout10() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnThreadDeath0(client, client);
    }

    /**
     * Tests if deadlock is resolved on timeout with correct message.
     *
     * @throws Exception If failed.
     */
    public void testDeadlockUnblockedOnTimeout1() throws Exception {
        testDeadlockUnblockedOnTimeout0(ignite(0), ignite(1));
    }

    /**
     * Tests if deadlock is resolved on timeout with correct message.
     *
     * @throws Exception If failed.
     */
    public void testDeadlockUnblockedOnTimeout2() throws Exception {
        testDeadlockUnblockedOnTimeout0(ignite(0), ignite(0));
    }

    /**
     * Tests if deadlock is resolved on timeout with correct message.
     *
     * @throws Exception If failed.
     */
    public void testDeadlockUnblockedOnTimeout3() throws Exception {
        Ignite client = startGrid("client");

        testDeadlockUnblockedOnTimeout0(ignite(0), client);
    }

    /**
     * Tests if deadlock is resolved on timeout with correct message.
     * @throws Exception
     */
    private void testDeadlockUnblockedOnTimeout0(final Ignite node1, final Ignite node2) throws Exception {
        final CountDownLatch l = new CountDownLatch(2);

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    try (Transaction tx = node1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, TX_TIMEOUT, 2)) {
                        node1.cache(CACHE_NAME).put(1, 1);

                        l.countDown();
                        U.awaitQuiet(l);

                        node1.cache(CACHE_NAME).putAll(Collections.singletonMap(2, 2));

                        tx.commit();

                        fail();
                    }
                } catch (CacheException e) {
                    // No-op.
                    validateException(e);
                }
            }
        }, 1, "First");

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = node2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 2)) {
                    node2.cache(CACHE_NAME).put(2, 2);

                    l.countDown();
                    U.awaitQuiet(l);

                    node2.cache(CACHE_NAME).put(1, 1);

                    tx.commit();
                }
            }
        }, 1, "Second");

        fut1.get();
        fut2.get();

        assertTrue(node1.cache(CACHE_NAME).containsKey(1));
        assertTrue(node1.cache(CACHE_NAME).containsKey(2));
    }

    /**
     * Tests timeout object cleanup on tx commit.
     *
     * @throws Exception If failed.
     */
    public void testTimeoutRemoval() throws Exception {
        IgniteEx client = (IgniteEx)startGrid("client");

        int modesCnt = 5;

        for (int i = 0; i < modesCnt; i++)
            testTimeoutRemoval0(grid(0), i, TX_TIMEOUT);

        for (int i = 0; i < modesCnt; i++)
            testTimeoutRemoval0(client, i, TX_TIMEOUT);

        for (int i = 0; i < modesCnt; i++)
            testTimeoutRemoval0(grid(0), i, TX_MIN_TIMEOUT);

        for (int i = 0; i < modesCnt; i++)
            testTimeoutRemoval0(client, i, TX_MIN_TIMEOUT);

        // Repeat with more iterations to make sure everything is cleared.
        for (int i = 0; i < 500; i++)
            testTimeoutRemoval0(client, ThreadLocalRandom8.current().nextInt(modesCnt), TX_MIN_TIMEOUT);
    }

    /**
     * Tests timeouts in all tx configurations.
     *
     * @throws Exception If failed.
     */
    public void testSimple() throws Exception {
        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values())
                testSimple0(concurrency, isolation);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     *
     * @throws Exception If failed.
     */
    private void testSimple0(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        Ignite near = grid(0);

        final int key = 1, val = 1;

        try (Transaction tx = near.transactions().txStart(concurrency, isolation, TX_TIMEOUT, 1)) {
            near.cache(CACHE_NAME).put(key, val);

            U.sleep(TX_TIMEOUT * 3);

            try {
                tx.commit();

                fail("Tx must timeout");
            } catch (IgniteException e) {
                assertTrue("Expected timeout exception", X.hasCause(e, TransactionTimeoutException.class));
            }
        }

        assertFalse("Must be removed by rollback on timeout", near.cache(CACHE_NAME).containsKey(key));
    }

    /**
     * @param near Node.
     * @param mode Test mode.
     *
     * @throws Exception If failed.
     */
    private void testTimeoutRemoval0(IgniteEx near, int mode, long timeout) throws Exception {
        Throwable saved = null;

        try (Transaction tx = near.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 1)) {
            near.cache(CACHE_NAME).put(1, 1);

            switch (mode) {
                case 0:
                    tx.commit();
                    break;

                case 1:
                    tx.commitAsync().get();
                    break;

                case 2:
                    tx.rollback();
                    break;

                case 3:
                    tx.rollbackAsync().get();
                    break;

                case 4:
                    break;

                default:
                    fail();
            }
        }
        catch (Throwable t) {
            saved = t;
        }

        GridConcurrentSkipListSet set = U.field(near.context().cache().context().time(), "timeoutObjs");

        for (Object obj : set)
            if (obj.getClass().isAssignableFrom(GridNearTxLocal.class)) {
                log.error("Last saved exception", saved);

                fail("Not remove for mode=" + mode + " and timeout=" + timeout);
            }
    }

    /**
     * @param near Node starting tx which is timed out.
     * @param other Node starting second tx.
     * @throws Exception If failed.
     */
    private void testWaitingTxUnblockedOnTimeout0(final Ignite near, final Ignite other) throws Exception {
        final int recordsCnt = 100;

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = near.transactions().txStart()) {
                    for (int i = 0; i < recordsCnt; i++)
                        near.cache(CACHE_NAME).put(i, i);

                    blocked.countDown();

                    // Will be unblocked after tx timeout occurs.
                    U.awaitQuiet(unblocked);

                    try {
                        near.cache(CACHE_NAME).put(0, 0);

                        fail();
                    }
                    catch (CacheException e) {
                        log.info("Expecting error: " + e.getMessage());
                    }

                    try {
                        tx.commit();

                        fail();
                    }
                    catch (IgniteException e) {
                        log.info("Expecting error: " + e.getMessage());
                    }
                }

                // Check thread is able to start new tx.
                try (Transaction tx = near.transactions().txStart()) {
                    for (int i = 0; i < recordsCnt; i++)
                        near.cache(CACHE_NAME).put(i, i);

                    tx.commit();
                }
            }
        }, 1, "First");

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(blocked);

                try (Transaction tx = other.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                    for (int i = 0; i < recordsCnt; i++)
                        other.cache(CACHE_NAME).put(i, i);

                    // Will wait until timeout on first tx will unblock put.
                    tx.commit();
                }
            }
        }, 1, "Second");

        fut2.get();

        unblocked.countDown();

        fut1.get();
    }

    /**
     * @param near Node starting tx which is timed out.
     * @param other Node starting second tx.
     * @throws Exception If failed.
     */
    private void testWaitingTxUnblockedOnThreadDeath0(final Ignite near, final Ignite other) throws Exception {
        final int recordsCnt = 100;

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                near.transactions().txStart();

                for (int i = 0; i < recordsCnt; i++)
                    near.cache(CACHE_NAME).put(i, i);

                blocked.countDown();

                throw new IgniteException("Failure");
            }
        }, 1, "First");

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(blocked);

                try (Transaction tx = other.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                    for (int i = 0; i < recordsCnt; i++)
                        other.cache(CACHE_NAME).put(i, i);

                    // Will wait until timeout on first tx will unblock put.
                    tx.commit();
                }
            }
        }, 1, "Second");

        try {
            fut1.get();

            fail();
        }
        catch (IgniteCheckedException e) {
            // No-op.
        }

        fut2.get();
    }

    /**
     * Returns root cause for an exception.
     * @param t Throwable.
     *
     * @return Root cause or input if none.
     */
    private static Throwable getRootCause(Throwable t) {
        Throwable cause;
        Throwable res = t;

        while(null != (cause = res.getCause()) && (res != cause) )
            res = cause;

        return res;
    }
}