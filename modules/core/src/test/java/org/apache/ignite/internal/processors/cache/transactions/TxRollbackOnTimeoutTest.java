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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Tests an ability to eagerly rollback timed out transactions.
 */
public class TxRollbackOnTimeoutTest extends GridCommonAbstractTest {
    /** */
    private static final long TX_TIMEOUT = 3_000L;

    /** */
    private static final String CACHE_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private final CountDownLatch blocked = new CountDownLatch(1);

    /** */
    private CountDownLatch unblocked = new CountDownLatch(1);

    /** */
    private static int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        TransactionConfiguration txCfg = new TransactionConfiguration();
        txCfg.setDefaultTxTimeout(TX_TIMEOUT);

        cfg.setTransactionConfiguration(txCfg);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
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

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout1() throws Exception {
        testWaitingTxUnblockedOnTimeout0(grid(0), grid(0));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout2() throws Exception {
        testWaitingTxUnblockedOnTimeout0(grid(0), grid(1));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout3() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnTimeout0(grid(0), client);
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout4() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnTimeout0(client, grid(0));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout5() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnTimeout0(client, client);
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout6() throws Exception {
        testWaitingTxUnblockedOnThreadDeath0(grid(0), grid(0));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout7() throws Exception {
        testWaitingTxUnblockedOnThreadDeath0(grid(0), grid(1));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout8() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnThreadDeath0(grid(0), client);
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout9() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnThreadDeath0(client, grid(0));
    }

    /**
     * Tests if timeout on first tx unblocks second tx waiting for the locked key.
     */
    public void testWaitingTxUnblockedOnTimeout10() throws Exception {
        Ignite client = startGrid("client");

        testWaitingTxUnblockedOnThreadDeath0(client, client);
    }

    /**
     * Tests timeout object cleanup on tx commit.
     */
    public void testTimeoutRemovalOnCommit() throws Exception {
        testTimeoutRemoval(grid(0), true);
    }

    /**
     * Tests timeout object cleanup on tx rollback.
     */
    public void testTimeoutRemovalOnRollback() throws Exception {
        testTimeoutRemoval(grid(0), false);
    }

    /** */
    private void testTimeoutRemoval(IgniteEx near, boolean commit) throws Exception {
        GridTimeoutProcessor timeProc = near.context().cache().context().time();

        try (Transaction tx = near.transactions().txStart()) {
            near.cache(CACHE_NAME).put(1, 1);

            if (commit)
                tx.commit();
        }

        GridConcurrentSkipListSet set = U.field(timeProc, "timeoutObjs");

        for (Object obj : set)
            assertFalse(obj.getClass().isAssignableFrom(GridNearTxLocal.class));
    }

    /** */
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
                        tx.commit();

                        fail();
                    }
                    catch (IgniteException e) {
                        log.info("Expecting error: " + e.getMessage());
                    }
                }
            }
        }, 1, "First");

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Runnable() {
            @Override public void run() {
                U.awaitQuiet(blocked);

                try (Transaction tx = other.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                    TransactionIsolation.REPEATABLE_READ, 0, 1)) {
                    for (int i = 0; i < recordsCnt; i++)
                        other.cache(CACHE_NAME).put(i, i);

                    // Will wait until timeout on first tx will unblock put.
                    tx.commit();
                }
            }
        }, 2, "Second");

        fut2.get(5, TimeUnit.SECONDS);

        unblocked.countDown();

        fut1.get(5, TimeUnit.SECONDS);
    }

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

                try (Transaction tx = other.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                    TransactionIsolation.REPEATABLE_READ, 0, 1)) {
                    for (int i = 0; i < recordsCnt; i++)
                        other.cache(CACHE_NAME).put(i, i);

                    // Will wait until timeout on first tx will unblock put.
                    tx.commit();
                }
            }
        }, 2, "Second");

        try {
            fut1.get();

            fail();
        }
        catch (IgniteCheckedException e) {
            // No-op.
        }

        fut2.get(5, TimeUnit.SECONDS);
    }
}