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

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;

import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class SetTxTimeoutOnPartitionMapExchangeTest extends GridCommonAbstractTest {
    /** Wait condition timeout. */
    private static final long WAIT_CONDITION_TIMEOUT = 10_000L;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     *
     */
    @Test
    public void testDefaultTxTimeoutOnPartitionMapExchange() throws Exception {
        IgniteEx ig1 = startGrid(1);
        IgniteEx ig2 = startGrid(2);

        TransactionConfiguration txCfg1 = ig1.configuration().getTransactionConfiguration();
        TransactionConfiguration txCfg2 = ig2.configuration().getTransactionConfiguration();

        final long expDfltTimeout = TransactionConfiguration.TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE;

        assertEquals(expDfltTimeout, txCfg1.getTxTimeoutOnPartitionMapExchange());
        assertEquals(expDfltTimeout, txCfg2.getTxTimeoutOnPartitionMapExchange());
    }

    /**
     *
     */
    @Test
    public void testJmxSetTxTimeoutOnPartitionMapExchange() throws Exception {
        startGrid(1);
        startGrid(2);

        TransactionsMXBean mxBean1 = txMXBean(1);
        TransactionsMXBean mxBean2 = txMXBean(2);

        final long expTimeout1 = 20_000L;
        final long expTimeout2 = 30_000L;

        mxBean1.setTxTimeoutOnPartitionMapExchange(expTimeout1);
        assertTxTimeoutOnPartitionMapExchange(expTimeout1);
        assertEquals(expTimeout1, mxBean1.getTxTimeoutOnPartitionMapExchange());

        mxBean2.setTxTimeoutOnPartitionMapExchange(expTimeout2);
        assertTxTimeoutOnPartitionMapExchange(expTimeout2);
        assertEquals(expTimeout2, mxBean2.getTxTimeoutOnPartitionMapExchange());
    }

    /**
     *
     */
    @Test
    public void testClusterSetTxTimeoutOnPartitionMapExchange() throws Exception {
        Ignite ig1 = startGrid(1);
        Ignite ig2 = startGrid(2);

        final long expTimeout1 = 20_000L;
        final long expTimeout2 = 30_000L;

        ig1.cluster().setTxTimeoutOnPartitionMapExchange(expTimeout1);
        assertTxTimeoutOnPartitionMapExchange(expTimeout1);

        ig2.cluster().setTxTimeoutOnPartitionMapExchange(expTimeout2);
        assertTxTimeoutOnPartitionMapExchange(expTimeout2);
    }

    /**
     * Tests applying new txTimeoutOnPartitionMapExchange while an exchange future runs.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testSetTxTimeoutDuringPartitionMapExchange() throws Exception {
        IgniteEx ig = (IgniteEx) startGrids(2);

        checkSetTxTimeoutDuringPartitionMapExchange(ig);
    }

    /**
     * Tests applying new txTimeoutOnPartitionMapExchange while an exchange future runs on client node.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testSetTxTimeoutOnClientDuringPartitionMapExchange() throws Exception {
        IgniteEx ig = startGrids(2);
        IgniteEx client = startClientGrid(getConfiguration("client"));

        checkSetTxTimeoutDuringPartitionMapExchange(client);
    }

    /**
     * @param ig Ignite instance where deadlock tx will start.
     * @throws Exception If fails.
     */
    private void checkSetTxTimeoutDuringPartitionMapExchange(IgniteEx ig) throws Exception {
        final long longTimeout = 600_000L;
        final long shortTimeout = 5_000L;

        TransactionsMXBean mxBean = txMXBean(0);

        // Case 1: set very long txTimeoutOnPME, transaction should be rolled back.
        mxBean.setTxTimeoutOnPartitionMapExchange(longTimeout);
        assertTxTimeoutOnPartitionMapExchange(longTimeout);

        AtomicReference<Exception> txEx = new AtomicReference<>();

        IgniteInternalFuture<Long> fut = startDeadlock(ig, txEx, 0);

        startGridAsync(2);

        waitForExchangeStarted(ig);

        mxBean.setTxTimeoutOnPartitionMapExchange(shortTimeout);

        awaitPartitionMapExchange();

        fut.get();

        assertTrue("Transaction should be rolled back", hasCause(txEx.get(), TransactionRollbackException.class));

        // Case 2: txTimeoutOnPME will be set to 0 after starting of PME, transaction should be cancelled on timeout.
        mxBean.setTxTimeoutOnPartitionMapExchange(longTimeout);
        assertTxTimeoutOnPartitionMapExchange(longTimeout);

        fut = startDeadlock(ig, txEx, 10000L);

        startGridAsync(3);

        waitForExchangeStarted(ig);

        mxBean.setTxTimeoutOnPartitionMapExchange(0);

        fut.get();

        assertTrue("Transaction should be canceled on timeout", hasCause(txEx.get(), TransactionTimeoutException.class));
    }

    /**
     * Start test deadlock
     *
     * @param ig Ig.
     * @param txEx Atomic reference to transaction exception.
     * @param timeout Transaction timeout.
     */
    private IgniteInternalFuture<Long> startDeadlock(Ignite ig, AtomicReference<Exception> txEx, long timeout) {
        IgniteCache<Object, Object> cache = ig.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        AtomicInteger thCnt = new AtomicInteger();

        CyclicBarrier barrier = new CyclicBarrier(2);

        return GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() {
                int thNum = thCnt.incrementAndGet();

                try (Transaction tx = ig.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, timeout, 0)) {
                    cache.put(thNum, 1);

                    barrier.await();

                    cache.put(thNum % 2 + 1, 1);

                    tx.commit();
                }
                catch (Exception e) {
                    txEx.set(e);
                }

                return null;
            }
        }, 2, "tx-thread");
    }

    /**
     * Starts grid asynchronously and returns just before grid starting.
     * Avoids blocking on PME.
     *
     * @param idx Test grid index.
     * @throws Exception If fails.
     */
    private void startGridAsync(int idx) throws Exception {
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    startGrid(idx);
                }
                catch (Exception e) {
                    // no-op.
                }
            }
        });
    }

    /**
     * Waits for srarting PME on grid.
     *
     * @param ig Ignite grid.
     * @throws IgniteCheckedException If fails.
     */
    private void waitForExchangeStarted(IgniteEx ig) throws IgniteCheckedException {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (GridDhtPartitionsExchangeFuture fut: ig.context().cache().context().exchange().exchangeFutures()) {
                    if (!fut.isDone())
                        return true;
                }

                return false;
            }
        }, WAIT_CONDITION_TIMEOUT);

        // Additional waiting to ensure that code really start waiting for partition release.
        U.sleep(5_000L);
    }

    /** */
    private TransactionsMXBean txMXBean(int igniteInt) throws Exception {
        return getMxBean(getTestIgniteInstanceName(igniteInt), "Transactions",
            TransactionsMXBeanImpl.class, TransactionsMXBean.class);
    }

    /**
     * Checking the transaction timeout on all grids.
     *
     * @param expTimeout Expected timeout.
     * @throws IgniteInterruptedCheckedException If failed.
     */
    private void assertTxTimeoutOnPartitionMapExchange(final long expTimeout)
        throws IgniteInterruptedCheckedException {

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Ignite ignite : G.allGrids()) {
                    long actualTimeout = ignite.configuration()
                        .getTransactionConfiguration().getTxTimeoutOnPartitionMapExchange();

                    if (actualTimeout != expTimeout) {
                        log.warning(String.format(
                            "Wrong transaction timeout on partition map exchange [grid=%s, timeout=%d, expected=%d]",
                            ignite.name(), actualTimeout, expTimeout));

                        return false;
                    }
                }

                return true;

            }
        }, WAIT_CONDITION_TIMEOUT));
    }
}
