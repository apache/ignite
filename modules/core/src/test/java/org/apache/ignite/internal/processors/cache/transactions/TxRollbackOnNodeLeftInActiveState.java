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

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.NODE_LEFT_ROLLBACK_MSG;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * If a primary node for a transaction key left, a transaction must rollback imminently.
 */
public class TxRollbackOnNodeLeftInActiveState extends GridCommonAbstractTest {
    /** Tx timeout. */
    private static final long TX_TIMEOUT = 100_000;

    /** Partition primary node id. */
    private static final int PART_PRIMARY_NODE_ID = 0;

    /** Near node id. */
    private static final int NEAR_NODE_ID = 1;

    /** Partition primary node. */
    private IgniteEx partPrimaryNode;

    /** Near node. */
    private IgniteEx nearNode;

    /** Partition primary node id. */
    private UUID partPrimaryNodeId;

    /** Partition primary node consistent id. */
    private Object partPrimaryNodeConsistentId;

    /** Stop partition primary node. */
    private CountDownLatch stopPartPrimaryNode;

    /** Primary node stopped future. */
    private IgniteInternalFuture primaryNodeStoppedFut;

    /** Near key. */
    private Integer nearKey;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(2);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        partPrimaryNode = startGrid(PART_PRIMARY_NODE_ID);
        nearNode = startGrid(NEAR_NODE_ID);

        awaitPartitionMapExchange();

        partPrimaryNodeId = partPrimaryNode.localNode().id();
        partPrimaryNodeConsistentId = partPrimaryNode.localNode().consistentId();

        nearKey = primaryKey(nearNode.cache(DEFAULT_CACHE_NAME));

        stopPartPrimaryNode = new CountDownLatch(1);

        primaryNodeStoppedFut = runAsync(() -> {
            U.awaitQuiet(stopPartPrimaryNode);

            grid(PART_PRIMARY_NODE_ID).close();
        });
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test #1  If a primary node for a transaction key left, a transaction must rollback imminently.
     */
    @Test
    public void testFastRollbackAfterNodeLeftRepeatableRead() throws Exception {
        doTestCheckFastTransactionInterruptionAfterNodeLeft(REPEATABLE_READ);
    }

    /**
     * Test #2 After auto rollback a cache modification operation doesn't lead to a new transaction.
     */
    @Test
    public void testNewTransactionWillNotProducedOnPutRepeatableRead() throws Exception {
        doTestOperationAfterRollback(() -> nearNode.cache(DEFAULT_CACHE_NAME).put(nearKey, 1243), REPEATABLE_READ);
    }

    /**
     * Test #2 After auto rollback a cache modification operation doesn't lead to a new transaction.
     */
    @Test
    public void testNewTransactionWillNotProducedOnRemoveRepeatableRead() throws Exception {
        doTestOperationAfterRollback(() -> nearNode.cache(DEFAULT_CACHE_NAME).remove(nearKey), REPEATABLE_READ);
    }

    /**
     * Test #1  If a primary node for a transaction key left, a transaction must rollback imminently.
     */
    @Test
    public void testFastRollbackAfterNodeLeftReadCommitted() throws Exception {
        doTestCheckFastTransactionInterruptionAfterNodeLeft(READ_COMMITTED);
    }

    /**
     * Test #2 After auto rollback a cache modification operation doesn't lead to a new transaction.
     */
    @Test
    public void testNewTransactionWillNotProducedOnPutReadCommitted() throws Exception {
        doTestOperationAfterRollback(() -> nearNode.cache(DEFAULT_CACHE_NAME).put(nearKey, 1243), READ_COMMITTED);
    }

    /**
     * Test #2 After auto rollback a cache modification operation doesn't lead to a new transaction.
     */
    @Test
    public void testNewTransactionWillNotProducedOnRemoveReadCommitted() throws Exception {
        doTestOperationAfterRollback(() -> nearNode.cache(DEFAULT_CACHE_NAME).remove(nearKey), READ_COMMITTED);
    }

    /**
     * Checks that time of waiting on {@link IgniteInternalFuture < IgniteInternalTx >} less than timeout.
     */
    private void doTestCheckFastTransactionInterruptionAfterNodeLeft(
        TransactionIsolation isolation) throws IgniteCheckedException {
        final Integer k = primaryKey(partPrimaryNode.cache(DEFAULT_CACHE_NAME));

        try (Transaction tx = nearNode.transactions().txStart(PESSIMISTIC, isolation, TX_TIMEOUT, 10)) {
            nearNode.cache(DEFAULT_CACHE_NAME).put(k, k);

            assertThatLocalTransactionExecuting(partPrimaryNode);

            stopPartPrimaryNode.countDown();

            long t1 = System.currentTimeMillis();

            ((TransactionProxyImpl)tx).tx().finishFuture().get(); // Shouldn't wait here until timeout.

            long t2 = System.currentTimeMillis();

            assertTrue(t2 - t1 < TX_TIMEOUT / 100); // Interrupted immediately!

            assertThrows(log, tx::commit, TransactionRollbackException.class, null);
        }

        primaryNodeStoppedFut.get();
    }

    /**
     * Does common logic of test.
     */
    private void doTestOperationAfterRollback(GridTestUtils.RunnableX operation,
        TransactionIsolation isolation) throws Exception {
        final Integer k = primaryKey(partPrimaryNode.cache(DEFAULT_CACHE_NAME));

        nearNode.cache(DEFAULT_CACHE_NAME).put(nearKey, 10);

        try (Transaction tx = nearNode.transactions().txStart(PESSIMISTIC, isolation, TX_TIMEOUT, 10)) {
            nearNode.cache(DEFAULT_CACHE_NAME).put(k, k);

            assertThatLocalTransactionExecuting(partPrimaryNode);

            stopPartPrimaryNode.countDown();

            Thread.sleep(500);

            assertThrows(log, operation, TransactionRollbackException.class, String.format(NODE_LEFT_ROLLBACK_MSG, partPrimaryNodeId, partPrimaryNodeConsistentId));

            assertThrows(log, tx::commit, TransactionRollbackException.class, null);
        }

        primaryNodeStoppedFut.get();
    }

    /**
     * Checks that local transaction exists.
     */
    private void assertThatLocalTransactionExecuting(IgniteEx node) {
        final Collection<IgniteInternalTx> txs = node.context().cache().context().tm().activeTransactions();
        assertFalse(txs.isEmpty());

        final IgniteInternalTx locTx = txs.iterator().next();

        assertTrue(locTx.local());
        assertFalse(locTx.near());
    }
}
