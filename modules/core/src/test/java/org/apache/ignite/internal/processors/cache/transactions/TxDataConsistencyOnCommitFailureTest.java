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

import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests data consistency if transaction is failed due to heuristic exception on originating node.
 */
public class TxDataConsistencyOnCommitFailureTest extends GridCommonAbstractTest {
    /** */
    public static final int KEY = 0;

    /** */
    public static final String CLIENT = "client";

    /** */
    private int nodesCnt;

    /** */
    private int backups;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setCacheMode(CacheMode.PARTITIONED).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setBackups(backups).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testCommitErrorOnNearNode2PC() throws Exception {
        nodesCnt = 3;

        backups = 2;

        doTestCommitError(() -> {
            try {
                return startClientGrid(CLIENT);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** */
    @Test
    public void testCommitErrorOnNearNode1PC() throws Exception {
        nodesCnt = 2;

        backups = 1;

        doTestCommitError(() -> {
            try {
                return startClientGrid(CLIENT);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9806")
    @Test
    public void testCommitErrorOnColocatedNode2PC() throws Exception {
        nodesCnt = 3;

        backups = 2;

        doTestCommitError(() -> primaryNode(KEY, DEFAULT_CACHE_NAME));
    }

    /**
     * @param factory Factory.
     */
    private void doTestCommitError(Supplier<Ignite> factory) throws Exception {
        Ignite crd = startGridsMultiThreaded(nodesCnt);

        crd.cache(DEFAULT_CACHE_NAME).put(KEY, KEY);

        Ignite ignite = factory.get();

        if (ignite == null)
            ignite = startClientGrid(CLIENT);

        assertNotNull(ignite.cache(DEFAULT_CACHE_NAME));

        injectMockedTxManager(ignite);

        checkKey();

        IgniteTransactions transactions = ignite.transactions();

        try (Transaction tx = transactions.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 0, 1)) {
            assertNotNull(transactions.tx());

            ignite.cache(DEFAULT_CACHE_NAME).put(KEY, KEY + 1);

            tx.commit();

            fail();
        }
        catch (Exception t) {
            // No-op.
        }

        checkKey();

        checkFutures();
    }

    /**
     * @param ignite Ignite.
     */
    private void injectMockedTxManager(Ignite ignite) {
        IgniteEx igniteEx = (IgniteEx)ignite;

        GridCacheSharedContext<Object, Object> ctx = igniteEx.context().cache().context();

        IgniteTxManager tm = ctx.tm();

        IgniteTxManager mockTm = Mockito.spy(tm);

        MockGridNearTxLocal locTx = new MockGridNearTxLocal(ctx, false, false, false, GridIoPolicy.SYSTEM_POOL,
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 0, true, null, 1, null, 0, null,
            null);

        Mockito.doAnswer(new Answer<GridNearTxLocal>() {
            @Override public GridNearTxLocal answer(InvocationOnMock invocation) throws Throwable {
                mockTm.onCreated(null, locTx);

                return locTx;
            }
        }).when(mockTm).
            newTx(locTx.implicit(), locTx.implicitSingle(), null, locTx.concurrency(),
                locTx.isolation(), locTx.timeout(), locTx.storeEnabled(), null, locTx.size(), locTx.label(), false);

        ctx.setTxManager(mockTm);
    }

    /** */
    private void checkKey() {
        for (Ignite ignite : G.allGrids()) {
            if (!ignite.configuration().isClientMode())
                assertNotNull(ignite.cache(DEFAULT_CACHE_NAME).localPeek(KEY));
        }
    }

    /** */
    private static class MockGridNearTxLocal extends GridNearTxLocal {
        /** Empty constructor. */
        public MockGridNearTxLocal() {
        }

        /**
         * @param ctx Context.
         * @param implicit Implicit.
         * @param implicitSingle Implicit single.
         * @param sys System.
         * @param plc Policy.
         * @param concurrency Concurrency.
         * @param isolation Isolation.
         * @param timeout Timeout.
         * @param storeEnabled Store enabled.
         * @param mvccOp Mvcc op.
         * @param txSize Tx size.
         * @param subjId Subj id.
         * @param taskNameHash Task name hash.
         * @param lb Label.
         * @param txDumpsThrottling Log throttling information.
         */
        public MockGridNearTxLocal(GridCacheSharedContext ctx, boolean implicit, boolean implicitSingle, boolean sys,
            byte plc, TransactionConcurrency concurrency, TransactionIsolation isolation, long timeout,
            boolean storeEnabled, Boolean mvccOp, int txSize, @Nullable UUID subjId, int taskNameHash, @Nullable String lb,
            IgniteTxManager.TxDumpsThrottling txDumpsThrottling) {
            super(ctx, implicit, implicitSingle, sys, plc, concurrency, isolation, timeout, storeEnabled, mvccOp,
                txSize, subjId, taskNameHash, lb, txDumpsThrottling, false);
        }

        /** {@inheritDoc} */
        @Override public void userCommit() throws IgniteCheckedException {
            throw new IgniteCheckedException("Force failure");
        }
    }
}
