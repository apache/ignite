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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccSnapshotResponse;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheMvccProcessorTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTreeWithPersistence() throws Exception {
        persistence = true;

        checkTreeOperations();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTreeWithoutPersistence() throws Exception {
        persistence = true;

        checkTreeOperations();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkTreeOperations() throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().active(true);

        grid.createCache(new CacheConfiguration<>("test").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT));

        MvccProcessorImpl mvccProcessor = mvccProcessor(grid);

        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 1, MvccUtils.MVCC_OP_COUNTER_NA)));

        mvccProcessor.updateState(new MvccVersionImpl(1, 1, MvccUtils.MVCC_OP_COUNTER_NA), TxState.PREPARED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 2, MvccUtils.MVCC_OP_COUNTER_NA), TxState.PREPARED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 3, MvccUtils.MVCC_OP_COUNTER_NA), TxState.COMMITTED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 4, MvccUtils.MVCC_OP_COUNTER_NA), TxState.ABORTED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 5, MvccUtils.MVCC_OP_COUNTER_NA), TxState.ABORTED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 6, MvccUtils.MVCC_OP_COUNTER_NA), TxState.PREPARED);

        if (persistence) {
            stopGrid(0, false);
            grid = startGrid(0);

            grid.cluster().active(true);

            mvccProcessor = mvccProcessor(grid);
        }

        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 1, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 2, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.COMMITTED, mvccProcessor.state(new MvccVersionImpl(1, 3, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.ABORTED, mvccProcessor.state(new MvccVersionImpl(1, 4, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.ABORTED, mvccProcessor.state(new MvccVersionImpl(1, 5, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 6, MvccUtils.MVCC_OP_COUNTER_NA)));

        mvccProcessor.removeUntil(new MvccVersionImpl(1, 5, MvccUtils.MVCC_OP_COUNTER_NA));

        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 1, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 2, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 3, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 4, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 5, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 6, MvccUtils.MVCC_OP_COUNTER_NA)));
    }

    /**
     * @throws Exception If fails.
     */
    public void testMvccTrackerFailure() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-7388");

        testSpi = true;
        disableScheduledVacuum = true;

        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT);

        startGrids(2);

        awaitPartitionMapExchange();

        IgniteEx node1 = grid(1);
        final IgniteCache<Object, Object> cache = node1.cache(DEFAULT_CACHE_NAME);

        final Integer key = primaryKey(cache);

        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)grid(0).context().config().getCommunicationSpi();
        spi.blockMessages(MvccSnapshotResponse.class, grid(1).name());

        IgniteTransactions txs1 = grid(1).transactions();

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try(Transaction tx = txs1.txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    cache.put(key, 1);

                    tx.commit();
                }
            }
        });

        try {
            assertTrue("Failed to wait MvccSnapshotResponse message",
                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return spi.hasBlockedMessages();
                    }
                }, 3_000));

            fut.cancel();

            try {
                fut.get();
            }
            catch (IgniteCheckedException ex) {
                if(!X.hasCause(ex, IgniteInterruptedException.class))
                    throw ex;
            }
        }
        finally {
            spi.stopBlock(true);
        }

        assertNull(cache.get(1));

        try (Transaction tx = txs1.txStart()) {
            tx.timeout(2000);

            cache.put(key, 2);

            tx.commit();
        }

        assertEquals(2, cache.get(key));
    }
}
