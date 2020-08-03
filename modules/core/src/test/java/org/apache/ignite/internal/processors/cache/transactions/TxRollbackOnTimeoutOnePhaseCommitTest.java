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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests rollback on timeout scenarios for one-phase commit protocol.
 */
public class TxRollbackOnTimeoutOnePhaseCommitTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (!igniteInstanceName.startsWith("client")) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(GRID_CNT);

        startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testRollbackOnTimeoutPartitionDesyncPessimistic() throws Exception {
        doTestRollbackOnTimeoutPartitionDesync(PESSIMISTIC);
    }

    /** */
    @Test
    public void testRollbackOnTimeoutPartitionDesyncOptimistic() throws Exception {
        doTestRollbackOnTimeoutPartitionDesync(OPTIMISTIC);
    }

    /** */
    @Test
    public void testUnlockOptimistic() throws IgniteCheckedException {
        IgniteEx client = grid("client");

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        int key = 0;

        CountDownLatch lock = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);

        IgniteInternalFuture fut = runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                client.cache(DEFAULT_CACHE_NAME).put(key, key + 1);

                lock.countDown();

                try {
                    assertTrue(U.await(finish, 30, TimeUnit.SECONDS));
                }
                catch (IgniteInterruptedCheckedException e) {
                    fail();
                }

                tx.commit();
            }
        });

        try (Transaction tx = client.transactions().txStart(OPTIMISTIC, REPEATABLE_READ, 200, 1)) {
            try {
                assertTrue(U.await(lock, 30, TimeUnit.SECONDS));
            }
            catch (IgniteInterruptedCheckedException e) {
                fail();
            }

            client.cache(DEFAULT_CACHE_NAME).put(key, key);

            tx.commit();

            fail();
        }
        catch (Exception e) {
            assertTrue(e.getClass().getName(), X.hasCause(e, TransactionTimeoutException.class));
        }

        assertNull(client.cache(DEFAULT_CACHE_NAME).get(key));

        finish.countDown();

        fut.get();

        assertEquals(1, client.cache(DEFAULT_CACHE_NAME).get(key));
    }

    /** */
    private void doTestRollbackOnTimeoutPartitionDesync(TransactionConcurrency concurrency) throws Exception {
        IgniteEx client = grid("client");

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        int key = 0;

        Ignite primary = primaryNode(key, DEFAULT_CACHE_NAME);
        Ignite backup = backupNode(key, DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi backupSpi = TestRecordingCommunicationSpi.spi(backup);
        backupSpi.blockMessages(GridDhtTxPrepareResponse.class, primary.name());

        IgniteInternalFuture fut = runAsync(() -> {
            try {
                backupSpi.waitForBlocked(1, 5000);
            }
            catch (InterruptedException e) {
                fail();
            }

            doSleep(500);

            backupSpi.stopBlock();
        });

        try (Transaction tx = client.transactions().txStart(concurrency, REPEATABLE_READ, 500, 1)) {
            client.cache(DEFAULT_CACHE_NAME).put(key, key);

            tx.commit();
        }
        catch (Exception e) {
            assertTrue(e.getClass().getName(), X.hasCause(e, TransactionTimeoutException.class));
        }

        fut.get();

        IdleVerifyResultV2 res = idleVerify(client, DEFAULT_CACHE_NAME);

        if (res.hasConflicts()) {
            StringBuilder b = new StringBuilder();

            res.print(b::append);

            fail(b.toString());
        }

        checkFutures();
    }
}
