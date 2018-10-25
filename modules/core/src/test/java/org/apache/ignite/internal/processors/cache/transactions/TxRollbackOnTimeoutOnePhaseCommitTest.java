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
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.mockito.Mockito;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests rollback on timeout scenarios for one-phase commit protocol.
 */
public class TxRollbackOnTimeoutOnePhaseCommitTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** */
    public void testRollbackOnTimeoutPartitionDesync() throws Exception {
        try {
            Ignite crd = startGridsMultiThreaded(GRID_CNT);

            IgniteEx client = startGrid("client");

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            int key = 0;

            Ignite primary = primaryNode(key, DEFAULT_CACHE_NAME);
            Ignite backup = backupNode(key, DEFAULT_CACHE_NAME);

            TestRecordingCommunicationSpi backupSpi = TestRecordingCommunicationSpi.spi(backup);
            backupSpi.blockMessages(GridDhtTxPrepareResponse.class, primary.name());

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        backupSpi.waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        fail();
                    }

                    doSleep(500);

                    backupSpi.stopBlock();
                }
            });

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 200, 1)) {
                client.cache(DEFAULT_CACHE_NAME).put(key, key);

                tx.commit();
            }
            catch (Exception e) {
                assertTrue(e.getClass().getName(), X.hasCause(e, TransactionTimeoutException.class));
            }

            doSleep(5000);

            checkFutures();

            IdleVerifyResultV2 res = idleVerify(client, DEFAULT_CACHE_NAME);

            if (res.hasConflicts()) {
                StringBuilder b = new StringBuilder();

                res.print(b::append);

                fail(b.toString());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    public void testRollbackOnTimeoutBackupTxHang() throws Exception {
        try (Ignite crd = startGridsMultiThreaded(GRID_CNT)) {
            IgniteEx client = startGrid("client");

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            int key = 0;

            IgniteEx backup = (IgniteEx)backupNode(key, DEFAULT_CACHE_NAME);

            GridCacheSharedContext<Object, Object> backupCtx = backup.context().cache().context();

            IgniteTxManager newTm = Mockito.spy(backupCtx.tm());

            CountDownLatch l = new CountDownLatch(1);

            // Simulate race between tx timeout on primary node and tx timeout on backup node.
            Mockito.doAnswer(invocation -> {
                l.await();

                IgniteInternalTx tx = (IgniteInternalTx)invocation.getArguments()[0];

                while (tx.remainingTime() >= 0)
                    doSleep(10);

                // Call to prepare will trigger timeout exception.
                return invocation.callRealMethod();
            }).when(newTm).prepareTx(Mockito.any(), Mockito.any());

            backupCtx.setTxManager(newTm);

            TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);

            clientSpi.blockMessages((node, msg) -> {
                if (msg instanceof GridNearTxFinishRequest) {
                    GridNearTxFinishRequest req = (GridNearTxFinishRequest)msg;

                    return !req.commit();
                }

                return false;
            });

            IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
                // Wait until client received timeout
                try {
                    clientSpi.waitForBlocked();
                }
                catch (InterruptedException e) {
                    fail();
                }

                // Notify mocked call to proceed.
                l.countDown();

                doSleep(1000);

                clientSpi.stopBlock();
            });

            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 500, 1)) {
                client.cache(DEFAULT_CACHE_NAME).put(key, key);

                tx.commit();

                fail();
            }
            catch (Exception e) {
                assertTrue(e.getClass().getName(), X.hasCause(e, TransactionTimeoutException.class));
            }

            fut.get();

            checkFutures();
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }
}
