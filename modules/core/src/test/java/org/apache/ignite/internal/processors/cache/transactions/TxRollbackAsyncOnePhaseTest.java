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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests async rollback scenarios for one-phase protocol.
 */
public class TxRollbackAsyncOnePhaseTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 2;

    /** */
    public static final int MB = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

//        if (persistenceEnabled())
//        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(WALMode.LOG_ONLY).setPageSize(1024).
//            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
//                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

//            if (nearCacheEnabled())
//                ccfg.setNearConfiguration(new NearCacheConfiguration());

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    public void testRollbackOnTimeoutPartitionDesync() throws Exception {
        try (Ignite crd = startGridsMultiThreaded(2)) {
            IgniteEx client = startGrid("client");

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            int key = 0;

            Ignite primary = primaryNode(key, DEFAULT_CACHE_NAME);
            Ignite backup = backupNode(key, DEFAULT_CACHE_NAME);

            TestRecordingCommunicationSpi backupSpi = TestRecordingCommunicationSpi.spi(backup);
            backupSpi.blockMessages(GridDhtTxPrepareResponse.class, primary.name());

            try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 200, 1)) {
                client.cache(DEFAULT_CACHE_NAME).put(key, key);

                tx.commit();

                fail();
            }
            catch (Exception e) {
                assertTrue(e.getClass().getName(), X.hasCause(e, TransactionTimeoutException.class));
            }

            backupSpi.waitForBlocked();
            backupSpi.stopBlock(true);

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

    public void testRollbackOnTimeoutPartitionDesync2() throws Exception {
        try (Ignite crd = startGridsMultiThreaded(2)) {
            IgniteEx client = startGrid("client");

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));

            int key = 0;

            Ignite primary = primaryNode(key, DEFAULT_CACHE_NAME);
            IgniteEx backup = (IgniteEx)backupNode(key, DEFAULT_CACHE_NAME);

            GridCacheSharedContext<Object, Object> backupCtx = backup.context().cache().context();

            IgniteTxManager newTm = Mockito.spy(backupCtx.tm());

            CountDownLatch l = new CountDownLatch(1);

            Mockito.doAnswer(new Answer() {
                @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                    l.await();

                    IgniteInternalTx tx = (IgniteInternalTx)invocation.getArguments()[0];

                    while (tx.remainingTime() >= 0)
                        doSleep(10);

                    // Call to prepare will trigger timeout exception.
                    return invocation.callRealMethod();
                }
            }).when(newTm).prepareTx(Mockito.any(), Mockito.any());

            backupCtx.setTxManager(newTm);

            TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
            clientSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message message) {
                    if (message instanceof GridNearTxFinishRequest) {
                        GridNearTxFinishRequest req = (GridNearTxFinishRequest)message;

                        return !req.commit();
                    }

                    return false;
                }
            });

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    // Wait until client received timeout
                    try {
                        clientSpi.waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        fail();
                    }

                    // Notify mocked call to proceed.
                    l.countDown();

                    doSleep(3000);
                    clientSpi.stopBlock();
                }
            });

            try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 500, 1)) {
                client.cache(DEFAULT_CACHE_NAME).put(key, key);

                tx.commit();

                fail();
            }
            catch (Exception e) {
                assertTrue(e.getClass().getName(), X.hasCause(e, TransactionTimeoutException.class));
            }

            doSleep(5000);

            fut.get();

            checkFutures();
        }
        finally {
            stopAllGrids();
        }
    }

    public void testRollbackOnShortTimeout() throws Exception {
        try (Ignite crd = startGridsMultiThreaded(2)) {
            IgniteEx client = startGrid("client");
            IgniteEx client2 = startGrid("client2");

            Class[] classes = {
                GridNearLockRequest.class, GridNearLockResponse.class,
                GridDhtTxPrepareRequest.class, GridDhtTxPrepareResponse.class, GridNearTxPrepareRequest.class,
                GridNearTxPrepareResponse.class, GridNearTxFinishRequest.class, GridNearTxFinishResponse.class};
            TestRecordingCommunicationSpi.spi(client).record(classes);

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));
            assertNotNull(client2.cache(DEFAULT_CACHE_NAME));

            Ignite primary = primaryNode(0, DEFAULT_CACHE_NAME);
            Ignite backup = backupNode(0, DEFAULT_CACHE_NAME);

            TestRecordingCommunicationSpi clientSpi = TestRecordingCommunicationSpi.spi(client);
            clientSpi.blockMessages(GridNearTxFinishRequest.class, primary.name());

            TestRecordingCommunicationSpi.spi(primary).record(classes);

            TestRecordingCommunicationSpi.spi(backup).record(classes);

            CyclicBarrier b = new CyclicBarrier(2);

            IgniteInternalFuture fut2 = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        TestRecordingCommunicationSpi.spi(client).waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    doSleep(3000);
                    TestRecordingCommunicationSpi.spi(client).stopBlock();
                }
            });

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = client2.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 0, 1)) {
                        client2.cache(DEFAULT_CACHE_NAME).put(0, 0);

                        U.awaitQuiet(b);

                        // Sleep while holding lock to trigger timeout future on primary node for second tx
                        doSleep(300);

                        tx.commit();
                    }
                }
            });

            try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 500, 1)) {
                U.awaitQuiet(b);

                client.cache(DEFAULT_CACHE_NAME).put(0, 0);

                tx.commit();
            }
            catch (Exception e) {
                boolean timedOut = X.hasCause(e, TransactionTimeoutException.class);

                if (!timedOut)
                    log.error("Error", e);

                assertTrue(timedOut);
            }

            fut.get();
            fut2.get();

            doSleep(5000);

            checkFutures();
        }
        finally {
            stopAllGrids();
        }
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }
}
