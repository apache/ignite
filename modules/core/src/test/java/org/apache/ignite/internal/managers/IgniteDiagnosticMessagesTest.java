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

package org.apache.ignite.internal.managers;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgniteDiagnosticMessagesTest extends GridCommonAbstractTest {
    /** */
    private Integer connectionsPerNode;

    /** */
    private boolean testSpi;

    /** */
    private GridStringLogger strLog;

    /** */
    private ListeningTestLogger testLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (connectionsPerNode != null)
            ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setConnectionsPerNode(connectionsPerNode);

        if (strLog != null) {
            cfg.setGridLogger(strLog);

            strLog = null;
        }

        if (testLog != null) {
            cfg.setGridLogger(testLog);

            testLog = null;
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiagnosticMessages1() throws Exception {
        checkBasicDiagnosticInfo(CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiagnosticMessagesMvcc1() throws Exception {
        checkBasicDiagnosticInfo(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiagnosticMessages2() throws Exception {
        connectionsPerNode = 5;

        checkBasicDiagnosticInfo(CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDiagnosticMessagesMvcc2() throws Exception {
        connectionsPerNode = 5;

        checkBasicDiagnosticInfo(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongRunning() throws Exception {
        checkLongRunning(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongRunningMvcc() throws Exception {
        checkLongRunning(TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    public void checkLongRunning(CacheAtomicityMode atomicityMode) throws Exception {
        System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, "3500");

        try {
            testSpi = true;

            startGrid(0);

            GridStringLogger strLog = this.strLog = new GridStringLogger();

            strLog.logLength(1024 * 100);

            startGrid(1);

            awaitPartitionMapExchange();

            CacheConfiguration ccfg = cacheConfiguration(atomicityMode);

            final Ignite node0 = ignite(0);

            node0.createCache(ccfg);

            final Ignite node1 = ignite(1);

            UUID id0 = node0.cluster().localNode().id();

            TestRecordingCommunicationSpi.spi(node0).blockMessages(GridNearSingleGetResponse.class, node1.name());

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    Integer key = primaryKey(node0.cache(DEFAULT_CACHE_NAME));

                    node1.cache(DEFAULT_CACHE_NAME).get(key);

                    return null;
                }
            }, "get");

            U.sleep(10_000);

            assertFalse(fut.isDone());

            TestRecordingCommunicationSpi.spi(node0).stopBlock();

            fut.get();

            String log = strLog.toString();

            assertTrue(log.contains("GridPartitionedSingleGetFuture waiting for response [node=" + id0));
            assertTrue(log.contains("General node info [id=" + id0));
        }
        finally {
            System.clearProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration(CacheAtomicityMode atomicityMode) {
        return defaultCacheConfiguration()
            .setAtomicityMode(atomicityMode)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setNearConfiguration(null);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10637") // Support diagnostics message or disable test.
    @Test
    public void testSeveralLongRunningMvccTxs() throws Exception {
        checkSeveralLongRunningTxs(TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSeveralLongRunningTxs() throws Exception {
        checkSeveralLongRunningTxs(TRANSACTIONAL);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    public void checkSeveralLongRunningTxs(CacheAtomicityMode atomicityMode) throws Exception {
        int timeout = 3500;

        System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, String.valueOf(timeout));

        try {
            testSpi = true;

            startGrid(0);

            GridStringLogger strLog = this.strLog = new GridStringLogger();

            strLog.logLength(1024 * 100);

            startGrid(1);

            awaitPartitionMapExchange();

            CacheConfiguration ccfg = cacheConfiguration(atomicityMode);

            final Ignite node0 = ignite(0);
            final Ignite node1 = ignite(1);

            node0.createCache(ccfg);

            UUID id0 = node0.cluster().localNode().id();

            TestRecordingCommunicationSpi.spi(node0).blockMessages(GridNearLockResponse.class, node1.name());

            IgniteCache<Object, Object> cache = node0.cache(DEFAULT_CACHE_NAME);

            int txCnt = 4;

            final List<Integer> keys = primaryKeys(cache, txCnt, 0);

            final AtomicInteger idx = new AtomicInteger();

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCache<Object, Object> cache = node1.cache(DEFAULT_CACHE_NAME);

                    try (Transaction tx = node1.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        Integer key = keys.get(idx.getAndIncrement() % keys.size());

                        cache.putIfAbsent(key, String.valueOf(key));

                        tx.commit();
                    }

                    return null;
                }
            }, txCnt * 2, "tx");

            U.sleep(timeout * 2);

            assertFalse(fut.isDone());

            TestRecordingCommunicationSpi.spi(node0).stopBlock();

            fut.get();

            String log = strLog.toString();

            assertTrue(log.contains("Cache entries [cacheId=" + CU.cacheId(DEFAULT_CACHE_NAME) +
                ", cacheName=" + DEFAULT_CACHE_NAME + "]:"));
            assertTrue(countTxKeysInASingleBlock(log) == txCnt);

            assertTrue(log.contains("General node info [id=" + id0));
        }
        finally {
            System.clearProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);
        }
    }

    /**
     * @param log Log.
     * @return Count of keys in the first Cache entries block.
     */
    private int countTxKeysInASingleBlock(String log) {
        int idx1 = log.indexOf("Cache entries");
        int idx2 = log.indexOf("Local communication statistics");

        assert idx1 != -1 && idx2 != -1;

        // The first cache entries info block.
        String txInfo = log.substring(idx1, idx2);

        String srch = "    Key [";  // Search string.
        int len = 9;                // Search string length.

        int idx0, cnt = 0;

        while ((idx0 = txInfo.indexOf(srch) + len) >= len) {
            txInfo = txInfo.substring(idx0);

            cnt++;
        }

        return cnt;
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10637") // Support diagnostic messages or disable test.
    @Test
    public void testLongRunningMvccTx() throws Exception {
        checkLongRunningTx(TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongRunningTx() throws Exception {
        checkLongRunningTx(TRANSACTIONAL);

    }

    /**
     * Tests that {@link org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockFuture} timeout object
     * dumps debug info to log.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testTimeOutTxLock() throws Exception {
        final int longOpDumpTimeout = 500;

        ListeningTestLogger testLog = new ListeningTestLogger(false, log);

        IgniteLogger oldLog = GridTestUtils.getFieldValue(GridDhtLockFuture.class, "log");

        GridTestUtils.setFieldValue(GridDhtLockFuture.class, "log", testLog);

        try {
            IgniteEx grid1 = startGrid(0);

            LogListener lsnr = LogListener
                    .matches(Pattern.compile("Transaction tx=GridNearTxLocal \\[.*\\] timed out, can't acquire lock for"))
                    .andMatches(Pattern.compile(".*xid=.*, xidVer=.*, nearXid=.*, nearXidVer=.*, label=lock, " +
                            "nearNodeId=" + grid1.cluster().localNode().id() + ".*"))
                    .build();

            testLog.registerListener(lsnr);

            this.testLog = testLog;

            IgniteEx grid2 = startGrid(1);

            grid2.context().cache().context().tm().longOperationsDumpTimeout(longOpDumpTimeout);

            awaitPartitionMapExchange();

            emulateTxLockTimeout(grid1, grid2);

            assertTrue(lsnr.check());
        }
        finally {
            GridTestUtils.setFieldValue(GridDhtLockFuture.class, "log", oldLog);
        }
    }

    /**
     * Ensure that dumpLongRunningTransaction doesn't block scheduler.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDumpLongRunningOperationDoesntBlockTimeoutWorker() throws Exception {
        long longOpsDumpTimeout = 100;

        IgniteEx ignite = startGrid(0);

        IgniteCache cache = ignite.createCache(new CacheConfiguration<>("txCache").
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        ignite.transactions().txStart(PESSIMISTIC, SERIALIZABLE);

        cache.put(1, 1);

        // Wait for some time for transaction to be considered as long running.
        Thread.sleep(longOpsDumpTimeout * 2);

        // That will allow to block dumpLongRunningTransaction on line
        // {@code ClusterGroup nearNode = ignite.cluster().forNodeId(nearNodeId);}
        ignite.context().gateway().writeLock();

        try {
            ignite.context().cache().context().tm().longOperationsDumpTimeout(100);

            // Wait for some time to guarantee start dumping long running transaction.
            Thread.sleep(longOpsDumpTimeout * 2);

            AtomicBoolean schedulerAssertionFlag = new AtomicBoolean(false);

            CountDownLatch scheduleLatch = new CountDownLatch(1);

            ignite.context().timeout().schedule(
                () -> {
                    schedulerAssertionFlag.set(true);
                    scheduleLatch.countDown();
                },
                0,
                -1);

            scheduleLatch.await(5_000, TimeUnit.MILLISECONDS);

            // Ensure that dumpLongRunning transaction doesn't block scheduler.
            assertTrue(schedulerAssertionFlag.get());
        }
        finally {
            ignite.context().gateway().writeUnlock();
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    public void checkLongRunningTx(CacheAtomicityMode atomicityMode) throws Exception {

        final int longOpDumpTimeout = 1000;

        System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, String.valueOf(longOpDumpTimeout));

        try {
            final Ignite node0 = startGrid(0);

            CacheConfiguration ccfg = cacheConfiguration(atomicityMode);

            node0.createCache(ccfg);

            UUID id0 = node0.cluster().localNode().id();

            ListeningTestLogger testLog = this.testLog = new ListeningTestLogger(false, log);

            String msg1 = "Cache entries [cacheId=" + CU.cacheId(DEFAULT_CACHE_NAME) +
                ", cacheName=" + DEFAULT_CACHE_NAME + "]:";

            String msg2 = "General node info [id=" + id0;

            LogListener lsnr = LogListener.matches(msg1).andMatches(msg2).build();

            testLog.registerListener(lsnr);

            final Ignite node1 = startGrid(1);

            awaitPartitionMapExchange();

            final CountDownLatch l1 = new CountDownLatch(1);
            final CountDownLatch l2 = new CountDownLatch(1);

            final AtomicReference<Integer> key = new AtomicReference<>();

            GridCompoundFuture<Void, Void> fut = new GridCompoundFuture<>();

            fut.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCache<Object, Object> cache = node0.cache(DEFAULT_CACHE_NAME);

                    try (Transaction tx = node0.transactions().txStart()) {
                        key.set(primaryKey(cache));

                        cache.putIfAbsent(key.get(), "dummy val");

                        l1.countDown();
                        l2.await();

                        tx.commit();
                    }

                    return null;
                }
            }, "tx-1"));

            fut.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCache<Object, Object> cache = node1.cache(DEFAULT_CACHE_NAME);

                    try (Transaction tx = node1.transactions().txStart()) {
                        l1.await();

                        cache.replace(key.get(), "dummy val2");

                        tx.commit();
                    }

                    return null;
                }
            }, "tx-2"));

            fut.markInitialized();

            U.sleep(longOpDumpTimeout);

            assertFalse(fut.isDone());

            boolean wait = waitForCondition(lsnr::check, longOpDumpTimeout * 2);

            l2.countDown();

            fut.get();

            assertTrue("Unable to found diagnostic messages.", wait);
        }
        finally {
            System.clearProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteTx() throws Exception {
        checkRemoteTx(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteMvccTx() throws Exception {
        checkRemoteTx(TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    public void checkRemoteTx(CacheAtomicityMode atomicityMode) throws Exception {
        int timeout = 3500;

        System.setProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, String.valueOf(timeout));

        try {
            testSpi = true;

            startGrid(0);

            GridStringLogger strLog = this.strLog = new GridStringLogger();

            strLog.logLength(1024 * 100);

            startGrid(1);

            awaitPartitionMapExchange();

            CacheConfiguration ccfg = cacheConfiguration(atomicityMode).setBackups(1);

            if (atomicityMode != TRANSACTIONAL_SNAPSHOT ||
                MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.NEAR_CACHE))
                ccfg.setNearConfiguration(new NearCacheConfiguration<>());

            final Ignite node0 = ignite(0);
            final Ignite node1 = ignite(1);

            node0.createCache(ccfg);

            UUID id0 = node0.cluster().localNode().id();

            TestRecordingCommunicationSpi.spi(node0).blockMessages(GridDhtTxPrepareResponse.class, node1.name());

            int txCnt = 4;

            final List<Integer> keys = primaryKeys(node1.cache(DEFAULT_CACHE_NAME), txCnt, 0);

            final AtomicInteger idx = new AtomicInteger();

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCache<Object, Object> cache = node1.cache(DEFAULT_CACHE_NAME);

                    try (Transaction tx = node1.transactions().txStart()) {
                        Integer key = keys.get(idx.getAndIncrement());

                        cache.getAndPut(key, "new-" + key);

                        tx.commit();
                    }

                    return null;
                }
            }, txCnt, "tx");

            U.sleep(timeout * 2);

            assertFalse(fut.isDone());

            TestRecordingCommunicationSpi.spi(node0).stopBlock();

            fut.get();

            String log = strLog.toString();

            assertTrue(log.contains("Related transactions ["));
            assertTrue(log.contains("General node info [id=" + id0));
        }
        finally {
            System.clearProperty(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @throws Exception If failed.
     */
    private void checkBasicDiagnosticInfo(CacheAtomicityMode atomicityMode) throws Exception {
        startGrids(3);

        startClientGrid(3);
        startClientGrid(4);

        CacheConfiguration ccfg = cacheConfiguration(atomicityMode).setCacheMode(REPLICATED);

        ignite(0).createCache(ccfg);

        awaitPartitionMapExchange();

        sendDiagnostic();

        for (int i = 0; i < 5; i++) {
            final IgniteCache<Object, Object> cache = ignite(i).cache(DEFAULT_CACHE_NAME);

            // Put from multiple threads to create multiple connections.
            GridTestUtils.runMultiThreaded(new Runnable() {
                @Override public void run() {
                    for (int j = 0; j < 10; j++)
                        cache.put(ThreadLocalRandom.current().nextInt(), j);
                }
            }, 10, "cache-thread");
        }

        sendDiagnostic();
    }

    /**
     * @throws Exception If failed.
     */
    private void sendDiagnostic() throws Exception {
        for (int i = 0; i < 5; i++) {
            IgniteKernal node = (IgniteKernal)ignite(i);

            for (int j = 0; j < 5; j++) {
                if (i != j) {
                    ClusterNode dstNode = ignite(j).cluster().localNode();

                    final GridFutureAdapter<String> fut = new GridFutureAdapter<>();

                    IgniteDiagnosticPrepareContext ctx = new IgniteDiagnosticPrepareContext(node.getLocalNodeId());

                    ctx.basicInfo(dstNode.id(), "Test diagnostic");

                    ctx.send(node.context(), new IgniteInClosure<IgniteInternalFuture<String>>() {
                        @Override public void apply(IgniteInternalFuture<String> diagFut) {
                            try {
                                fut.onDone(diagFut.get());
                            }
                            catch (Exception e) {
                                fut.onDone(e);
                            }
                        }
                    });

                    String msg = fut.get();

                    assertTrue("Unexpected message: " + msg,
                        msg.contains("Test diagnostic") &&
                            msg.contains("General node info [id=" + dstNode.id() + ", client=" + dstNode.isClient() + ", discoTopVer=AffinityTopologyVersion [topVer=5, minorTopVer=") &&
                            msg.contains("Partitions exchange info [readyVer=AffinityTopologyVersion [topVer=5, minorTopVer="));
                }
            }
        }
    }

    /**
     * Emulates tx lock timeout.
     *
     * @param node1 First node.
     * @param node2 Second node.
     * @throws Exception If failed.
     */
    private void emulateTxLockTimeout(Ignite node1, Ignite node2) throws Exception {
        node1.createCache(cacheConfiguration(TRANSACTIONAL).setBackups(1));

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        IgniteInternalFuture<?> fut1 = runAsync(new Runnable() {
            @Override public void run() {
                try {
                    try (Transaction tx = node1.transactions().withLabel("lock")
                            .txStart(PESSIMISTIC, REPEATABLE_READ, 60_000, 2)) {
                        node1.cache(DEFAULT_CACHE_NAME).put(1, 10);

                        l1.countDown();

                        U.awaitQuiet(l2);

                        U.sleep(100);

                        tx.commit();
                    }
                }
                catch (Exception e) {
                    log.error("Failed on node1", e);
                }
            }
        }, "First");

        IgniteInternalFuture<?> fut2 = runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = node2.transactions().withLabel("lock")
                        .txStart(PESSIMISTIC, REPEATABLE_READ, 2000L, 2)) {
                    U.awaitQuiet(l1);

                    node2.cache(DEFAULT_CACHE_NAME).put(1, 20);

                    tx.commit();
                }
                catch (Exception e) {
                    log.error("Failed on node2 " + node2.cluster().localNode().id(), e);

                    l2.countDown();
                }
            }
        }, "Second");

        fut1.get();
        fut2.get();
    }
}
