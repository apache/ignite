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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedBaseMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.lang.GridFunc.isEmpty;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests log content of timeouted transaction.
 */
@RunWith(Parameterized.class)
public class TransactionTimeoutLogTest extends GridCommonAbstractTest {
    /** */
    private Map<String, ListeningTestLogger> logs = new HashMap<>();

    /** Cache sync mode. */
    @Parameterized.Parameter(0)
    public CacheWriteSynchronizationMode cacheSyncMode;

    /** If {@code true}, use IgniteCache#putAll(). Or IgniteCache#put() if {@code false}. */
    @Parameterized.Parameter(1)
    public boolean usePutAll;

    /** Number of records to put within single transaction. */
    @Parameterized.Parameter(2)
    public int valuesCnt;

    /** Transaction node type. */
    @Parameterized.Parameter(3)
    public TxNodeType txNodeType;

    /** Isolation type. */
    @Parameterized.Parameter(4)
    public TransactionIsolation txIsolation;

    /** Numberof backups. */
    @Parameterized.Parameter(5)
    public int backups;

    /** Run params set. */
//    @Parameterized.Parameters(name = "syncMode={0},putAll={1},records={2},txNode={3},isolation={4},backups={5}")
//    public static Iterable<Object[]> params() {
//        return cartesianProduct(
//            // Sync mode
//            F.asList(FULL_SYNC, PRIMARY_SYNC),
//            // Use putAll()
//            F.asList(false, true),
//            // Number of records to chnage
//            F.asList(1, 10),
//            // Transaction initiator type
//            F.asList(TxNodeType.values()),
//            // Transaction isolation level
//            F.asList(TransactionIsolation.values()),
//            // Number of backups / one phase commit
//            F.asList(2, 1)
//        );
//    }

    /** Run params set. */
    @Parameterized.Parameters(name = "syncMode={0},putAll={1},records={2},txNode={3},isolation={4},backups={5}")
    public static Iterable<Object[]> params() {
        return cartesianProduct(
            // Sync mode
            F.asList(FULL_SYNC),
            // Use putAll()
            F.asList(true),
            // Number of records to chnage
            F.asList(1),
            // Transaction initiator type
            F.asList(TxNodeType.CLIENT),
            // Transaction isolation level
            F.asList(REPEATABLE_READ),
            // Number of backups. 1 is for one-phase commit.
            F.asList(2, 2)
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTransactionConfiguration(new TransactionConfiguration()
            .setDefaultTxConcurrency(PESSIMISTIC)
            .setDefaultTxIsolation(txIsolation)
            .setDefaultTxTimeout(3_000)
        );

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        ListeningTestLogger ll = new ListeningTestLogger(cfg.getGridLogger());

        logs.put(igniteInstanceName, ll);

        cfg.setGridLogger(ll);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ((AtomicReference<IgniteLogger>)U.staticField(IgniteTxAdapter.class, "logRef")).set(null);
        U.findField(IgniteTxAdapter.class, "log").set(null, null);

        ((AtomicReference<IgniteLogger>)U.staticField(GridDhtTxPrepareFuture.class, "logRef")).set(null);
        U.findField(GridDhtTxPrepareFuture.class, "log").set(null, null);

        startGridsMultiThreaded(3);

        grid(0).createCache(new CacheConfiguration<>()
            .setName("cache")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(backups)
        );

        try (IgniteDataStreamer<Object, Object> s = grid(1).dataStreamer("cache")) {
            for (int i = 0; i < 1_000; ++i)
                s.addData(i, i);
        }
    }

    /**
     * Test backup not responding the prepare phase.
     */
    @Test
    public void testBackupDelaysOnPrepare() throws Exception {
        doTest(false, GridDhtTxPrepareResponse.class);
    }

//    /**
//     * Test primary not responding on near prepare.
//     */
//    @Test
//    public void testPrimaryDelaysOnPrepare() throws Exception {
//        doTest(true, GridNearTxPrepareResponse.class);
//    }
//
    /**
     * Test primary not responding on lock request.
     */
    @Test
    public void testPrimaryDelaysOnLock() throws Exception {
        doTest(true, GridNearLockResponse.class);
    }

    /** TODO */
    private void doTest(boolean delayOnPrimary, Class<? extends GridDistributedBaseMessage> msgToDelay) throws Exception {
        IgniteEx delayedNode = txNode(TxNodeType.SERVER_DELAYED);

        IgniteEx putter = txNode(txNodeType);

        List<Integer> keys = keys(putter, delayedNode, delayOnPrimary);

        assert keys.size() == valuesCnt;

        Ignite primary = primaryNode(keys.get(0), "cache");

        blockMessage(delayedNode, msgToDelay);

        IgniteCache<Integer, Integer> cache = putter.cache("cache");

        ListeningTestLogger txNodeLog = logs.get(putter.name());

        LogListener txNodeLsnr = LogListener.matches("Not responded primary nodes (or its backups): " +
            primary.cluster().localNode().id()).times(1).build();

        txNodeLog.registerListener(txNodeLsnr);

        if (backups > 1) {
            LogListener nearNodeLsnr = null;

            if (!delayOnPrimary) {
                ListeningTestLogger nearNodeLog = logs.get(primary.name());

                nearNodeLsnr = LogListener.matches("Not responded backup nodes: " +
                    delayedNode.cluster().localNode().id()).times(1).build();

                nearNodeLog.registerListener(nearNodeLsnr);
            }

            assertThrowsAnyCause(
                null,
                () -> {
                    doTx(putter, cache, keys);

                    return null;
                },
                TransactionTimeoutException.class,
                null
            );

            if (nearNodeLsnr != null)
                assertTrue(nearNodeLsnr.check());
        }
        else {
            // One-phase commit doesn't wait apply the finish command and doesn't use prepare timeout on the
            // near/primary node. See IGNITE-19336.
            runAsync(() -> doTx(putter, cache, keys));
        }

        assertTrue(txNodeLsnr.check(putter.configuration().getTransactionConfiguration().getDefaultTxTimeout() * 4));

        for (int i = 0; i < keys.size(); ++i)
            assertEquals(keys.get(i), cache.get(keys.get(i)));
    }

    /** */
    private void doTx(Ignite putter, IgniteCache<Integer, Integer> cache, Collection<Integer> keys) {
        SortedMap<Integer, Integer> putAllData = new TreeMap<>();

        if (usePutAll)
            keys.forEach(k -> putAllData.put(k, updatedValue(k)));

        try (Transaction tx = putter.transactions().txStart()) {
            if (!isEmpty(putAllData))
                cache.putAll(putAllData);
            else
                keys.forEach(k -> cache.put(k, updatedValue(k)));

            tx.commit();
        }
    }

    /**
     * @return Transaction node.
     */
    private IgniteEx txNode(TxNodeType txNodeType) throws Exception {
        switch (txNodeType) {
            case CLIENT:
                return startClientGrid();

            case SERVER:
                return grid(1);

            case SERVER_DELAYED:
                return grid(2);

            default:
                assert false;
        }

        return null;
    }

    /**
     * Generates key for transaction.
     */
    private List<Integer> keys(IgniteEx putter, IgniteEx delayedNode, boolean primary) {
        assert txNodeType != TxNodeType.SERVER_DELAYED ||
            (putter.equals(delayedNode) && !putter.cluster().localNode().isClient());

        List<Integer> keys = new ArrayList<>(valuesCnt);

        int key = 0;

        while (keys.size() < valuesCnt) {
            if (delayedNode.equals(primary ? primaryNode(key, "cache") : backupNode(key, "cache")))
                keys.add(key);

            ++key;
        }

        return keys;
    }

    /** */
    private static Integer updatedValue(Integer key) {
        return key + 1;
    }

//    /** */
//    @Test
//    public void testPrimaryPrepareNearResponseLost() throws Exception {
//        test(() -> blockMessage(primaryNode(0, "cache"), GridNearTxPrepareResponse.class),
//            ccfg -> ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));
//    }
//
//    /** This test works. The transaction timeout is processed. */
//    //@Test
//    public void testPrimaryLockResponseLost() throws Exception {
//        test(() -> blockMessage(primaryNode(0, "cache"), GridNearLockResponse.class),
//            ccfg -> ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));
//    }

//    /** */
//    @Test
//    public void testPrimaryNearFinishResponseLost() throws Exception {
//        test(() -> blockMessage(primaryNode(0, "cache"), GridNearTxFinishResponse.class),
//            ccfg -> ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));
//    }

    /** */
    private static void blockMessage(Ignite grid, Class<? extends GridDistributedBaseMessage> cl) {
        ((TestRecordingCommunicationSpi)grid.configuration().getCommunicationSpi())
            .blockMessages((n, m) -> cl.isAssignableFrom(m.getClass()));
    }

    /**
     * Transaction node type.
     */
    private enum TxNodeType {
        /** 'Fat' client. */
        CLIENT,

        /** Server node, not delayed. */
        SERVER,

        /** Not-responding server node. */
        SERVER_DELAYED
    }
}