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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;
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
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests log content of timeouted transaction.
 */
@RunWith(Parameterized.class)
public class TransactionTimeoutLogTest extends GridCommonAbstractTest {
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

    /** Run params set. */
    @Parameterized.Parameters(name = "syncMode={0},putAll={1},records={2},txNode={3},isolation={4}")
    public static Iterable<Object[]> params() {
        return cartesianProduct(
//            F.asList(FULL_SYNC, PRIMARY_SYNC),
            F.asList(PRIMARY_SYNC),
//            F.asList(false, true),
            F.asList(false),
//            F.asList(1, 10),
            F.asList(10),
//            F.asList(TxNodeType.values())
            F.asList(TxNodeType.SERVER),
            F.asList(TransactionIsolation.values())
//            F.asList(REPEATABLE_READ)
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
            .setDefaultTxTimeout(5_000)
        );

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(3);

        grid(0).createCache(new CacheConfiguration<>()
            .setName("cache")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(2)
        );

        try (IgniteDataStreamer<Object, Object> s = grid(1).dataStreamer("cache")) {
            for (int i = 0; i < 1_000; ++i)
                s.addData(i, i);
        }
    }

//    /** */
//    @Test
//    public void testBackupFinishResponseLost() throws Exception {
//        test(() -> blockMessage(backupNodes(0, "cache").get(0), GridDhtTxFinishResponse.class),
//            ccfg -> ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));
//    }

    /** */
    @Test
    public void testBackupPrepareResponseLost() throws Exception {
        assert grid(0).cluster().forServers().nodes().size() > 2;
        assert valuesCnt > 0 && valuesCnt <= grid(0).cache("cache").size();

        IgniteEx putter = txNodeType == TxNodeType.CLIENT ? startClientGrid() : grid(1);

        IgniteEx delayedNode = grid(2);

        List<Integer> keys = keys(putter, delayedNode, txNodeType);

        assert keys.size() == valuesCnt;

        blockMessage(delayedNode, GridDhtTxPrepareResponse.class);

        IgniteCache<Object, Object> cache = putter.cache("cache");

        ListeningTestLogger testLog = new ListeningTestLogger(putter.log());

        LogListener logLsnr = LogListener.matches("Not responded backup nodes: " +
            delayedNode.cluster().localNode().id()).times(1).build();

        testLog.registerListener(logLsnr);

        Map<Integer, Integer> data = new TreeMap<>();

        if (usePutAll)
            keys.forEach(k -> data.put(k, updatedValue(k)));

        assertThrowsAnyCause(
            null,
            () -> {
                try (Transaction tx = putter.transactions().txStart()) {
                    if (usePutAll)
                        cache.putAll(data);
                    else
                        keys.forEach(k -> cache.put(k, updatedValue(k)));

                    tx.commit();
                }

                return null;
            },
            TransactionTimeoutException.class,
            null
        );

        logLsnr.check();

        for (int i = 0; i < 10; ++i)
            assertEquals(i, cache.get(i));
    }

    /**
     * Generates key for transaction.
     */
    private List<Integer> keys(IgniteEx putter, IgniteEx delayedNode, TxNodeType txNodeType) {
        assert !putter.equals(delayedNode);

        List<Integer> keys = new ArrayList<>(valuesCnt);

        int key = 0;

        keys.add(key);

        while (keys.size() < valuesCnt) {
            if (delayedNode.equals(backupNodes(key, "cache").get(0)))
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
    private static void blockMessage(Ignite grid, Class<? extends Message> cl) {
        ((TestRecordingCommunicationSpi)grid.configuration().getCommunicationSpi())
            .blockMessages((n, m) -> cl.isAssignableFrom(m.getClass()));
    }

    /**
     * Transaction node type.
     */
    private enum TxNodeType {
        /** 'Fat' client. */
        CLIENT,

        /** Server node, not primary. */
        SERVER
    }
}