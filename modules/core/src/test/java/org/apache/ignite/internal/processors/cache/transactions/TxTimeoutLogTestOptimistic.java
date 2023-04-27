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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedBaseMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 * Tests log content of timeouted optimistic transaction.
 */
@RunWith(Parameterized.class)
public class TxTimeoutLogTestOptimistic extends GridCommonAbstractTest {
    /** */
    private static final int TX_TIMEOUT = 3_000;

    /** */
    protected TransactionConcurrency txConcurrency = OPTIMISTIC;

    /** */
    protected Map<String, ListeningTestLogger> nodesLogs = new HashMap<>();

    /** Cache sync mode. */
    @Parameterized.Parameter(0)
    public CacheWriteSynchronizationMode cacheSyncMode;

    /**
     * Number of records to put within single transaction.
     */
    @Parameterized.Parameter(1)
    public int valuesCnt;

    /** Transaction node type (initiator). */
    @Parameterized.Parameter(2)
    public TxNodeType txNodeType;

    /** Transaction isolation. */
    @Parameterized.Parameter(3)
    public TransactionIsolation txIsolation;

    /** Number of the cache backups. 1 for one-phase commit. */
    @Parameterized.Parameter(4)
    public int backups;

    /** Run params set. */
    @Parameterized.Parameters(name = "syncMode={0},records={1},txNode={2},isolation={3},backups={4}")
    public static Iterable<Object[]> params() {
        return cartesianProduct(
            // Sync mode.
            F.asList(FULL_SYNC, PRIMARY_SYNC),
            // Number of records to change.
            F.asList(1, 20),
            // Transaction initiator type.
            F.asList(TxNodeType.CLIENT),
            // Transaction isolation level.
            F.asList(TransactionIsolation.READ_COMMITTED),
            // Number of backups / one phase commit.
            F.asList(2, 1)
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ((TestRecordingCommunicationSpi)txNode(TxNodeType.SERVER_DELAYED).configuration().getCommunicationSpi())
            .stopBlock();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTransactionConfiguration(new TransactionConfiguration()
            .setDefaultTxConcurrency(txConcurrency)
            .setDefaultTxIsolation(txIsolation)
            .setDefaultTxTimeout(TX_TIMEOUT)
        );

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        ListeningTestLogger ll = new ListeningTestLogger(cfg.getGridLogger());

        nodesLogs.put(igniteInstanceName, ll);

        cfg.setGridLogger(ll);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Clean static singletoned logs for the log listeners.
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

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10_000 + TX_TIMEOUT * 5;
    }

    /**
     * Test unresponding nodes logged if backup delays the prepare request.
     */
    @Test
    public void testBackupDelaysOnPrepare() throws Exception {
        doTest(false, GridDhtTxPrepareResponse.class);
    }

    /**
     * Test unresponded primary is logged if it delays the prepare request.
     */
    @Test
    public void testPrimaryDelaysOnPrepare() throws Exception {
        doTest(true, GridNearTxPrepareResponse.class);
    }

    /**
     * Test unresponded nodes are logged or transaction is successful.
     *
     * @param onPrimary If {@code true}, blocked node is considered as primary node.
     * @param msgToDelay Transaction message to block on the target node {@code txNodeType}.
     */
    protected void doTest(boolean onPrimary, Class<? extends GridDistributedBaseMessage> msgToDelay) throws Exception {
        IgniteEx delayed = txNode(TxNodeType.SERVER_DELAYED);

        blockMessage(delayed, msgToDelay);

        IgniteEx putter = txNode(txNodeType);

        IgniteCache<Integer, Integer> cache = putter.cache("cache");

        List<Integer> keys = keys(putter, delayed, onPrimary);

        // Tx initiator node and tode to block response is thesame server node. Nothing expected to stuck. The tx
        // should work normally.
        if (onPrimary && txNodeType == TxNodeType.SERVER_DELAYED) {
            doTx(putter, keys);

            for (int i = 0; i < keys.size(); ++i)
                assertEquals(updatedValue(keys.get(i)), cache.get(keys.get(i)));

            return;
        }

        Collection<ClusterNode> primaries = awaitedPrimaries(delayed, putter, onPrimary, keys);

        Map<UUID, Integer> unespondedPrimaries = new ConcurrentHashMap<>();

        LogListener txNodeLsnr = txNodeLsnr(putter.localNode().id(), unespondedPrimaries);

        // Since we got various singleton static loggers, we check all the loggers for the exact results indicated by
        // 'nodeId=' for the transaction node.
        G.allGrids().forEach(ign -> nodesLogs.get(ign.name()).registerListener(txNodeLsnr));

        // One-phase commit (and some other cases, see IGNITE-19336) doesn't wait apply the finish/prepare command and
        // doesn't use prepare timeout on the near/primary node. Also, we don't need to wait on primry if we block it.
        List<LogListener> backupsLsnrs = onPrimary || backups < 2
            ? Collections.emptyList()
            : primaryLogListeners(primaries, delayed, msgToDelay);

        runAsync(() -> doTx(putter, keys));

        assertTrue("Transaction timeout message not detected.", txNodeLsnr.check(TX_TIMEOUT * 3));

        checkBackupNotRespondedDetected(backupsLsnrs);

        Set<UUID> notRespondedTo =
            ((TestRecordingCommunicationSpi)delayed.configuration().getCommunicationSpi()).blockedMessages().stream()
                .map(bm -> bm.destinationNode().id()).collect(Collectors.toSet());

        assertEquals("Unexpected number of not responded primary nodes.", notRespondedTo.size(),
            unespondedPrimaries.size());

        if (!onPrimary) {
            for (UUID nid : notRespondedTo)
                assertTrue("Not found not responded primary " + nid, unespondedPrimaries.containsKey(nid));
        }
    }

    /**
     * if required, checks if delayed backup is detected.
     */
    private void checkBackupNotRespondedDetected(Collection<LogListener> lsnrs) throws InterruptedException {
        CountDownLatch backupsFlag = new CountDownLatch(1);

        lsnrs.forEach(bLsnr -> runAsync(() -> {
            if (bLsnr.check(TX_TIMEOUT))
                backupsFlag.countDown();
        }));

        backupsFlag.await(TX_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * Extracts primary nodes to watch for 'not responded primary...' on the transaction timeout.
     */
    private Collection<ClusterNode> awaitedPrimaries(IgniteEx delayed, IgniteEx putter, boolean delayOnPrimary,
        Collection<Integer> keys) {
        assert !delayed.cluster().localNode().isClient();

        if (delayOnPrimary)
            return Collections.singletonList(delayed.localNode());

        // All primary nodes for the keys where the delayed is backup for each key too.
        Set<ClusterNode> res = grid(0).cluster().forServers().nodes().stream().filter(n -> keys.stream()
            .anyMatch(k -> primaryNode(k, "cache").cluster().localNode().equals(n)
                && backupNodes(k, "cache").contains(delayed))).collect(Collectors.toSet());

        if (txNodeType == TxNodeType.SERVER)
            res.remove(putter.localNode());

        return res;
    }

    /**
     * Log listener storing detected unresponded primary ids to {@code unresponded}.
     *
     * @param txNodeId Id of current transaction processing node.
     * @param unresponded Ids of detected unresponded primaries.
     * @return Log listener for transaction initiator node.
     */
    private static LogListener txNodeLsnr(UUID txNodeId, Map<UUID, Integer> unresponded) {
        return new LogListener() {
            private final AtomicBoolean timeoutDetected = new AtomicBoolean();

            @Override public boolean check() {
                return timeoutDetected.get();
            }

            @Override public synchronized void reset() {
                timeoutDetected.set(false);

                unresponded.clear();
            }

            @Override public void accept(String m) {
                if (!m.startsWith("The transaction was forcibly rolled back because a timeout is reached: " +
                    "GridNearTxLocal[") || !m.contains("nodeId=" + txNodeId))
                    return;

                timeoutDetected.set(true);

                String t = "Not responded primary nodes (or their backups): ";

                int idx = m.indexOf(t);

                if (idx > 0) {
                    m = m.substring(idx + t.length());

                    if (m.endsWith("."))
                        m = m.substring(0, m.length() - 1);

                    Stream.of(m.split(","))
                        .map(UUID::fromString).forEach(nid -> unresponded.compute(nid, (nid0, cnt) -> {
                            if (cnt == null)
                                cnt = 0;

                            return ++cnt;
                        }));
                }
            }
        };
    }

    /**
     * @return Log listeners on {@code waitingPrimaries} watching unresponded {@code delayed}.
     */
    private List<LogListener> primaryLogListeners(Collection<ClusterNode> waitingPrimaries, IgniteEx delayed,
        Class<? extends GridDistributedBaseMessage> msgToDelay) {
        List<LogListener> res = new ArrayList<>();

        waitingPrimaries.forEach(pn -> {
            // Certain message only for current primary/near note indicated by 'nodeId='
            LogListener lsnr = LogListener.matches(", nodeId=" + pn.id() +
                    "]. Not responded backup nodes: " + delayed.localNode().id())
                .times(msgToDelay == null ? 0 : 1).build();

            res.add(lsnr);
        });

        // Since we got various singleton static loggers, we check all the loggers for the exact results.
        grid(0).cluster().forServers().nodes().forEach(n -> {
            ListeningTestLogger serverLog = nodesLogs.get(grid(n).name());

            serverLog.registerAllListeners(res.toArray(new LogListener[res.size()]));
        });

        return res;
    }

    /**
     * Performs ransaction.
     */
    private void doTx(Ignite putter, Collection<Integer> keys) {
        IgniteCache<Integer, Integer> cache = putter.cache("cache");

        try (Transaction tx = putter.transactions().txStart()) {
            keys.forEach(k -> cache.put(k, updatedValue(k)));

            tx.commit();
        }
    }

    /**
     * @return Transaction node (initiator) type.
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
     * @return cache keys for current test to use in transaction. At least the first key is always for the delayed node.
     */
    private List<Integer> keys(IgniteEx putter, IgniteEx delayedNode, boolean primary) {
        assert txNodeType != TxNodeType.SERVER_DELAYED ||
            (putter.equals(delayedNode) && !putter.cluster().localNode().isClient());

        List<Integer> keys = new ArrayList<>(valuesCnt);

        int key = 0;

        while (keys.isEmpty()) {
            if (delayedNode.equals(primary ? primaryNode(key, "cache") : backupNode(key, "cache")))
                keys.add(key);

            ++key;
        }

        keys.addAll(IntStream.range(key + 1, key + valuesCnt).boxed().collect(Collectors.toList()));

        return keys;
    }

    /** */
    private static void blockMessage(Ignite grid, Class<? extends GridDistributedBaseMessage> cl) {
        ((TestRecordingCommunicationSpi)grid.configuration().getCommunicationSpi())
            .blockMessages((n, m) -> cl.isAssignableFrom(m.getClass()));
    }

    /** */
    private static Integer updatedValue(Integer key) {
        return key + 1;
    }

    /**
     * Transaction node (initiator) type.
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
