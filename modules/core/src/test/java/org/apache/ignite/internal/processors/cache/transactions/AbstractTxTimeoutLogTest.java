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
import java.util.TreeMap;
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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedBaseMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests log content of timeouted optimistic transaction.
 */
@RunWith(Parameterized.class)
public abstract class AbstractTxTimeoutLogTest extends GridCommonAbstractTest {
    /** */
    private static final int TX_TIMEOUT = 3_000;

    /** */
    private static final int VALUES_CNT = 30;

    /** */
    protected TransactionConcurrency txConcurrency;

    /** */
    protected final Map<String, ListeningTestLogger> logs = new HashMap<>();

    /** Cache sync mode. */
    @Parameterized.Parameter(0)
    public CacheWriteSynchronizationMode cacheSyncMode;

    /** Transaction node type (initiator). */
    @Parameterized.Parameter(1)
    public TxNodeType txNodeType;

    /** Transaction isolation. */
    @Parameterized.Parameter(2)
    public TransactionIsolation txIsolation;

    /** Number of the cache backups. 1 for one-phase commit. */
    @Parameterized.Parameter(3)
    public int backups;

    /** Run params set. */
    @Parameterized.Parameters(name = "syncMode={0},txNode={1},isolation={2},backups={3}")
    public static Iterable<Object[]> params() {
        return cartesianProduct(
            // Sync mode.
            F.asList(FULL_SYNC, PRIMARY_SYNC),
            // Transaction initiator type.
            F.asList(TxNodeType.values()),
            // Transaction isolation level.
            F.asList(TransactionIsolation.values()),
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

        logs.put(igniteInstanceName, ll);

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

        startGridsMultiThreaded(backups + 2);

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

    /** Test no 'not responded nodes' message is when backup just leaves on prepare phase. */
    @Test
    public void testBackupLeftOnPrepare() throws Exception {
        IgniteEx putter = txNode(txNodeType);
        IgniteEx gone = txNode(TxNodeType.SERVER_DELAYED);

        if (putter == gone)
            return;

        LogListener backupLsnr = LogListener.matches("Detected unresponded backup nodes").build();

        logs.values().forEach(l -> l.registerListener(backupLsnr));

        LogListener txNodeLogLsnr = txNodeLsnr(putter.localNode().id(), null);

        IgniteEx primary = txNode(TxNodeType.SERVER);

        List<Integer> keys = IntStream.range(0, 10_000).boxed().filter(key -> {
            Ignite prim = primaryNode(key, "cache");
            List<Ignite> backs = backupNodes(key, "cache");

            return prim.equals(primary) && backs.contains(gone);
        }).limit(VALUES_CNT).collect(Collectors.toList());

        assert keys.size() == VALUES_CNT;

        blockMessage(gone, GridDhtTxPrepareResponse.class);

        IgniteInternalFuture<?> txFut = runAsync(() -> doTx(putter, keys));

        ((TestRecordingCommunicationSpi)gone.configuration().getCommunicationSpi()).waitForBlocked();

        stopGrid(gone.name(), true);

        try {
            txFut.get();

            // Backup gone. Transaction is complete.
            for (int i = 0; i < keys.size(); ++i)
                assertEquals(updatedValue(keys.get(i)), txNode(TxNodeType.SERVER).cache("cache").get(keys.get(i)));

            assertFalse(txNodeLogLsnr.check());
            assertFalse(backupLsnr.check());
        }
        finally {
            // Make reusable.
            startGrid(gone.name());
        }
    }

    /**
     * Test no 'not responded nodes' messages are not when some primary node just leaves at prephare phase.
     */
    @Test
    public void testPrimaryLeftOnNearPrepare() throws Exception {
        doTestPrimaryLeft(GridNearTxPrepareResponse.class);
    }

    /**
     * Does test that no 'not responded nodes' messages are not when some primary node just leaves at prephare phase.
     */
    protected void doTestPrimaryLeft(Class<? extends GridDistributedBaseMessage> msgToBlock) throws Exception {
        IgniteEx putter = txNode(txNodeType);
        IgniteEx gone = txNode(TxNodeType.SERVER_DELAYED);

        // We are not interrested here if initiator leaves or if there is no other primaries.
        if (putter == gone)
            return;

        LogListener txNodeLogLsnr = txNodeLsnr(putter.localNode().id(), null);

        LogListener backupLsnr = LogListener.matches("Detected unresponded backup nodes").build();

        logs.values().forEach(l -> l.registerListener(backupLsnr));

        List<Integer> keys = keys(putter, gone, true);

        assert keys.size() == VALUES_CNT;

        // Ensure single key is for the gone primary node.
        assert keys.size() != 1 || primaryNode(keys.get(0), "cache").equals(gone);
        // If several keys, ensure they are for several primaries, not for just one.
        assert keys.size() < 2 || keys.stream().anyMatch(key -> !primaryNode(key, "cache").equals(gone));

        blockMessage(gone, msgToBlock);

        IgniteInternalFuture<?> txFut = runAsync(() -> doTx(putter, keys));

        ((TestRecordingCommunicationSpi)gone.configuration().getCommunicationSpi()).waitForBlocked();

        stopGrid(gone.name(), true);

        try {
            assertTrue(txFut.get() instanceof Exception);

            IgniteCache<Integer, Integer> c0 = txNode(TxNodeType.SERVER).cache("cache");

            // A primary gone. Pessimistic transaction is not complete when a primary leaves.
            for (int i = 0; i < keys.size(); ++i)
                assertEquals(keys.get(i), c0.get(keys.get(i)));

            // But has no 'not responded nodes' in the log.
            assertFalse(txNodeLogLsnr.check());
            assertFalse(backupLsnr.check());
        }
        finally {
            // Make reusable.
            startGrid(gone.name());
        }
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
        G.allGrids().forEach(ign -> logs.get(ign.name()).registerListener(txNodeLsnr));

        // One-phase commit (and some other cases, see IGNITE-19336) doesn't wait apply the finish/prepare command and
        // doesn't use prepare timeout on the near/primary node. Also, we don't need to wait on primry if we block it.
        List<LogListener> backupsLsnrs = onPrimary || backups < 2
            ? Collections.emptyList()
            : primaryLogListeners(primaries, delayed, msgToDelay);

        IgniteInternalFuture<Exception> txFut = runAsync(() -> doTx(putter, keys));

        // If {@code true} the timeout is raised by local timer or received as a tx error from certain remote node.
        boolean localTimeout = true;

        if (!txNodeLsnr.check(TX_TIMEOUT * 3)) {
            Exception err = txFut.get();

            assertTrue("Transaction timeout not detected.", hasCause(err, IgniteTxTimeoutCheckedException.class));

            localTimeout = false;
        }

        // If timeout detected on putter, we should see an unresponded node.
        if (localTimeout) {
            // Nodes of caught blocked messages to.
            Set<UUID> notRespondedTo =
                ((TestRecordingCommunicationSpi)delayed.configuration().getCommunicationSpi()).blockedMessages().stream()
                    .map(bm -> bm.destinationNode().id()).collect(Collectors.toSet());

            // At least one primary sould not respond yet if we detect timeout locally.
            assertTrue("Unresponded primary not detected.", !unespondedPrimaries.isEmpty());

            if (onPrimary) {
                assertTrue("Not found unresponded primary.", unespondedPrimaries.size() == 1 &&
                    unespondedPrimaries.containsKey(delayed.cluster().localNode().id()));
            }
            else {
                for (UUID nid : unespondedPrimaries.keySet())
                    assertTrue("Unexpected notresponded primary node.", notRespondedTo.contains(nid));
            }
        }

        // In any case we sould see unresponded backups if any.
        checkBackupNotRespondedDetected(backupsLsnrs);
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
    protected static LogListener txNodeLsnr(UUID txNodeId, @Nullable Map<UUID, Integer> unresponded) {
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

                if (unresponded == null)
                    return;

                String t = "Detected unresponded primary nodes: ";

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
                    "]. Detected unresponded backup nodes: " + delayed.localNode().id())
                .times(msgToDelay == null ? 0 : 1).build();

            res.add(lsnr);
        });

        // Since we got various singleton static loggers, we check all the loggers for the exact results.
        grid(0).cluster().forServers().nodes().forEach(n -> {
            ListeningTestLogger serverLog = logs.get(grid(n).name());

            serverLog.registerAllListeners(res.toArray(new LogListener[res.size()]));
        });

        return res;
    }

    /**
     * Performs ransaction.
     */
    protected Exception doTx(Ignite putter, Collection<Integer> keys) {
        Map<Integer, Integer> data = new TreeMap<>();

        keys.forEach(k -> data.put(k, updatedValue(k)));

        IgniteCache<Integer, Integer> cache = putter.cache("cache");

        try (Transaction tx = putter.transactions().txStart()) {
            cache.putAll(data);

            tx.commit();
        }
        catch (Exception e) {
            log.warning("Unable to commit transaction.", e);

            return e;
        }

        return null;
    }

    /**
     * @return Transaction node (initiator) type.
     */
    protected IgniteEx txNode(TxNodeType txNodeType) throws Exception {
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
    protected List<Integer> keys(IgniteEx putter, IgniteEx delayedNode, boolean primary) {
        assert txNodeType != TxNodeType.SERVER_DELAYED ||
            (putter.equals(delayedNode) && !putter.cluster().localNode().isClient());

        List<Integer> keys = new ArrayList<>(VALUES_CNT);

        int key = 0;

        while (keys.isEmpty()) {
            if (delayedNode.equals(primary ? primaryNode(key, "cache") : backupNode(key, "cache")))
                keys.add(key);

            ++key;
        }

        keys.addAll(IntStream.range(key + 1, key + VALUES_CNT).boxed().collect(Collectors.toList()));

        return keys;
    }

    /** */
    protected static void blockMessage(Ignite grid, Class<? extends GridDistributedBaseMessage> cl) {
        ((TestRecordingCommunicationSpi)grid.configuration().getCommunicationSpi())
            .blockMessages((n, m) -> cl.isAssignableFrom(m.getClass()));
    }

    /** */
    protected static Integer updatedValue(Integer key) {
        return key + 1;
    }

    /**
     * Transaction node (initiator) type.
     */
    protected enum TxNodeType {
        /** 'Fat' client. */
        CLIENT,

        /** Server node, not delayed. */
        SERVER,

        /** Not-responding server node. */
        SERVER_DELAYED
    }
}
