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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkCutType.AFTER_VERSION_UPDATE;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.PRIMARY;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** Base class for testing Consistency Cut blocking some events. */
public abstract class AbstractConsistentCutBlockingTest extends AbstractConsistentCutTest {
    /** Number of current testing case. */
    private int caseNum;

    /** Latch that blocks transaction execution. */
    private static volatile CountDownLatch txLatch = new CountDownLatch(0);

    /** Latch that blocks writing Consistent Cut record on single node. */
    private static volatile CountDownLatch cutBlkLatch = new CountDownLatch(0);

    /**
     * Latch that counts down when Consistent Cut procedure started on every node. It means that every node writes
     * ConsistentCutStartRecord to WAL and publish its state.
     */
    private static volatile CountDownLatch cutGlobalStartLatch = new CountDownLatch(0);

    /** */
    private final Map<IgniteUuid, Integer> txNearNode = new ConcurrentHashMap<>();

    /** */
    private static BlkNodeType cutBlkNodeType;

    /** */
    private static UUID cutBlkNodeId;

    /** */
    private static BlkCutType cutBlkType;

    /** */
    private static BlkNodeType txBlkNodeType;

    /** */
    private static UUID txBlkNodeId;

    /** */
    private static TransactionState txBlkState;

    /** */
    private static String txMsgBlkCls;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new BlockingCommunicationSpi());

        cfg.setPluginProviders(
            new BlockingWALPluginProvider(),
            new BlockingConsistentCutPluginProvider());

        return cfg;
    }

    /** Initialize latches for test cases with blocking tx messages. */
    protected final void initMsgCase(String msgCls, BlkCutType cutBlkType, BlkNodeType cutBlkNode) {
        txBlkState = null;
        txBlkNodeType = null;

        txMsgBlkCls = msgCls;
        AbstractConsistentCutBlockingTest.cutBlkType = cutBlkType;
        cutBlkNodeType = cutBlkNode;
    }

    /** Initialize latches for test cases with blocking WAL tx states. */
    protected final void initWALCase(TransactionState txState, BlkNodeType txBlkNode, BlkCutType cutBlkType, BlkNodeType cutBlkNode) {
        txMsgBlkCls = null;

        txBlkState = txState;
        txBlkNodeType = txBlkNode;

        AbstractConsistentCutBlockingTest.cutBlkType = cutBlkType;
        cutBlkNodeType = cutBlkNode;
    }

    /**
     * Run cases on Ignite cluster.
     *
     * @param cases Collection of cases. Every case is a collection of key description - tuple (primaryNodeId, backupNodeId).
     */
    protected void runCases(List<List<T2<Integer, Integer>>> cases) throws Exception {
        runCases(cases, false);
    }

    /**
     * Run cases on Ignite cluster.
     *
     * @param cases Collection of cases. Every case is a collection of key description - tuple (primaryNodeId, backupNodeId).
     * @param stopClient Whether to stop client during the test.
     */
    protected void runCases(List<List<T2<Integer, Integer>>> cases, boolean stopClient) throws Exception {
        // Ignite coordinator, ordinary server node, client node.
        // If test with stopping client node then use it as near node.
        Set<Integer> nears = stopClient ? F.asSet(nodes()) : F.asSet(0, nodes() - 1, nodes());

        for (int near: nears) {
            for (int cs = 0; cs < cases.size(); cs++) {
                final int n = near;
                final int c = cs;

                runCase(() -> tx(n, cases.get(c), TransactionConcurrency.PESSIMISTIC), near, cases.get(c), stopClient);
                runCase(() -> tx(n, cases.get(c), TransactionConcurrency.OPTIMISTIC), near, cases.get(c), stopClient);
            }
        }
    }

    /** Checks WALs for correct Consistency Cut. */
    protected void checkWalsConsistency() throws Exception {
        checkWalsConsistency(txNearNode, caseNum);
    }

    /** Checks WALs for correct Consistency Cut. */
    protected void checkWalsConsistency(boolean checkAmountTxs) throws Exception {
        checkWalsConsistency(txNearNode, caseNum, checkAmountTxs);
    }

    /**
     * Test case is sequence of steps:
     * 1. Start TX
     * 2. Block TX somewhere (on sending tx message or on writing tx state to WAL)
     * 3. Start Consistent Cut procedure
     * 4. Optionally block consistent cut on single node (Start WAL / Publishing)
     * 5. Await consistent cut started on all nodes (excl blocking node)
     * 5'. Stop client node if needed.
     * 6. Resume transaction
     * 7. Await transaction committed
     * 8. Resume blocking consistent cut
     * 9. Await Consistent Cut finished.
     *
     * @param tx Function that performs transaction.
     * @param nearNodeId ID of near node.
     * @param c Test case - list of tuples (prim, backup) to be written.
     * @param stopClient Whether to stop client node during test.
     */
    protected void runCase(Runnable tx, int nearNodeId, List<T2<Integer, Integer>> c, boolean stopClient) throws Exception {
        long prevVer = grid(0).context().cache().context().consistentCutMgr().latestKnownCutVersion().version();

        initLatches();

        Integer cutBlkNode;

        if (stopClient)
            startClientGrid(nodes());

        if (cutBlkType != BlkCutType.NONE) {
            cutBlkNode = blkNode(nearNodeId, cutBlkNodeType, c);

            assert cutBlkNode != null : c;

            if (cutBlkNode == 0) {
                log.info("SKIP CASE (to avoid block coordinator) " + caseNum + ". Data=" + c + ", nearNodeId=" + nearNodeId);

                if (stopClient)
                    stopGrid(nodes());

                return;
            }

            cutBlkNodeId = grid(cutBlkNode).localNode().id();
        }
        else
            cutBlkNodeId = null;

        caseNum++;

        if (txBlkNodeType != null) {
            Integer id = blkNode(nearNodeId, txBlkNodeType, c);

            assert id != null || txBlkNodeType == BlkNodeType.BACKUP : c;

            if (id != null)
                txBlkNodeId = grid(id).localNode().id();
            else
                txBlkNodeId = null;
        }
        else
            txBlkNodeId = null;

        log.info("START CASE " + caseNum + ". Data=" + c + ", nearNodeId=" + nearNodeId);

        // 1. Start transaction.
        IgniteInternalFuture<?> txFut = multithreadedAsync(() -> {
            tx.run();

            // In some cases tx isn't blocked and reaches this without counting down the latch.
            // Then explicitly unblock awaited threads.
            txLatch.countDown();
        }, 1);

        // 2. Block transaction.
        if (txMsgBlkCls != null && !txMsgBlkCls.contains("Recovery"))
            txLatch.await(100, TimeUnit.MILLISECONDS);

        // 3. Start Consistent Cut procedure concurrently with running transaction.
        triggerConsistentCut();

        // 4-5. Await Consistent Cut version received and handled.
        awaitGlobalCutVersionReceived(prevVer + 1);

        // Stop client node if needed.
        if (stopClient) {
            assert nearNodeId == nodes() : "Wrong test configuration. Stopping node doesn't affect it.";

            stopGrid(nodes());

            // Await blocks on recovery messages.
            if (txMsgBlkCls != null && txMsgBlkCls.contains("Recovery"))
                txLatch.await(100, TimeUnit.MILLISECONDS);
        }

        // 6. Resume the blocking transaction.
        cutGlobalStartLatch.countDown();

        // 7. Await transaction completed.
        if (!stopClient)
            txFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        // 8. Resume the blocking Consistent Cut.
        cutBlkLatch.countDown();

        // 9. Await while Consistent Cut completed.
        awaitGlobalCutReady(prevVer + 1, true);
    }

    /** Init latches depending on test parameters. */
    private void initLatches() {
        txLatch = new CountDownLatch(1);

        cutGlobalStartLatch = new CountDownLatch(1);

        if (cutBlkType != BlkCutType.NONE)
            cutBlkLatch = new CountDownLatch(1);
    }

    /**
     * Function that runs transaction with specified keys.
     *
     * @param near ID of node to coordinate a transaction.
     * @param keys List of pairs { primary -> backup } for keys to participate in tx.
     */
    private void tx(int near, List<T2<Integer, Integer>> keys, TransactionConcurrency concurrency) {
        try (Transaction tx = grid(near).transactions().txStart(concurrency, SERIALIZABLE)) {
            txNearNode.put(tx.xid(), near);

            for (T2<Integer, Integer> desc: keys) {
                int primary = desc.getKey();
                Integer backup = desc.getValue();

                ClusterNode backupNode = backup == null ? null : grid(backup).localNode();

                int key = key(CACHE, grid(primary).localNode(), backupNode);

                grid(near).cache(CACHE).put(key, key);
            }

            tx.commit();
        }
    }

    /** Finds ID of node to be blocked during a case. */
    private Integer blkNode(int nearNodeId, BlkNodeType blkNodeType, List<T2<Integer, Integer>> c) {
        Integer node = null;

        if (blkNodeType == NEAR)
            node = nearNodeId;
        else if (blkNodeType == PRIMARY) {
            for (T2<Integer, Integer> cc: c) {
                node = cc.get1();

                if (node != 0)
                    break;
            }

        }
        else {
            for (T2<Integer, Integer> cc: c) {
                node = cc.get2();

                if (node == null || node != 0)
                    break;
            }
        }

        return node;
    }

    /** Manually triggers new Consistent Cut. */
    private void triggerConsistentCut() {
        grid(0).context().cache().context().consistentCutMgr().triggerConsistentCutOnCluster("explicit");
    }

    /** Blocks sending transaction message between nodes, and awaits for Consistent Cut procedure starts on every node. */
    private static class BlockingCommunicationSpi extends LogCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (blkMessage(msg)) {
                try {
                    if (txLatch.getCount() > 0) {
                        txLatch.countDown();

                        cutGlobalStartLatch.await(100, TimeUnit.MILLISECONDS);
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            super.sendMessage(node, msg, ackC);
        }

        /** */
        private boolean blkMessage(Message msg) {
            if (msg instanceof GridIoMessage) {
                msg = ((GridIoMessage)msg).message();

                return msg.getClass().getSimpleName().equals(txMsgBlkCls);
            }

            return false;
        }
    }

    /** */
    private static class BlockingWALPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "BlockingWALProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (IgniteWriteAheadLogManager.class.equals(cls))
                return (T)new BlockingWALManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** Blocks writing to WAL specific tx record, and awaits for Consistent Cut procedure starts on every node. */
    private static class BlockingWALManager extends FileWriteAheadLogManager {
        /** */
        private final GridKernalContext ctx;

        /**
         * Constructor.
         *
         * @param ctx Kernal context.
         */
        public BlockingWALManager(GridKernalContext ctx) {
            super(ctx);

            this.ctx = ctx;
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord record) throws IgniteCheckedException, StorageException {
            if (blkRecord(record)) {
                try {
                    txLatch.countDown();

                    cutGlobalStartLatch.await(100, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteCheckedException(e);
                }
            }

            return super.log(record);
        }

        /** */
        private boolean blkRecord(WALRecord record) {
            return record instanceof TxRecord
                && ((TxRecord)record).state() == txBlkState
                && ctx.localNodeId().equals(txBlkNodeId);
        }
    }

    /** */
    private static class BlockingConsistentCutPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "BlockingConsistentCutProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (ConsistentCutManager.class.equals(cls))
                return (T)new BlockingConsistentCutManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** Blocks Consistent Cut procedure on writing ConsistentCutStartRecord or preparing and publishing Consistent Cut state. */
    private static class BlockingConsistentCutManager extends TestConsistentCutManager {
        /** */
        private final GridKernalContext ctx;

        /** {@inheritDoc} */
        @Override protected ConsistentCut newConsistentCut() {
            return new BlockingConsistentCut(ctx.cache().context());
        }

        /** */
        public BlockingConsistentCutManager(GridKernalContext ctx) {
            this.ctx = ctx;
        }

        /** */
        private final class BlockingConsistentCut extends ConsistentCut {
            /** */
            BlockingConsistentCut(GridCacheSharedContext<?, ?> cctx) {
                super(cctx);
            }

            /** Blocks before or after ConsistentCut preparation. */
            @Override protected void init(ConsistentCutVersion ver) throws IgniteCheckedException {
                if (blkCut(AFTER_VERSION_UPDATE)) {
                    try {
                        cutBlkLatch.await(100, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException(e);
                    }
                }

                super.init(ver);
            }
        }

        /** */
        private boolean blkCut(BlkCutType type) {
            return type == cutBlkType && ctx.localNodeId().equals(cutBlkNodeId);
        }
    }

    /** */
    protected enum BlkNodeType {
        /** */
        NEAR,

        /** */
        PRIMARY,

        /** */
        BACKUP
    }

    /** */
    protected enum BlkCutType {
        /** */
        NONE,

        /** */
        AFTER_VERSION_UPDATE
    }
}
