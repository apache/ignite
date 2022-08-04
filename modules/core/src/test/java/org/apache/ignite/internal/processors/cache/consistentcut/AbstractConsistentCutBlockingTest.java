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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkCutType.AFTER_VERSION_UPDATE;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkCutType.BEFORE_VERSION_UPDATE;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.BACKUP;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.PRIMARY;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** Base class for testing Consistency Cut blocking some events. */
public abstract class AbstractConsistentCutBlockingTest extends AbstractConsistentCutTest {
    /** Number of current testing case. */
    private int caseNum;

    /** */
    private final Map<IgniteUuid, Integer> txNearNode = new ConcurrentHashMap<>();

    /** */
    private static BlkNodeType cutBlkNodeType;

    /** */
    private static BlkCutType cutBlkType;

    /** */
    private static BlkNodeType txBlkNodeType;

    /** */
    private static TransactionState txBlkState;

    /** */
    private static Class<?> txMsgBlkCls;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setPluginProviders(
            new BlockingWALPluginProvider(),
            new BlockingConsistentCutPluginProvider());

        return cfg;
    }

    /** Initialize latches for test cases with blocking tx messages. */
    protected final void initMsgCase(Class<?> msgCls, BlkNodeType txBlkNode, BlkCutType cutBlkType, BlkNodeType cutBlkNode) {
        txBlkState = null;
        txBlkNodeType = txBlkNode;

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

    /** */
    protected void runWalBlkCase(
        List<T2<Integer, Integer>> testCase,
        int nearNodeId,
        TransactionConcurrency txConcurrency
    ) throws Exception {
        int txBlkNodeId = blkNode(nearNodeId, txBlkNodeType, testCase);

        int cutBlkNodeId = -1;

        if (cutBlkType != BlkCutType.NONE)
            cutBlkNodeId = blkNode(nearNodeId, cutBlkNodeType, testCase);

        if (txBlkNodeId != nodes())
            BlockingWALManager.walMgr(grid(txBlkNodeId)).block();

        log.info("START CASE " + (++caseNum) + ". Data=" + testCase + ", nearNodeId=" + nearNodeId);

        runCase(() -> tx(nearNodeId, testCase, txConcurrency), txBlkNodeId, cutBlkNodeId);

        if (txBlkNodeId != nodes())
            BlockingWALManager.walMgr(grid(txBlkNodeId)).clear();
    }

    /** */
    protected void runMsgBlkCase(
        List<T2<Integer, Integer>> testCase,
        int nearNodeId,
        TransactionConcurrency txConcurrency
    ) throws Exception {
        if (skipMsgTestCase())
            return;

        int txBlkNodeId = blkNode(nearNodeId, txBlkNodeType, testCase);

        int cutBlkNodeId = -1;

        if (cutBlkType != BlkCutType.NONE)
            cutBlkNodeId = blkNode(nearNodeId, cutBlkNodeType, testCase);

        TestRecordingCommunicationSpi.spi(grid(txBlkNodeId)).blockMessages((n, msg) ->
            msg.getClass().equals(txMsgBlkCls)
        );

        log.info("START CASE " + (++caseNum) + ". Data=" + testCase + ", nearNodeId=" + nearNodeId);

        runCase(() -> tx(nearNodeId, testCase, txConcurrency), txBlkNodeId, cutBlkNodeId);
    }

    /** */
    private boolean skipMsgTestCase() {
        if (txMsgBlkCls.equals(GridNearTxPrepareRequest.class) && txBlkNodeType != NEAR)
            return true;

        if (txMsgBlkCls.equals(GridNearTxPrepareResponse.class) && txBlkNodeType == NEAR)
            return true;

        if (txMsgBlkCls.equals(GridNearTxFinishRequest.class) && txBlkNodeType != NEAR)
            return true;

        if (txMsgBlkCls.equals(GridNearTxFinishResponse.class) && txBlkNodeType == NEAR)
            return true;

        if (txMsgBlkCls.equals(GridDhtTxPrepareRequest.class) && txBlkNodeType == BACKUP)
            return true;

        if (txMsgBlkCls.equals(GridDhtTxPrepareResponse.class) && txBlkNodeType != BACKUP)
            return true;

        if (txMsgBlkCls.equals(GridDhtTxFinishRequest.class) && txBlkNodeType == BACKUP)
            return true;

        if (txMsgBlkCls.equals(GridDhtTxFinishResponse.class) && txBlkNodeType == NEAR)
            return true;

        return false;
    }

    /**
     * Run cases on Ignite cluster.
     *
     * @param cases Collection of cases. Every case is a collection of key description - tuple (primaryNodeId, backupNodeId).
     * @param walOrMsg {@code true} for running test cases with blocking WAL writing, {@code false} for blocking messages.
     */
    protected void runCases(List<List<T2<Integer, Integer>>> cases, boolean walOrMsg) throws Exception {
        // Ignite coordinator, ordinary server node, client node.
        // If test with stopping client node then use it as near node.
        Set<Integer> nears = F.asSet(0, nodes() - 1, nodes());

        for (int near: nears) {
            for (int c = 0; c < cases.size(); c++) {
                if (walOrMsg) {
                    runWalBlkCase(cases.get(c), near, TransactionConcurrency.PESSIMISTIC);
                    runWalBlkCase(cases.get(c), near, TransactionConcurrency.OPTIMISTIC);
                }
                else {
                    runMsgBlkCase(cases.get(c), near, TransactionConcurrency.PESSIMISTIC);
                    runMsgBlkCase(cases.get(c), near, TransactionConcurrency.OPTIMISTIC);
                }
            }
        }
    }

    /** Checks WALs for correct Consistency Cut. */
    protected void checkWalsConsistency() throws Exception {
        checkWalsConsistency(txNearNode, caseNum);
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
     * @param txBlkNodeId ID of node to block transaction.
     * @param cutBlkNodeId ID of node to block Consistent Cut.
     */
    private void runCase(Runnable tx, int txBlkNodeId, int cutBlkNodeId) throws Exception {
        long prevVer = grid(0).context().cache().context().consistentCutMgr().cutVersion().version();

        // 1. Start transaction.
        IgniteInternalFuture<?> txFut = multithreadedAsync(() -> {
            tx.run();

            // In some cases tx isn't blocked and reaches this without counting down the latch.
            // Then explicitly unblock awaited threads.
            unblockTx(grid(txBlkNodeId));
        }, 1);

        // 2. Block transaction.
        blockTx(grid(txBlkNodeId));

        // 3. Start Consistent Cut procedure concurrently with running transaction.
        if (cutBlkNodeId != -1)
            BlockingConsistentCutManager.cutMgr(grid(cutBlkNodeId)).block(cutBlkType);

        multithreadedAsync(this::triggerConsistentCut, 1);

        // 4-5. Await Consistent Cut version received and handled.
        if (cutBlkNodeId != -1)
            BlockingConsistentCutManager.cutMgr(grid(cutBlkNodeId)).awaitBlocked();

        awaitGlobalCutVersionReceived(prevVer + 1, cutBlkNodeId);

        // 6. Resume the blocking transaction.
        unblockTx(grid(txBlkNodeId));

        // 7. Await transaction completed.
        txFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        // 8. Resume the blocking Consistent Cut.
        if (cutBlkNodeId != -1)
            BlockingConsistentCutManager.cutMgr(grid(cutBlkNodeId)).unblock(cutBlkType);

        // 9. Await while Consistent Cut completed.
        awaitGlobalCutReady(prevVer + 1, true);
    }

    /** */
    private void blockTx(IgniteEx blkNode) throws Exception {
        if (txBlkState != null) {
            if (!blkNode.localNode().isClient())
                BlockingWALManager.walMgr(blkNode).awaitBlocked();
        }
        else
            // In some cases blocking node doesn't send required message. Just hangs a little for such cases.
            TestRecordingCommunicationSpi.spi(blkNode).waitForBlocked(1, 100, 100);
    }

    /** */
    private void unblockTx(IgniteEx blkNode) {
        if (txBlkState != null) {
            if (!blkNode.localNode().isClient())
                BlockingWALManager.walMgr(blkNode).unblock();
        }
        else
            TestRecordingCommunicationSpi.spi(blkNode).stopBlock();
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
    private int blkNode(int nearNodeId, BlkNodeType blkNodeType, List<T2<Integer, Integer>> c) {
        if (blkNodeType == NEAR)
            return nearNodeId;
        else if (blkNodeType == PRIMARY)
            return c.get(0).get1();
        else
            return c.get(0).get2();
    }

    /** Manually triggers new Consistent Cut. */
    private void triggerConsistentCut() {
        grid(0).context().cache().context().consistentCutMgr().triggerConsistentCutOnCluster("explicit");
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
        private volatile CountDownLatch latch;

        /** */
        private volatile boolean blocked;

        /**
         * Constructor.
         *
         * @param ctx Kernal context.
         */
        public BlockingWALManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** */
        static BlockingWALManager walMgr(IgniteEx ign) {
            return (BlockingWALManager)ign.context().cache().context().wal();
        }

        /** */
        public void block() {
            latch = new CountDownLatch(1);
        }

        /** */
        public void unblock() {
            latch.countDown();
        }

        /** */
        public void awaitBlocked() throws Exception {
            GridTestUtils.waitForCondition(() -> blocked, 60_000, 10);
        }

        /** */
        public void clear() {
            blocked = false;
            latch = null;
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord record) throws IgniteCheckedException, StorageException {
            if (blkRecord(record)) {
                blocked = true;

                U.awaitQuiet(latch);
            }

            return super.log(record);
        }

        /** */
        private boolean blkRecord(WALRecord record) {
            return latch != null
                && latch.getCount() > 0
                && record instanceof TxRecord
                && ((TxRecord)record).state() == txBlkState;
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
    static class BlockingConsistentCutManager extends TestConsistentCutManager {
        /** */
        private final GridKernalContext ctx;

        /** Latch that blocks before Consistent Cut Version updated. */
        private volatile CountDownLatch beforeVerUpd;

        /** Latch that blocks after Consistent Cut Version updated. */
        private volatile CountDownLatch afterVerUpd;

        /** */
        private volatile boolean blocked;

        /** */
        static BlockingConsistentCutManager cutMgr(IgniteEx ign) {
            return (BlockingConsistentCutManager)ign.context().cache().context().consistentCutMgr();
        }

        /** */
        public void block(BlkCutType blkType) {
            if (blkType == BEFORE_VERSION_UPDATE)
                beforeVerUpd = new CountDownLatch(1);
            else if (blkType == AFTER_VERSION_UPDATE)
                afterVerUpd = new CountDownLatch(1);
        }

        /** */
        public void unblock(BlkCutType blkType) {
            blocked = false;

            // It's safe to do that, because Consistent Cut's never invoke concurrently.
            if (blkType == BEFORE_VERSION_UPDATE) {
                CountDownLatch latch = beforeVerUpd;

                beforeVerUpd = null;

                latch.countDown();
            }
            else if (blkType == AFTER_VERSION_UPDATE) {
                CountDownLatch latch = afterVerUpd;

                afterVerUpd = null;

                latch.countDown();
            }
        }

        /** */
        public void awaitBlocked() throws Exception {
            GridTestUtils.waitForCondition(() -> blocked, 60_000, 10);
        }

        /** {@inheritDoc} */
        @Override protected ConsistentCut newConsistentCut() {
            if (beforeVerUpd != null) {
                blocked = true;

                try {
                    // Just hang for 100ms to make other threads do their work.
                    U.await(beforeVerUpd, 100, TimeUnit.MILLISECONDS);
                }
                catch (IgniteInterruptedCheckedException e) {
                    // No-op.
                }
            }

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
                if (afterVerUpd != null) {
                    blocked = true;

                    U.awaitQuiet(afterVerUpd);
                }

                super.init(ver);
            }
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
        BEFORE_VERSION_UPDATE,

        /** */
        AFTER_VERSION_UPDATE
    }
}
