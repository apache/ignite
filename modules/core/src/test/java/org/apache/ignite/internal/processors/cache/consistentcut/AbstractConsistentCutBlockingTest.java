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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
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
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
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
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
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

        // Skip cases with blocking WAL on clients (no WAL actually)
        if (txBlkNodeId == nodes())
            return;

        if (txBlkNodeId >= 0)
            BlockingWALManager.walMgr(grid(txBlkNodeId)).block();

        log.info("START CASE " + (++caseNum) + ". Data=" + testCase + ", nearNodeId=" + nearNodeId);

        runCase(() -> tx(nearNodeId, testCase, txConcurrency), txBlkNodeId, cutBlkNodeId);

        BlockingWALManager.walMgr(grid(txBlkNodeId)).clear();
    }

    /** */
    protected void runMsgBlkCase(
        List<T2<Integer, Integer>> testCase,
        int nearNodeId,
        TransactionConcurrency txConcurrency
    ) throws Exception {
        int txBlkNodeId = blkNode(nearNodeId, txBlkNodeType, testCase);

        int cutBlkNodeId = -1;

        if (cutBlkType != BlkCutType.NONE)
            cutBlkNodeId = blkNode(nearNodeId, cutBlkNodeType, testCase);

        if (skipMsgTestCase(txBlkNodeId, nearNodeId, testCase))
            return;

        TestRecordingCommunicationSpi.spi(grid(txBlkNodeId)).blockMessages((n, msg) ->
            msg.getClass().equals(txMsgBlkCls)
        );

        log.info("START CASE " + (++caseNum) +
            ". Data=" + testCase +
            ", nearNodeId=" + nearNodeId +
            ", txBlkNodeId=" + txBlkNodeId +
            ", txBlkNodeType=" + txBlkNodeType +
            ", cutBlkNodeId=" + cutBlkNodeId +
            ", msg=" + txMsgBlkCls.getSimpleName());

        runCase(() -> tx(nearNodeId, testCase, txConcurrency), txBlkNodeId, cutBlkNodeId);
    }

    /** */
    private boolean skipMsgTestCase(int txBlkNodeId, int nearNodeId, List<T2<Integer, Integer>> testCase) {
        if (txMsgBlkCls.equals(GridNearTxPrepareRequest.class)) {
            if (txBlkNodeType != NEAR)
                return true;

            return allPrimaryOnNear(testCase, nearNodeId);
        }

        if (txMsgBlkCls.equals(GridNearTxPrepareResponse.class)) {
            if (txBlkNodeType != PRIMARY || txBlkNodeId == nearNodeId)
                return true;

            return allPrimaryOnNear(testCase, nearNodeId);
        }

        if (txMsgBlkCls.equals(GridNearTxFinishRequest.class)) {
            if (txBlkNodeType != NEAR || onePhase(testCase))
                return true;

            return allPrimaryOnNear(testCase, nearNodeId);
        }

        if (txMsgBlkCls.equals(GridNearTxFinishResponse.class)) {
            if (onePhase(testCase) || txBlkNodeType != PRIMARY || txBlkNodeId == nearNodeId)
                 return true;

            return allPrimaryOnNear(testCase, nearNodeId);
        }

        if (txMsgBlkCls.equals(GridDhtTxPrepareRequest.class)) {
            // Near node might send the request to the backup nodes in case if it is collocated.
            if (txBlkNodeType == NEAR)
                return !allPrimaryOnNear(testCase, nearNodeId);

            return txBlkNodeType == BACKUP;
        }

        if (txMsgBlkCls.equals(GridDhtTxPrepareResponse.class))
            return txBlkNodeType != BACKUP;

        if (txMsgBlkCls.equals(GridDhtTxFinishRequest.class)) {
            if (txBlkNodeType == BACKUP || onePhase(testCase))
                return true;

            // Near node might send the request to the backup nodes in case if near node is collocated.
            if (txBlkNodeType == NEAR)
                return !allPrimaryOnNear(testCase, nearNodeId);

            return false;
        }

        return false;
    }

    /** */
    private boolean allPrimaryOnNear(List<T2<Integer, Integer>> testCase, int nearNodeId) {
        // If all primary partitions are on the near node.
        return testCase.stream()
            .map(IgniteBiTuple::get1)
            .allMatch(prim -> prim == nearNodeId);
    }

    /** */
    private boolean onePhase(List<T2<Integer, Integer>> testCase) {
        int prims = testCase.stream()
            .map(T2::get1)
            .collect(Collectors.toSet())
            .size();

        int backups = testCase.stream()
            .map(T2::get2)
            .collect(Collectors.toSet())
            .size();

        return prims == 1 && backups <= 1;
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
        List<Integer> nears = F.asList(0, nodes() - 1, nodes());

        for (int near: nears) {
            // Client nodes doesn't prepare anything after version update.
            if (near == nodes() && cutBlkNodeType == NEAR && cutBlkType == AFTER_VERSION_UPDATE)
                continue;

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
     * 5. Resume transaction
     * 6. Await transaction committed
     * 7. Resume blocking consistent cut
     * 8. Await Consistent Cut finished.
     *
     * @param tx Function that performs transaction.
     * @param txBlkNodeId ID of node to block transaction.
     * @param cutBlkNodeId ID of node to block Consistent Cut.
     */
    private void runCase(Runnable tx, int txBlkNodeId, int cutBlkNodeId) throws Exception {
        // 1. Start transaction.
        IgniteInternalFuture<?> txFut = multithreadedAsync(tx, 1);

        // 2. Await transaction has blocked.
        awaitTxBlocked(grid(txBlkNodeId));

        // 3. Start Consistent Cut procedure concurrently with running transaction.
        if (cutBlkNodeId != -1)
            BlockingConsistentCutManager.cutMgr(grid(cutBlkNodeId)).block(cutBlkType);

        IgniteFuture<Void> cutFut = triggerConsistentCut();

        // 4. Await Consistent Cut has blocked.
        if (cutBlkNodeId != -1)
            BlockingConsistentCutManager.cutMgr(grid(cutBlkNodeId)).awaitBlockedOrFinishedCut(cutFut);

        // 5. Resume the blocking transaction.
        unblockTx(grid(txBlkNodeId));

        // 6. Await transaction completed.
        txFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        // 7. Resume the blocking Consistent Cut.
        if (cutBlkNodeId != -1)
            BlockingConsistentCutManager.cutMgr(grid(cutBlkNodeId)).unblock(cutBlkType);

        // 8. Await while Consistent Cut completed.
        cutFut.get(getTestTimeout());
    }

    /** */
    private void awaitTxBlocked(IgniteEx blkNode) throws Exception {
        if (txBlkState != null)
            BlockingWALManager.walMgr(blkNode).awaitBlocked();
        else
            // In some cases blocking node doesn't send required message. Just hangs a little for such cases.
            TestRecordingCommunicationSpi.spi(blkNode).waitForBlocked(1, 100);
    }

    /** */
    private void unblockTx(IgniteEx blkNode) {
        if (txBlkState != null)
            BlockingWALManager.walMgr(blkNode).unblock();
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
    private IgniteFuture<Void> triggerConsistentCut() {
        awaitAllNodesReadyForIncrementalSnapshot();

        return snp(grid(0)).createIncrementalSnapshot(SNP);
    }

    /** */
    protected void waitForCutIsStartedOnAllNodes() throws Exception {
        GridTestUtils.waitForCondition(() -> {
            boolean allNodeStartedCut = true;

            for (int i = 0; i < nodes(); i++)
                allNodeStartedCut &= BlockingConsistentCutManager.cutMgr(grid(i)).cutFuture() != null;

            return allNodeStartedCut;
        }, getTestTimeout(), 10);
    }

    /** */
    protected void waitForCutIsFinishedOnAllNodes() throws Exception {
        GridTestUtils.waitForCondition(() -> {
            boolean allNodeFinishedCut = true;

            for (int i = 0; i < nodes(); i++)
                allNodeFinishedCut &= BlockingConsistentCutManager.cutMgr(grid(i)).cutFuture() == null;

            return allNodeFinishedCut;
        }, getTestTimeout(), 10);
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
                return (T)new BlockingConsistentCutManager();

            return null;
        }
    }

    /** Blocks Consistent Cut procedure on writing ConsistentCutStartRecord or preparing and publishing Consistent Cut state. */
    static class BlockingConsistentCutManager extends TestConsistentCutManager {
        /** Latch that blocks before Consistent Cut Version updated. */
        private volatile CountDownLatch beforeUpdVer;

        /** Latch that blocks after Consistent Cut Version updated. */
        private volatile CountDownLatch afterUpdVer;

        /** */
        private volatile CountDownLatch blockedLatch;

        /** */
        static BlockingConsistentCutManager cutMgr(IgniteEx ign) {
            return (BlockingConsistentCutManager)ign.context().cache().context().consistentCutMgr();
        }

        /** {@inheritDoc} */
        @Override public void handleConsistentCutId(UUID id) {
            // Do not block transaction threads.
            if (Thread.currentThread().getName().contains("disco")) {
                if (beforeUpdVer != null) {
                    blockedLatch.countDown();
                    blockedLatch = null;

                    U.awaitQuiet(beforeUpdVer);

                    beforeUpdVer = null;
                }
            }

            super.handleConsistentCutId(id);
        }

        /** {@inheritDoc} */
        @Override protected ConsistentCutFuture newConsistentCut(UUID id) {
            return new BlockingConsistentCutFuture(context(), id);
        }

        /** */
        public void block(BlkCutType type) {
            blockedLatch = new CountDownLatch(1);

            if (type == BEFORE_VERSION_UPDATE)
                beforeUpdVer = new CountDownLatch(1);
            else if (type == AFTER_VERSION_UPDATE)
                afterUpdVer = new CountDownLatch(1);
        }

        /** */
        public void awaitBlockedOrFinishedCut(@Nullable IgniteFuture<?> cutFut) {
            CountDownLatch latch = blockedLatch;

            if (latch == null)
                return;

            cutFut.listen((f) -> latch.countDown());

            U.awaitQuiet(latch);
        }

        /** */
        public void unblock(BlkCutType type) {
            if (type == BEFORE_VERSION_UPDATE && beforeUpdVer != null)
                beforeUpdVer.countDown();
            else if (type == AFTER_VERSION_UPDATE && afterUpdVer != null)
                afterUpdVer.countDown();
        }

        /** */
        private final class BlockingConsistentCutFuture extends ConsistentCutFuture {
            /** */
            BlockingConsistentCutFuture(GridCacheSharedContext<?, ?> cctx, UUID id) {
                super(cctx, id);
            }

            /** Blocks before or after ConsistentCut preparation. */
            @Override protected void init() throws IgniteCheckedException {
                if (afterUpdVer != null) {
                    blockedLatch.countDown();
                    blockedLatch = null;

                    U.awaitQuiet(afterUpdVer);

                    afterUpdVer = null;
                }

                super.init();
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
        AFTER_VERSION_UPDATE,
    }
}
