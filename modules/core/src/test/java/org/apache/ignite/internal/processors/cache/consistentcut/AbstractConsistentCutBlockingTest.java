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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
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
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.PRIMARY;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** Base class for testing Consistency Cut blocking some events. */
public abstract class AbstractConsistentCutBlockingTest extends AbstractConsistentCutTest {
    /** */
    private int caseNum;

    /** Latch that blocks transaction execution. */
    private static volatile CountDownLatch txLatch = new CountDownLatch(0);

    /** Latch that blocks publishing Consistent Cut state on single node. */
    private static volatile CountDownLatch cutPublishBlkLatch = new CountDownLatch(0);

    /** Latch that blocks writing Consistent Cut record on single node. */
    private static volatile CountDownLatch cutWalBlkLatch = new CountDownLatch(0);

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

    /** Use cached thread pool executor, because tests may run > 100 tests cases (every case spawns a thread for running tx). */
    protected static final ExecutorService exec = Executors.newCachedThreadPool(
        new IgniteThreadFactory("ConsistentCutTest", "consistent-cut-blk-test"));

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
        // Ignite coordinator, ordinary server node, client node.
        Set<Integer> nears = F.asSet(0, nodes() - 1, nodes());

        for (int near: nears) {
            for (int cs = 0; cs < cases.size(); cs++) {
                final int n = near;
                final int c = cs;

                runCase(() -> tx(n, cases.get(c), TransactionConcurrency.PESSIMISTIC), near, cases.get(c));
                runCase(() -> tx(n, cases.get(c), TransactionConcurrency.OPTIMISTIC), near, cases.get(c));
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
     * 6. Resume transaction
     * 7. Await transaction committed
     * 8. Resume blocking consistent cut
     * 9. Await Consistent Cut finished.
     *
     * @param tx Function that performs transaction.
     * @param nearNodeId ID of near node.
     * @param c Test case - list of tuples (prim, backup) to be written.
     */
    protected void runCase(Runnable tx, int nearNodeId, List<T2<Integer, Integer>> c) throws Exception {
        long prevVer = grid(0).context().cache().context().consistentCutMgr().latestKnownCutVersion();

        initLatches();

        Integer cutBlkNode = null;

        if (cutBlkType != BlkCutType.NONE) {
            cutBlkNode = blkNode(nearNodeId, cutBlkNodeType, c);

            assert cutBlkNode != null : c;

            if (cutBlkNode == 0) {
                log.info("SKIP CASE (to avoid block coordinator) " + caseNum + ". Data=" + c + ", nearNodeId=" + nearNodeId);

                return;
            }

            cutBlkNodeId = grid(cutBlkNode).localNode().id();
        }
        else
            cutBlkNodeId = null;

        caseNum++;

        log.info("START CASE " + caseNum + ". Data=" + c + ", nearNodeId=" + nearNodeId);

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

        // 1. Start transaction.
        Future<?> txFut = exec.submit(() -> {
            tx.run();

            // Some cases don't block on blkMsgCls. Then no need to await txLatch for such cases.
            txLatch.countDown();
        });

        // 2. Block transaction.
        txLatch.await(100, TimeUnit.MILLISECONDS);

        // 3. Start Consistent Cut procedure concurrently with running transaction.
        grid(0).context().cache().context().consistentCutMgr().triggerConsistentCutOnCluster();

        // 4-5. Await Consistent Cut started, excluding blocking node.
        awaitCutStarted(prevVer, cutBlkNode);

        // 6. Resume running transaction.
        cutGlobalStartLatch.countDown();

        // 7. Await transaction committed.
        txFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        // 8. Resume blocking Consistent Cut.
        cutWalBlkLatch.countDown();
        cutPublishBlkLatch.countDown();

        // 9. Await while Consistent Cut completed.
        awaitGlobalCutReady(prevVer);
    }

    /** Init latches depending on test parameters. */
    private void initLatches() {
        txLatch = new CountDownLatch(1);
        cutGlobalStartLatch = new CountDownLatch(1);

        switch (cutBlkType) {
            case NONE:
                cutWalBlkLatch = new CountDownLatch(0);
                cutPublishBlkLatch = new CountDownLatch(0);
                break;

            case WAL_START:
                cutWalBlkLatch = new CountDownLatch(1);
                cutPublishBlkLatch = new CountDownLatch(0);
                break;

            case PUBLISH:
                cutWalBlkLatch = new CountDownLatch(0);
                cutPublishBlkLatch = new CountDownLatch(1);
                break;
        }
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

    /**
     * Await Consistent Cut started on every node.
     *
     * @param prevCutVer Previous Consistent Cut version.
     * @param excl Node to exclude awaiting Consistent Cut starts.
     * @return Version of the latest Consistent Cut version.
     */
    protected long awaitCutStarted(long prevCutVer, @Nullable Integer excl) throws Exception {
        long newCutVer = -1L;

        Function<Integer, ConsistentCutManager> cutMgr = (n) -> grid(n).context().cache().context().consistentCutMgr();

        int starts = 0;

        // Wait Consistent Cut locally started on every node (prepared the check-list).
        for (int n = 0; n < nodes(); n++) {
            if (excl != null && n == excl) {
                if (++starts == nodes())
                    return newCutVer;

                continue;
            }

            // At most 1 sec to wait.
            for (int i = 0; i < 1_000; i++) {
                ConsistentCutState cutState = cutMgr.apply(n).latestPublishedCutState();

                long ver = cutState.version();

                if (ver > prevCutVer) {
                    if (newCutVer < 0)
                        newCutVer = ver;
                    else
                        assert newCutVer == ver : "new=" + newCutVer + ", rcv=" + ver + ", prev=" + prevCutVer;

                    if (++starts == nodes())
                        return newCutVer;

                    break;
                }

                Thread.sleep(1);
            }
        }

        StringBuilder bld = new StringBuilder()
            .append("Failed to wait Consitent Cut")
            .append(" newCutVer ").append(newCutVer)
            .append(", prevCutVer ").append(prevCutVer)
            .append(", excl node ").append(excl);

        for (int n = 0; n < nodes(); n++)
            bld.append("\nNode").append(n).append( ": ").append(cutMgr.apply(n).latestPublishedCutState());

        throw new Exception(bld.toString());
    }

    /** Blocks sending transaction message between nodes, and awaits for Consistent Cut procedure starts on every node. */
    private static class BlockingCommunicationSpi extends LogCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (blkMessage(msg)) {
                try {
                    txLatch.countDown();

                    cutGlobalStartLatch.await(100, TimeUnit.MILLISECONDS);
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
    private static class BlockingConsistentCutManager extends ConsistentCutManager {
        /** */
        private final GridKernalContext ctx;

        /** */
        public BlockingConsistentCutManager(GridKernalContext ctx) {
            this.ctx = ctx;
        }

        /** Blocks preparing and publishing Consistent Cut state. */
        @Override protected ConsistentCutState consistentCut(long prevCutVer, long cutVer) {
            if (blkCut()) {
                try {
                    cutPublishBlkLatch.await(100, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }

            return super.consistentCut(prevCutVer, cutVer);
        }

        /** Blocks writing ConsistentCutStartRecord. */
        @Override protected void walLog(long cutVer, WALRecord record) {
            if (blkCut()) {
                try {
                    cutWalBlkLatch.await(100, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }
            }

            super.walLog(cutVer, record);
        }

        /** */
        private boolean blkCut() {
            return ctx.localNodeId().equals(cutBlkNodeId);
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
        WAL_START,

        /** */
        PUBLISH
    }
}
