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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotStartRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.AbstractIncrementalSnapshotBlockingTest.BlkNodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.AbstractIncrementalSnapshotBlockingTest.BlkNodeType.PRIMARY;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.AbstractIncrementalSnapshotBlockingTest.BlkSnpType.AFTER_START;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.AbstractIncrementalSnapshotBlockingTest.BlkSnpType.BEFORE_START;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** Base class for testing incremental snapshot blocking some events. */
public abstract class AbstractIncrementalSnapshotBlockingTest extends AbstractIncrementalSnapshotTest {
    /** Number of current testing case. */
    protected int caseNum;

    /** */
    protected static BlkNodeType snpBlkNodeType;

    /** */
    protected static BlkSnpType snpBlkType;

    /** */
    protected static BlkNodeType txBlkNodeType;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setPluginProviders(
            new BlockingWALPluginProvider(),
            new BlockingSnapshotPluginProvider());

        return cfg;
    }

    /**
     * Run test cases.
     *
     * @param cases Collection of cases. Every case is a collection of key description - tuple (primaryNodeId, backupNodeId).
     */
    protected void runCases(List<TransactionTestCase> cases) throws Exception {
        // Ignite coordinator, ordinary server node, client node.
        // If test with stopping client node then use it as near node.
        List<Integer> nears = F.asList(0, nodes() - 1, nodes());

        for (int near: nears) {
            // Client nodes doesn't prepare anything after version update.
            if (near == nodes() && snpBlkNodeType == NEAR && snpBlkType == AFTER_START)
                continue;

            for (int c = 0; c < cases.size(); c++) {
                runCase(cases.get(c), near, TransactionConcurrency.PESSIMISTIC);
                runCase(cases.get(c), near, TransactionConcurrency.OPTIMISTIC);
            }
        }
    }

    /** Run single test cases. */
    protected abstract void runCase(TransactionTestCase testCase, int nearNodeIdx, TransactionConcurrency txConcurrency) throws Exception;

    /**
     * Run test case.
     *
     * @param tx Function that performs transaction.
     * @param txBlkNodeIdx ID of node to block transaction.
     * @param snpBlkNodeIdx ID of node to block incremental snapshot.
     */
    protected void run(Runnable tx, int txBlkNodeIdx, int snpBlkNodeIdx) throws Exception {
        caseNum++;

        // 1. Block transaction.
        blockTx(grid(txBlkNodeIdx));

        // 2. Start transaction.
        IgniteInternalFuture<?> txFut = multithreadedAsync(tx, 1);

        // 3. Await transaction has blocked.
        awaitTxBlocked(grid(txBlkNodeIdx));

        // 4. Start incremental snapshot procedure concurrently with running transaction.
        if (snpBlkNodeIdx != -1)
            ((BlockingSnapshotManager)snp(grid(snpBlkNodeIdx))).block(snpBlkType);

        IgniteFuture<Void> snpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

        // 5. Await incremental snapshot has blocked.
        if (snpBlkNodeIdx != -1)
            ((BlockingSnapshotManager)snp(grid(snpBlkNodeIdx))).awaitSnpBlockedOrFinished(snpFut);

        // 6. Resume the blocking transaction.
        unblockTx(grid(txBlkNodeIdx));

        // 7. Await transaction completed.
        txFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        // 8. Resume the blocking incremental snapshot.
        if (snpBlkNodeIdx != -1)
            ((BlockingSnapshotManager)snp(grid(snpBlkNodeIdx))).unblock(snpBlkType);

        // 9. Await while incremental snapshot completed.
        snpFut.get(getTestTimeout());

        clear();
    }

    /**
     * Function that runs transaction with specified keys.
     *
     * @param near ID of node to coordinate a transaction.
     * @param testCase Test case.
     */
    protected void tx(int near, TransactionTestCase testCase, TransactionConcurrency concurrency) {
        try (Transaction tx = grid(near).transactions().txStart(concurrency, SERIALIZABLE)) {
            for (int key: testCase.keys(grid(0), CACHE))
                grid(near).cache(CACHE).put(key, 0);

            tx.commit();
        }
    }

    /** */
    protected abstract void blockTx(IgniteEx blkNode);

    /** */
    protected abstract void awaitTxBlocked(IgniteEx blkNode) throws Exception;

    /** */
    protected abstract void unblockTx(IgniteEx blkNode);

    /** Finds index of node to be blocked during a case. */
    protected int blkNodeIndex(int nearNodeIdx, BlkNodeType blkNodeType, TransactionTestCase c) {
        if (blkNodeType == NEAR)
            return nearNodeIdx;
        else if (blkNodeType == PRIMARY)
            return c.keys[0][0];
        else
            return c.keys[0][1];
    }

    /** Checks WALs correctness for incremental snapshots from all nodes. */
    protected void checkWalsConsistency() throws Exception {
        checkWalsConsistency(caseNum, caseNum);
    }

    /** */
    protected void clear() {
        for (int n = 0; n < nodes(); n++)
            BlockingWALManager.walMgr(grid(n)).clear();
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

    /** Blocks writing to WAL {@link IncrementalSnapshotStartRecord}. */
    protected static class BlockingWALManager extends FileWriteAheadLogManager {
        /** First CDL is actual block of record, second CDL shows whether first is blocked. */
        private final Map<WALRecord.RecordType, T3<Predicate<WALRecord>, CountDownLatch, CountDownLatch>> map = new ConcurrentHashMap<>();

        /** */
        public BlockingWALManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** */
        static BlockingWALManager walMgr(IgniteEx ign) {
            return (BlockingWALManager)ign.context().cache().context().wal();
        }

        /** */
        public void block(WALRecord.RecordType recType, Predicate<WALRecord> pred) {
            map.put(recType, new T3<>(pred, new CountDownLatch(1), new CountDownLatch(1)));
        }

        /** */
        public void unblock(WALRecord.RecordType recType) {
            map.get(recType).get2().countDown();
        }

        /** */
        public void awaitBlocked(WALRecord.RecordType recType) {
            U.awaitQuiet(map.get(recType).get3());
        }

        /** */
        public void clear() {
            map.clear();
        }

        /** {@inheritDoc} */
        @Override public WALPointer log(WALRecord record) throws IgniteCheckedException, StorageException {
            T3<Predicate<WALRecord>, CountDownLatch, CountDownLatch> info = map.get(record.type());

            if (info != null && info.get2().getCount() > 0 && info.get1().test(record)) {
                info.get3().countDown();

                U.awaitQuiet(info.get2());
            }

            return super.log(record);
        }
    }

    /** */
    private static class BlockingSnapshotPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "BlockingSnapshotPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (IgniteSnapshotManager.class.equals(cls))
                return (T)new BlockingSnapshotManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** Blocks local incremental snapshot before or after start. */
    protected static class BlockingSnapshotManager extends IgniteSnapshotManager {
        /** Blocks this record after local incremental snapshot started. */
        private static final WALRecord.RecordType blkStartRecType = WALRecord.RecordType.INCREMENTAL_SNAPSHOT_START_RECORD;

        /** Latch that blocks before local incremental snapshot started. */
        private volatile CountDownLatch beforeStartLatch;

        /** */
        private volatile CountDownLatch blockedLatch;

        /** */
        public BlockingSnapshotManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public void handleIncrementalSnapshotId(UUID id) {
            // Block only discovery worker thread.
            if (Thread.currentThread().getName().contains("disco")) {
                if (beforeStartLatch != null) {
                    blockedLatch.countDown();
                    blockedLatch = null;

                    U.awaitQuiet(beforeStartLatch);

                    beforeStartLatch = null;
                }
            }

            super.handleIncrementalSnapshotId(id);
        }

        /** */
        public void block(BlkSnpType type) {
            blockedLatch = new CountDownLatch(1);

            if (type == BEFORE_START)
                beforeStartLatch = new CountDownLatch(1);
            else if (type == AFTER_START)
                ((BlockingWALManager)cctx.wal()).block(blkStartRecType, (rec) -> rec.type() == blkStartRecType);
        }

        /** */
        public void awaitSnpBlockedOrFinished(@Nullable IgniteFuture<?> snpFut) {
            CountDownLatch latch = blockedLatch;

            if (latch == null)
                return;

            snpFut.listen((f) -> latch.countDown());

            if (beforeStartLatch != null)
                U.awaitQuiet(latch);
            else {
                latch.countDown();

                ((BlockingWALManager)cctx.wal()).awaitBlocked(blkStartRecType);
            }
        }

        /** */
        public void unblock(BlkSnpType type) {
            if (type == BEFORE_START && beforeStartLatch != null)
                beforeStartLatch.countDown();
            else if (type == AFTER_START)
                ((BlockingWALManager)cctx.wal()).unblock(blkStartRecType);
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
    protected enum BlkSnpType {
        /** */
        NONE,

        /** */
        BEFORE_START,

        /** */
        AFTER_START,
    }
}
