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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_OP_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Utils for MVCC.
 */
public class MvccUtils {
    /** */
    private static final MvccClosure<Boolean> isNewVisible = new IsNewVisible();

    /** */
    private static final MvccClosure<Boolean> isUpdated = new IsUpdated();

    /** */
    private static final MvccClosure<MvccVersion> getNewVer = new GetNewVersion();

    /**
     *
     */
    private MvccUtils(){
    }

    /**
     * Checks if version is visible from the given snapshot.
     *
     * @param cctx Cache context.
     * @param snapshot Snapshot.
     * @param mvccCrd Mvcc coordinator.
     * @param mvccCntr Mvcc counter.
     * @param opCntr Operation counter.
     * @return {@code True} if visible.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean isVisible(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
        int opCntr) throws IgniteCheckedException {
        return isVisible(cctx, snapshot, mvccCrd, mvccCntr, opCntr, true);
    }

    /**
     * Checks if version is visible from the given snapshot.
     *
     * @param cctx Cache context.
     * @param snapshot Snapshot.
     * @param mvccCrd Mvcc coordinator.
     * @param mvccCntr Mvcc counter.
     * @param opCntr Operation counter.
     * @param useTxLog {@code True} if TxLog should be used.
     * @return {@code True} if visible.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean isVisible(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
        int opCntr, boolean useTxLog) throws IgniteCheckedException {
        if (mvccCrd == 0) {
            assert mvccCntr == MvccProcessor.MVCC_COUNTER_NA && opCntr == MvccProcessor.MVCC_OP_COUNTER_NA
                : "rowVer=" + mvccVersion(mvccCrd, mvccCntr, opCntr) + ", snapshot=" + snapshot;

            return false; // Unassigned version is always invisible
        }

        long snapshotCrd = snapshot.coordinatorVersion();

        assert mvccCrd <= snapshotCrd : "rowVer=" + mvccVersion(mvccCrd, mvccCntr, opCntr) + ", snapshot=" + snapshot;

        long snapshotCntr = snapshot.counter();
        long snapshotOpCntr = snapshot.operationCounter();

        if (mvccCrd < snapshotCrd)
            // Don't check the row with TxLog if the row is expected to be committed.
            return !useTxLog || isCommitted(cctx, mvccCrd, mvccCntr);

        if (mvccCntr > snapshotCntr) // we don't see future updates
            return false;

        if (mvccCntr == snapshotCntr) {
            assert opCntr <= snapshotOpCntr : "rowVer=" + mvccVersion(mvccCrd, mvccCntr, opCntr) + ", snapshot=" + snapshot;

            return opCntr < snapshotOpCntr; // we don't see own pending updates
        }

        if (snapshot.activeTransactions().contains(mvccCntr)) // we don't see of other transactions' pending updates
            return false;

        if (!useTxLog)
            return true; // The checking row is expected to be committed.

        byte state = cctx.shared().coordinators().state(mvccCrd, mvccCntr);

        assert state == TxState.COMMITTED || state == TxState.ABORTED : "Unexpected state: " + state +
            ", rowMvcc=" + mvccCntr + ", txMvcc=" + snapshot.counter() + ":" + snapshot.operationCounter();

        return state == TxState.COMMITTED;
    }

    /**
     * Checks visibility of the given row versions from the given snapshot.
     *
     * @param cctx Context.
     * @param snapshot Snapshot.
     * @param crd Mvcc coordinator counter.
     * @param cntr Mvcc counter.
     * @param opCntr Mvcc operation counter.
     * @param link Link to data row (new version is located there).
     * @return Visibility status.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean isVisible(GridCacheContext cctx, MvccSnapshot snapshot, long crd, long cntr,
        int opCntr, long link) throws IgniteCheckedException {
        return isVisible(cctx, snapshot, crd, cntr, opCntr) && !isUpdated(cctx, link, snapshot);
    }

    /**
     * Checks if a row has not empty new version (xid_max).
     *
     * @param row Row.
     * @return {@code True} if row has a new version.
     */
    public static boolean hasNewMvccVersionFast(MvccUpdateVersionAware row) {
        assert row.newMvccCoordinatorVersion() > 0 == row.newMvccCounter() > MVCC_COUNTER_NA;

        return row.newMvccCoordinatorVersion() > 0;
    }

    /**
     * Checks if a row's new version is visible for the given snapshot.
     *
     * @param cctx Cache context.
     * @param link Link to the row.
     * @param snapshot Mvcc snapshot.
     * @return {@code True} if row is visible for the given snapshot.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean isNewVisible(GridCacheContext cctx, long link, MvccSnapshot snapshot)
        throws IgniteCheckedException {
        return invoke(cctx, link, isNewVisible, snapshot);
    }

    /**
     * Returns new version of row (xid_max) if any.
     *
     * @param cctx Cache context.
     * @param link Link to the row.
     * @return New {@code MvccVersion} if row has xid_max, or null if doesn't.
     * @throws IgniteCheckedException If failed.
     */
    public static MvccVersion getNewVersion(GridCacheContext cctx, long link)
        throws IgniteCheckedException {
        return invoke(cctx, link, getNewVer, null);
    }

    /**
     * Compares to pairs of coordinator/counter versions. See {@link Comparable}.
     *
     * @param mvccCrdLeft First coordinator version.
     * @param mvccCntrLeft First counter version.
     * @param mvccCrdRight Second coordinator version.
     * @param mvccCntrRight Second counter version.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(long mvccCrdLeft, long mvccCntrLeft, long mvccCrdRight, long mvccCntrRight) {
        int cmp = Long.compare(mvccCrdLeft, mvccCrdRight);

        return cmp != 0 ? cmp : Long.compare(mvccCntrLeft, mvccCntrRight);
    }

    /**
     * Compares to pairs of coordinator/counter versions. See {@link Comparable}.
     *
     * @param mvccCrdLeft First coordinator version.
     * @param mvccCntrLeft First counter version.
     * @param mvccOpCntrLeft First operation counter.
     * @param other The object to compare with.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(long mvccCrdLeft, long mvccCntrLeft, int mvccOpCntrLeft, MvccVersionAware other) {
        return compare(mvccCrdLeft, mvccCntrLeft, mvccOpCntrLeft,
            other.mvccCoordinatorVersion(), other.mvccCounter(), other.mvccOperationCounter());
    }

    /**
     * Compares to pairs of coordinator/counter versions. See {@link Comparable}.
     *
     * @param mvccCrdLeft First coordinator version.
     * @param mvccCntrLeft First counter version.
     * @param mvccOpCntrLeft First operation counter.
     * @param mvccCrdRight Second coordinator version.
     * @param mvccCntrRight Second counter version.
     * @param mvccOpCntrRight Second operation counter.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(long mvccCrdLeft, long mvccCntrLeft, int mvccOpCntrLeft, long mvccCrdRight,
        long mvccCntrRight, int mvccOpCntrRight) {
        int cmp = compare(mvccCrdLeft, mvccCntrLeft, mvccCrdRight, mvccCntrRight);

        return cmp != 0 ? cmp : Integer.compare(mvccOpCntrLeft, mvccOpCntrRight);
    }

    /**
     * Compares to pairs of MVCC versions. See {@link Comparable}.
     *
     * @param mvccVerLeft First MVCC version.
     * @param mvccCrdRight Second coordinator version.
     * @param mvccCntrRight Second counter.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(MvccVersion mvccVerLeft, long mvccCrdRight, long mvccCntrRight) {
        return compare(mvccVerLeft.coordinatorVersion(), mvccVerLeft.counter(), mvccCrdRight, mvccCntrRight);
    }

    /**
     * Compares row version (xid_min) with the given version.
     *
     * @param row Row.
     * @param ver Version.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(MvccVersionAware row, MvccVersion ver) {
        return compare(row.mvccCoordinatorVersion(), row.mvccCounter(), row.mvccOperationCounter(),
            ver.coordinatorVersion(), ver.counter(), ver.operationCounter());
    }

    /**
     * Compares new row version (xid_max) with the given counter and coordinator versions.
     *
     * @param row Row.
     * @param mvccCrd Mvcc coordinator.
     * @param mvccCntr Mvcc counter.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compareNewVersion(MvccUpdateVersionAware row, long mvccCrd, long mvccCntr) {
        return compare(row.newMvccCoordinatorVersion(), row.newMvccCounter(), mvccCrd, mvccCntr);
    }

    /**
     * Compares new row version (xid_max) with the given counter and coordinator versions.
     *
     * @param row Row.
     * @param mvccCrd Mvcc coordinator.
     * @param mvccCntr Mvcc counter.
     * @param opCntr Mvcc operation counter.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compareNewVersion(MvccUpdateVersionAware row, long mvccCrd, long mvccCntr, int opCntr) {
        return compare(row.newMvccCoordinatorVersion(), row.newMvccCounter(), row.newMvccOperationCounter(), mvccCrd, mvccCntr, opCntr);
    }

    /**
     * Compares new row version (xid_max) with the given version.
     *
     * @param row Row.
     * @param ver Version.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compareNewVersion(MvccUpdateVersionAware row, MvccVersion ver) {
        return compare(row.newMvccCoordinatorVersion(), row.newMvccCounter(), row.newMvccOperationCounter(),
            ver.coordinatorVersion(), ver.counter(), ver.operationCounter());
    }

    /**
     * @param crdVer Mvcc coordinator version.
     * @param cntr Counter.
     * @param opCntr Operation counter.
     * @return Always {@code true}.
     */
    public static boolean mvccVersionIsValid(long crdVer, long cntr, int opCntr) {
        return mvccVersionIsValid(crdVer, cntr) && opCntr != MVCC_OP_COUNTER_NA;
    }

    /**
     * @param crdVer Mvcc coordinator version.
     * @param cntr Counter.
     * @return {@code True} if version is valid.
     */
    public static boolean mvccVersionIsValid(long crdVer, long cntr) {
        return crdVer > 0 && cntr != MVCC_COUNTER_NA;
    }

    /**
     * @param topVer Topology version for cache operation.
     * @return Error.
     */
    public static IgniteCheckedException noCoordinatorError(AffinityTopologyVersion topVer) {
        return new ClusterTopologyServerNotFoundException("Mvcc coordinator is not assigned for " +
            "topology version: " + topVer);
    }

    /**
     * @param cctx Cache context.
     * @param link Link to the row.
     * @param snapshot Mvcc snapshot.
     * @return {@code True} if row is updated for given snapshot.
     * @throws IgniteCheckedException If failed.
     */
    private static boolean isUpdated(GridCacheContext cctx, long link,
        MvccSnapshot snapshot)
        throws IgniteCheckedException {
        return invoke(cctx, link, isUpdated, snapshot);
    }

    /**
     * Encapsulates common logic for working with row mvcc info: page locking/unlocking, checks and other.
     * Strategy pattern.
     *
     * @param cctx Cache group.
     * @param link Row link.
     * @param clo Closure to apply.
     * @param snapshot Mvcc snapshot.
     * @param <R> Return type.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private static <R> R invoke(GridCacheContext cctx, long link, MvccClosure<R> clo, MvccSnapshot snapshot)
        throws IgniteCheckedException {
        assert cctx.mvccEnabled();

        PageMemory pageMem = cctx.dataRegion().pageMemory();
        int grpId = cctx.groupId();

        long pageId = pageId(link);
        long page = pageMem.acquirePage(grpId, pageId);

        try {
            long pageAddr = pageMem.readLock(grpId, pageId, page);

            try{
                DataPageIO dataIo = DataPageIO.VERSIONS.forPage(pageAddr);

                int offset = dataIo.getPayloadOffset(pageAddr, itemId(link), pageMem.pageSize(), MVCC_INFO_SIZE);

                long mvccCrd = dataIo.mvccCoordinator(pageAddr, offset);
                long mvccCntr = dataIo.mvccCounter(pageAddr, offset);
                int mvccOpCntr = dataIo.mvccOperationCounter(pageAddr, offset);

                assert mvccVersionIsValid(mvccCrd, mvccCntr, mvccOpCntr) : mvccVersion(mvccCrd, mvccCntr, mvccOpCntr);

                long newMvccCrd = dataIo.newMvccCoordinator(pageAddr, offset);
                long newMvccCntr = dataIo.newMvccCounter(pageAddr, offset);
                int newMvccOpCntr = dataIo.newMvccOperationCounter(pageAddr, offset);

                assert newMvccCrd == 0 || mvccVersionIsValid(newMvccCrd, newMvccCntr, newMvccOpCntr)
                    : mvccVersion(newMvccCrd, newMvccCntr, newMvccOpCntr);

                return clo.apply(cctx, snapshot, mvccCrd, mvccCntr, mvccOpCntr, newMvccCrd, newMvccCntr, newMvccOpCntr);
            }
            finally {
                pageMem.readUnlock(grpId, pageId, page);
            }
        }
        finally {
            pageMem.releasePage(grpId, pageId, page);
        }
    }

    /**
     *
     * @param cctx Cache context.
     * @param mvccCrd Coordinator version.
     * @param mvccCntr Counter.
     * @return {@code True} in case the corresponding transaction is in {@code TxState.COMMITTED} state.
     * @throws IgniteCheckedException If failed.
     */
    private static boolean isCommitted(GridCacheContext cctx, long mvccCrd, long mvccCntr) throws IgniteCheckedException {
        return cctx.shared().coordinators().state(mvccCrd, mvccCntr) == TxState.COMMITTED;
    }

    /**
     * Throw an {@link UnsupportedOperationException} if this cache is transactional and MVCC is enabled with
     * appropriate message about corresponding operation type.
     * @param cctx Cache context.
     * @param opType operation type to mention in error message.
     */
    public static void verifyMvccOperationSupport(GridCacheContext<?, ?> cctx, String opType) {
        if (cctx.mvccEnabled())
            throw new UnsupportedOperationException(opType + " operations are not supported on transactional " +
                "caches when MVCC is enabled.");
    }


    /**
     * @param ctx Grid kernal context.
     * @return Currently started user transaction, or {@code null} if none started.
     */
    @Nullable public static GridNearTxLocal tx(GridKernalContext ctx) {
        return tx(ctx, null);
    }

    /**
     * @param ctx Grid kernal context.
     * @param txId Transaction ID.
     * @return Currently started user transaction, or {@code null} if none started.
     */
    @Nullable private static GridNearTxLocal tx(GridKernalContext ctx, @Nullable GridCacheVersion txId) {
        IgniteTxManager tm = ctx.cache().context().tm();

        IgniteInternalTx tx0 = txId == null ? tm.tx() : tm.tx(txId);

        GridNearTxLocal tx = tx0 != null && tx0.user() ? (GridNearTxLocal)tx0 : null;

        assert tx == null || (tx.pessimistic() && tx.repeatableRead());

        return tx;
    }

    /**
     * @param ctx Grid kernal context.
     * @param txId Transaction ID.
     * @return Currently started active user transaction, or {@code null} if none started.
     */
    @Nullable private static GridNearTxLocal activeTx(GridKernalContext ctx, @Nullable GridCacheVersion txId) {
        GridNearTxLocal tx = tx(ctx, txId);

        if (tx != null) {
            assert tx.state() == TransactionState.ACTIVE;

            return tx;
        }

        return null;
    }

    /**
     * @param ctx Grid kernal context.
     * @return Currently started active user transaction, or {@code null} if none started.
     */
    @Nullable public static GridNearTxLocal activeSqlTx(GridKernalContext ctx) {
        return activeSqlTx(ctx, null);
    }

    /**
     * @param ctx Grid kernal context.
     * @param txId Transaction ID.
     * @return Currently started active user transaction, or {@code null} if none started.
     */
    @Nullable public static GridNearTxLocal activeSqlTx(GridKernalContext ctx, GridCacheVersion txId) {
        GridNearTxLocal tx = activeTx(ctx, txId);

        if (tx != null && !tx.isOperationAllowed(true))
            throw new IgniteSQLException("SQL queries and cache operations " +
                "may not be used in the same transaction.", IgniteQueryErrorCode.TRANSACTION_TYPE_MISMATCH);

        return tx;
    }

    /**
     * @param ctx Grid kernal context.
     * @param timeout Transaction timeout.
     * @return Newly started SQL transaction.
     */
    public static GridNearTxLocal txStart(GridKernalContext ctx, long timeout) {
        return txStart(ctx, null, timeout);
    }

    /**
     * @param cctx Cache context.
     * @param timeout Transaction timeout.
     * @return Newly started SQL transaction.
     */
    public static GridNearTxLocal txStart(GridCacheContext cctx, long timeout) {
        return txStart(cctx.kernalContext(), cctx, timeout);
    }

    /**
     * @param ctx Grid kernal context.
     * @param cctx Cache context.
     * @param timeout Transaction timeout.
     * @return Newly started SQL transaction.
     */
    private static GridNearTxLocal txStart(GridKernalContext ctx, @Nullable GridCacheContext cctx, long timeout) {
        if (timeout == 0) {
            TransactionConfiguration tcfg = cctx != null ?
                CU.transactionConfiguration(cctx, ctx.config()) : null;

            if (tcfg != null)
                timeout = tcfg.getDefaultTxTimeout();
        }

        GridNearTxLocal tx = ctx.cache().context().tm().newTx(
            false,
            false,
            cctx != null && cctx.systemTx() ? cctx : null,
            PESSIMISTIC,
            REPEATABLE_READ,
            timeout,
            cctx == null || !cctx.skipStore(),
            true,
            0
        );

        tx.syncMode(FULL_SYNC);

        return tx;
    }

    /**
     * @param ctx Grid kernal context.
     * @return Whether MVCC is enabled or not on {@link IgniteConfiguration}.
     */
    public static boolean mvccEnabled(GridKernalContext ctx) {
        return ctx.config().isMvccEnabled();
    }

    /**
     * Initialises MVCC filter and returns MVCC query tracker if needed.
     * @param cctx Cache context.
     * @return MVCC query tracker.
     * @throws IgniteCheckedException If failed.
     */
    @NotNull public static MvccQueryTracker mvccTracker(GridCacheContext cctx) throws IgniteCheckedException {
        return mvccTracker(cctx, false);
    }

    /**
     * Initialises MVCC filter and returns MVCC query tracker if needed.
     * @param cctx Cache context.
     * @param startTx Start transaction flag.
     * @return MVCC query tracker.
     * @throws IgniteCheckedException If failed.
     */
    @NotNull public static MvccQueryTracker mvccTracker(GridCacheContext cctx, boolean startTx) throws IgniteCheckedException {
        assert cctx != null && cctx.mvccEnabled();

        GridNearTxLocal tx = activeTx(cctx.kernalContext(), null);

        if (tx == null && startTx)
            tx = txStart(cctx, 0);

        if (tx != null)
            return new MvccQueryTracker(cctx, cctx.shared().coordinators().currentCoordinator(),
                requestMvccVersion(cctx, tx));

        final GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        MvccQueryTracker tracker = new MvccQueryTracker(cctx, true,
            new IgniteBiInClosure<AffinityTopologyVersion, IgniteCheckedException>() {
                @Override public void apply(AffinityTopologyVersion topVer, IgniteCheckedException e) {
                    fut.onDone(null, e);
                }
            });

        tracker.requestVersion(cctx.shared().exchange().readyAffinityVersion());

        fut.get();

        return tracker;
    }

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @throws IgniteCheckedException If failed.
     * @return Mvcc snapshot.
     */
    public static MvccSnapshot requestMvccVersion(GridCacheContext cctx, GridNearTxLocal tx) throws IgniteCheckedException {
        tx.addActiveCache(cctx, false);

        if (tx.mvccInfo() == null) {
            MvccProcessor mvccProc = cctx.shared().coordinators();
            MvccCoordinator crd = mvccProc.currentCoordinator();

            assert crd != null : tx.topologyVersion();

            if (crd.nodeId().equals(cctx.localNodeId()))
                tx.mvccInfo(new MvccTxInfo(cctx.localNodeId(), mvccProc.requestTxSnapshotOnCoordinator(tx)));
            else
                return mvccProc.requestTxSnapshot(crd, new MvccTxSnapshotResponseListener(tx), tx.nearXidVersion()).get(); // TODO IGNITE-7388
        }

        return tx.mvccInfo().snapshot();
    }

    /** */
    private static MvccVersion mvccVersion(long crd, long cntr, int opCntr) {
        return new MvccVersionImpl(crd, cntr, opCntr);
    }

    /**
     * Mvcc closure interface.
     * @param <R> Return type.
     */
    private interface MvccClosure<R> {
        /**
         * Runs closure over the Mvcc info.
         * @param snapshot Mvcc snapshot.
         * @param mvccCrd Coordinator version.
         * @param mvccCntr Counter.
         * @param mvccOpCntr Operation counter.
         * @param newMvccCrd New mvcc coordinator
         * @param newMvccCntr New mvcc counter.
         * @param newMvccOpCntr New mvcc operation counter.
         * @return Result.
         */
        public R apply(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr, int mvccOpCntr,
            long newMvccCrd, long newMvccCntr, int newMvccOpCntr) throws IgniteCheckedException;
    }

    /**
     * Closure for checking row visibility for snapshot.
     */
    private static class IsNewVisible implements MvccClosure<Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean apply(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
            int mvccOpCntr, long newMvccCrd, long newMvccCntr, int newMvccOpCntr) throws IgniteCheckedException {
            return isVisible(cctx, snapshot, newMvccCrd, newMvccCntr, newMvccOpCntr);
        }
    }

    /**
     * Closure for checking whether the row is updated for given snapshot.
     */
    private static class IsUpdated implements MvccClosure<Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean apply(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
            int mvccOpCntr, long newMvccCrd, long newMvccCntr, int newMvccOpCntr) throws IgniteCheckedException {

            if (newMvccCrd == 0)
                return false;

            assert mvccVersionIsValid(newMvccCrd, newMvccCntr, newMvccOpCntr);

            return (mvccCrd == newMvccCrd && mvccCntr == newMvccCntr) // Double-changed in scope of one transaction.
                || isVisible(cctx, snapshot, newMvccCrd, newMvccCntr, newMvccOpCntr);
        }
    }

    /**
     * Closure for getting xid_max version of row.
     */
    private static class GetNewVersion implements MvccClosure<MvccVersion> {
        /** {@inheritDoc} */
        @Override public MvccVersion apply(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
            int mvccOpCntr, long newMvccCrd, long newMvccCntr, int newMvccOpCntr) {
            return newMvccCrd == 0 ? null : new MvccVersionImpl(newMvccCrd, newMvccCntr, newMvccOpCntr);
        }
    }

    /** */
    private static class MvccTxSnapshotResponseListener implements MvccSnapshotResponseListener {
        /** */
        private final GridNearTxLocal tx;

        /**
         * @param tx Transaction.
         */
        MvccTxSnapshotResponseListener(GridNearTxLocal tx) {
            this.tx = tx;
        }

        /** {@inheritDoc} */
        @Override public void onResponse(UUID crdId, MvccSnapshot res) {
            tx.mvccInfo(new MvccTxInfo(crdId, res));
        }

        /** {@inheritDoc} */
        @Override public void onError(IgniteCheckedException e) {
            // No-op.
        }
    }
}
