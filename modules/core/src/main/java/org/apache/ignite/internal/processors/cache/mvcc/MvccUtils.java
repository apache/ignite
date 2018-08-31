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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.transactions.IgniteTxMvccVersionCheckedException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.MVCC_HINTS_BIT_OFF;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.MVCC_HINTS_MASK;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.TRANSACTION_COMPLETED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Utils for MVCC.
 */
public class MvccUtils {
    /** */
    public static final long MVCC_CRD_COUNTER_NA = 0L;
    /** */
    public static final long MVCC_CRD_START_CNTR = 1L;
    /** */
    public static final long MVCC_COUNTER_NA = 0L;
    /** */
    public static final long MVCC_INITIAL_CNTR = 1L;
    /** */
    public static final long MVCC_START_CNTR = 3L;
    /** */
    public static final int MVCC_OP_COUNTER_NA = 0;
    /** */
    public static final int MVCC_START_OP_CNTR = 1;
    /** */
    public static final int MVCC_READ_OP_CNTR = ~MVCC_HINTS_MASK;

    /** */
    public static final int MVCC_INVISIBLE = 0;
    /** */
    public static final int MVCC_VISIBLE_REMOVED = 1;
    /** */
    public static final int MVCC_VISIBLE = 2;

    /** */
    public static final MvccVersion INITIAL_VERSION =
        mvccVersion(MVCC_CRD_START_CNTR, MVCC_INITIAL_CNTR, MVCC_START_OP_CNTR);

    /** */
    public static final MvccVersion MVCC_VERSION_NA =
        mvccVersion(MVCC_CRD_COUNTER_NA, MVCC_COUNTER_NA, MVCC_OP_COUNTER_NA);

    /** */
    private static final MvccClosure<Integer> getVisibleState = new GetVisibleState();

    /** */
    private static final MvccClosure<Boolean> isVisible = new IsVisible();

    /** */
    private static final MvccClosure<MvccVersion> getNewVer = new GetNewVersion();

    /**
     *
     */
    private MvccUtils(){
    }

    /**
     * @param ctx Kernal context.
     * @return Newly created Mvcc processor.
     */
    public static MvccProcessor createProcessor(GridKernalContext ctx) {
        return mvccEnabled(ctx) ? new MvccProcessorImpl(ctx) : new NoOpMvccProcessor(ctx);
    }

    /**
     * @param cctx Cache context.
     * @param mvccCrd Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param snapshot Snapshot.
     * @return {@code True} if transaction is active.
     * @see TxState
     * @throws IgniteCheckedException If failed.
     */
    public static boolean isActive(GridCacheContext cctx, long mvccCrd, long mvccCntr, MvccSnapshot snapshot)
        throws IgniteCheckedException {
        if (isVisible(cctx, snapshot, mvccCrd, mvccCntr, MVCC_OP_COUNTER_NA, false))
            return false;

        byte state = state(cctx, mvccCrd, mvccCntr, 0);

        return state != TxState.COMMITTED && state != TxState.ABORTED
            || cctx.kernalContext().coordinators().hasLocalTransaction(mvccCrd, mvccCntr);
    }

    /**
     * @param cctx Cache context.
     * @param mvccCrd Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @return TxState
     * @see TxState
     * @throws IgniteCheckedException If failed.
     */
    public static byte state(GridCacheContext cctx, long mvccCrd, long mvccCntr, int mvccOpCntr) throws IgniteCheckedException {
        return state(cctx.kernalContext().coordinators(), mvccCrd, mvccCntr, mvccOpCntr);
    }

    /**
     * @param grp Cache group context.
     * @param mvccCrd Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @return TxState
     * @see TxState
     * @throws IgniteCheckedException If failed.
     */
    public static byte state(CacheGroupContext grp, long mvccCrd, long mvccCntr, int mvccOpCntr) throws IgniteCheckedException {
        return state(grp.shared().coordinators(), mvccCrd, mvccCntr, mvccOpCntr);
    }

    /**
     * @param proc Mvcc processor.
     * @param mvccCrd Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @return TxState
     * @see TxState
     * @throws IgniteCheckedException If failed.
     */
    private static byte state(MvccProcessor proc, long mvccCrd, long mvccCntr, int mvccOpCntr) throws IgniteCheckedException {
        if (compare(INITIAL_VERSION, mvccCrd, mvccCntr, mvccOpCntr) == 0)
            return TxState.COMMITTED; // Initial version is always committed;

        if ((mvccOpCntr & MVCC_HINTS_MASK) != 0)
            return (byte)(mvccOpCntr >>> MVCC_HINTS_BIT_OFF);

        return proc.state(mvccCrd, mvccCntr);
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
        if (mvccCrd == MVCC_CRD_COUNTER_NA) {
            assert mvccCntr == MVCC_COUNTER_NA && opCntr == MVCC_OP_COUNTER_NA
                : "rowVer=" + mvccVersion(mvccCrd, mvccCntr, opCntr) + ", snapshot=" + snapshot;

            return false; // Unassigned version is always invisible
        }

        if (compare(INITIAL_VERSION, mvccCrd, mvccCntr, opCntr) == 0)
            return true; // Initial version is always visible

        long snapshotCrd = snapshot.coordinatorVersion();

        long snapshotCntr = snapshot.counter();
        int snapshotOpCntr = snapshot.operationCounter();

        if (mvccCrd > snapshotCrd)
            return false; // Rows in the future are never visible.

        if (mvccCrd < snapshotCrd)
            // Don't check the row with TxLog if the row is expected to be committed.
            return !useTxLog || isCommitted(cctx, mvccCrd, mvccCntr, opCntr);

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

        byte state = state(cctx, mvccCrd, mvccCntr, opCntr);

        if (state != TxState.COMMITTED && state != TxState.ABORTED)
            throw unexpectedStateException(cctx, state, mvccCrd, mvccCntr, opCntr, snapshot);

        return state == TxState.COMMITTED;
    }

    /**
     *
     * @param grp Cache group context.
     * @param state State.
     * @param crd Mvcc coordinator counter.
     * @param cntr Mvcc counter.
     * @param opCntr Mvcc operation counter.
     * @return State exception.
     */
    public static IgniteTxMvccVersionCheckedException unexpectedStateException(
        CacheGroupContext grp, byte state, long crd, long cntr,
        int opCntr) {
        return unexpectedStateException(grp.shared().kernalContext(), state, crd, cntr, opCntr, null);
    }

    /**
     *
     * @param cctx Cache context.
     * @param state State.
     * @param crd Mvcc coordinator counter.
     * @param cntr Mvcc counter.
     * @param opCntr Mvcc operation counter.
     * @param snapshot Mvcc snapshot
     * @return State exception.
     */
    public static IgniteTxMvccVersionCheckedException unexpectedStateException(
        GridCacheContext cctx, byte state, long crd, long cntr,
        int opCntr, MvccSnapshot snapshot) {
        return unexpectedStateException(cctx.kernalContext(), state, crd, cntr, opCntr, snapshot);
    }

    /** */
    private static IgniteTxMvccVersionCheckedException unexpectedStateException(GridKernalContext ctx, byte state, long crd, long cntr,
        int opCntr, MvccSnapshot snapshot) {
        String msg = "Unexpected state: [state=" + state + ", rowVer=" + crd + ":" + cntr + ":" + opCntr;

        if (snapshot != null)
            msg += ", txVer=" + snapshot.coordinatorVersion() + ":" + snapshot.counter() + ":" + snapshot.operationCounter();

        msg += ", localNodeId=" + ctx.localNodeId()  + "]";

        return new IgniteTxMvccVersionCheckedException(msg);
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
        return isVisible(cctx, snapshot, crd, cntr, opCntr, false)
            && isVisible(cctx, link, snapshot);
    }

    /**
     * Checks if a row has not empty new version (xid_max).
     *
     * @param row Row.
     * @return {@code True} if row has a new version.
     */
    public static boolean hasNewVersion(MvccUpdateVersionAware row) {
        assert row.newMvccCoordinatorVersion() == MVCC_CRD_COUNTER_NA
            || mvccVersionIsValid(row.newMvccCoordinatorVersion(), row.newMvccCounter(), row.newMvccOperationCounter());

        return row.newMvccCoordinatorVersion() > MVCC_CRD_COUNTER_NA;
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
    public static int getVisibleState(GridCacheContext cctx, long link, MvccSnapshot snapshot)
        throws IgniteCheckedException {
        return invoke(cctx, link, getVisibleState, snapshot);
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
     * Compares to pairs of MVCC versions. See {@link Comparable}.
     *
     * @param row First MVCC version.
     * @param mvccCrdRight Second coordinator version.
     * @param mvccCntrRight Second counter.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(MvccVersionAware row, long mvccCrdRight, long mvccCntrRight) {
        return compare(row.mvccCoordinatorVersion(), row.mvccCounter(), mvccCrdRight, mvccCntrRight);
    }

    /**
     * Compares to pairs of MVCC versions. See {@link Comparable}.
     *
     * @param mvccVerLeft First MVCC version.
     * @param mvccCrdRight Second coordinator version.
     * @param mvccCntrRight Second counter.
     * @param mvccOpCntrRight Second operation counter.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(MvccVersion mvccVerLeft, long mvccCrdRight, long mvccCntrRight, int mvccOpCntrRight) {
        return compare(mvccVerLeft.coordinatorVersion(), mvccVerLeft.counter(),
            mvccVerLeft.operationCounter(), mvccCrdRight, mvccCntrRight, mvccOpCntrRight);
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
     * @param mvccCrdRight Second coordinator version.
     * @param mvccCntrRight Second counter version.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compare(long mvccCrdLeft, long mvccCntrLeft, long mvccCrdRight, long mvccCntrRight) {
        return compare(mvccCrdLeft, mvccCntrLeft, 0, mvccCrdRight, mvccCntrRight, 0);
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
        int cmp;

        if ((cmp = Long.compare(mvccCrdLeft, mvccCrdRight)) != 0
            || (cmp = Long.compare(mvccCntrLeft, mvccCntrRight)) != 0
            || (cmp = Integer.compare(mvccOpCntrLeft & ~MVCC_HINTS_MASK, mvccOpCntrRight & ~MVCC_HINTS_MASK)) != 0)
            return cmp;

        return 0;
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
        return crdVer > MVCC_CRD_COUNTER_NA && cntr != MVCC_COUNTER_NA;
    }

    /**
     * @param topVer Topology version for cache operation.
     * @return Error.
     */
    public static ClusterTopologyServerNotFoundException noCoordinatorError(AffinityTopologyVersion topVer) {
        return new ClusterTopologyServerNotFoundException("Mvcc coordinator is not assigned for " +
            "topology version: " + topVer);
    }

    /**
     * @return Error.
     */
    public static ClusterTopologyServerNotFoundException noCoordinatorError() {
        return new ClusterTopologyServerNotFoundException("Mvcc coordinator is not assigned.");
    }

    /**
     * @param cctx Cache context.
     * @param link Link to the row.
     * @param snapshot Mvcc snapshot.
     * @return {@code True} if row is updated for given snapshot.
     * @throws IgniteCheckedException If failed.
     */
    private static boolean isVisible(GridCacheContext cctx, long link,
        MvccSnapshot snapshot)
        throws IgniteCheckedException {
        return invoke(cctx, link, isVisible, snapshot);
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

                assert newMvccCrd == MVCC_CRD_COUNTER_NA || mvccVersionIsValid(newMvccCrd, newMvccCntr, newMvccOpCntr)
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
    private static boolean isCommitted(GridCacheContext cctx, long mvccCrd, long mvccCntr, int mvccOpCntr) throws IgniteCheckedException {
        return state(cctx, mvccCrd, mvccCntr, mvccOpCntr) == TxState.COMMITTED;
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
     * Checks transaction state.
     * @param tx Transaction.
     * @return Checked transaction.
     */
    public static GridNearTxLocal checkActive(GridNearTxLocal tx) {
        if (tx != null && tx.state() != TransactionState.ACTIVE)
            throw new IgniteSQLException("Transaction is already completed.", TRANSACTION_COMPLETED);

        return tx;
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
    @Nullable public static GridNearTxLocal tx(GridKernalContext ctx, @Nullable GridCacheVersion txId) {
        IgniteTxManager tm = ctx.cache().context().tm();

        IgniteInternalTx tx0 = txId == null ? tm.tx() : tm.tx(txId);

        GridNearTxLocal tx = tx0 != null && tx0.user() ? (GridNearTxLocal)tx0 : null;

        if (tx != null) {
            if (!tx.pessimistic() || !tx.repeatableRead()) {
                tx.setRollbackOnly();

                throw new IgniteSQLException("Only pessimistic repeatable read transactions are supported at the moment.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

            }

            if (!tx.isOperationAllowed(true)) {
                tx.setRollbackOnly();

                throw new IgniteSQLException("SQL queries and cache operations " +
                    "may not be used in the same transaction.", IgniteQueryErrorCode.TRANSACTION_TYPE_MISMATCH);
            }
        }

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
            0,
            null
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
     * @param startTx Start transaction flag.
     * @return MVCC query tracker.
     * @throws IgniteCheckedException If failed.
     */
    @NotNull public static MvccQueryTracker mvccTracker(GridCacheContext cctx, boolean startTx) throws IgniteCheckedException {
        assert cctx != null && cctx.mvccEnabled();

        GridNearTxLocal tx = tx(cctx.kernalContext());

        if (tx == null && startTx)
            tx = txStart(cctx, 0);

        return mvccTracker(cctx, tx);
    }

    /**
     * Initialises MVCC filter and returns MVCC query tracker if needed.
     * @param cctx Cache context.
     * @param tx Transaction.
     * @return MVCC query tracker.
     * @throws IgniteCheckedException If failed.
     */
    @NotNull public static MvccQueryTracker mvccTracker(GridCacheContext cctx,
        GridNearTxLocal tx) throws IgniteCheckedException {
        MvccQueryTracker tracker;

        if (tx == null)
            tracker = new MvccQueryTrackerImpl(cctx);
        else if ((tracker = tx.mvccQueryTracker()) == null)
            tracker = new StaticMvccQueryTracker(cctx, requestSnapshot(cctx, tx));

        if (tracker.snapshot() == null)
            // TODO IGNITE-7388
            tracker.requestSnapshot().get();

        return tracker;
    }

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @throws IgniteCheckedException If failed.
     * @return Mvcc snapshot.
     */
    public static MvccSnapshot requestSnapshot(GridCacheContext cctx,
        GridNearTxLocal tx) throws IgniteCheckedException {
        MvccSnapshot snapshot;

        tx = checkActive(tx);

        if ((snapshot = tx.mvccSnapshot()) == null) {
            MvccProcessor prc = cctx.shared().coordinators();

            snapshot = prc.tryRequestSnapshotLocal(tx);

            if (snapshot == null)
                // TODO IGNITE-7388
                snapshot = prc.requestSnapshotAsync(tx).get();

            tx.mvccSnapshot(snapshot);
        }

        return snapshot;
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
    private static class GetVisibleState implements MvccClosure<Integer> {
        /** {@inheritDoc} */
        @Override public Integer apply(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
            int mvccOpCntr, long newMvccCrd, long newMvccCntr, int newMvccOpCntr) throws IgniteCheckedException {

            if (!isVisible(cctx, snapshot, mvccCrd, mvccCntr, mvccOpCntr))
                return MVCC_INVISIBLE;

            if (newMvccCrd == MVCC_CRD_COUNTER_NA)
                return MVCC_VISIBLE;

            assert mvccVersionIsValid(newMvccCrd, newMvccCntr, newMvccOpCntr);

            if (mvccCrd == newMvccCrd && mvccCntr == newMvccCntr) // Double-changed in scope of one transaction.
                return MVCC_VISIBLE_REMOVED;

            return isVisible(cctx, snapshot, newMvccCrd, newMvccCntr, newMvccOpCntr) ? MVCC_VISIBLE_REMOVED :
                MVCC_VISIBLE;
        }
    }

    /**
     * Closure for checking whether the row is visible for given snapshot.
     */
    private static class IsVisible implements MvccClosure<Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean apply(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
            int mvccOpCntr, long newMvccCrd, long newMvccCntr, int newMvccOpCntr) throws IgniteCheckedException {

            if (!isVisible(cctx, snapshot, mvccCrd, mvccCntr, mvccOpCntr))
                return false;

            if (newMvccCrd == MVCC_CRD_COUNTER_NA)
                return true;

            assert mvccVersionIsValid(newMvccCrd, newMvccCntr, newMvccOpCntr);

            if (mvccCrd == newMvccCrd && mvccCntr == newMvccCntr) // Double-changed in scope of one transaction.
                return false;

            return !isVisible(cctx, snapshot, newMvccCrd, newMvccCntr, newMvccOpCntr);
        }
    }

    /**
     * Closure for getting xid_max version of row.
     */
    private static class GetNewVersion implements MvccClosure<MvccVersion> {
        /** {@inheritDoc} */
        @Override public MvccVersion apply(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
            int mvccOpCntr, long newMvccCrd, long newMvccCntr, int newMvccOpCntr) {
            return newMvccCrd == MVCC_CRD_COUNTER_NA ? null : mvccVersion(newMvccCrd, newMvccCntr, newMvccOpCntr);
        }
    }
}
