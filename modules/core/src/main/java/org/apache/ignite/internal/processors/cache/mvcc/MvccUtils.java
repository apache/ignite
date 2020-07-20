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
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxAlreadyCompletedCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxUnexpectedStateCheckedException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.transactions.TransactionMixedModeException;
import org.apache.ignite.transactions.TransactionState;
import org.apache.ignite.transactions.TransactionUnsupportedConcurrencyException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Utils for MVCC.
 */
public class MvccUtils {
    /** */
    public static final int MVCC_KEY_ABSENT_BEFORE_OFF = 29;

    /** */
    public static final int MVCC_HINTS_BIT_OFF = MVCC_KEY_ABSENT_BEFORE_OFF + 1;

    /** Mask for KeyAbsent flag. */
    public static final int MVCC_KEY_ABSENT_BEFORE_MASK = 1 << MVCC_KEY_ABSENT_BEFORE_OFF;

    /** Mask for tx hints. (2 highest bits)  */
    public static final int MVCC_HINTS_MASK = Integer.MIN_VALUE >> 1;

    /** Mask for operation counter bits. (Excludes hints and flags) */
    public static final int MVCC_OP_COUNTER_MASK = ~(Integer.MIN_VALUE >> 2);

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

    /** Used as 'read' snapshot op counter. */
    public static final int MVCC_READ_OP_CNTR = MVCC_OP_COUNTER_MASK;

    /** */
    public static final int MVCC_INVISIBLE = 0;

    /** */
    public static final int MVCC_VISIBLE_REMOVED = 1;

    /** */
    public static final int MVCC_VISIBLE = 2;

    /** A special version visible by everyone */
    public static final MvccVersion INITIAL_VERSION =
        mvccVersion(MVCC_CRD_START_CNTR, MVCC_INITIAL_CNTR, MVCC_START_OP_CNTR);

    /** A special snapshot for which all committed versions are visible */
    public static final MvccSnapshot MVCC_MAX_SNAPSHOT =
        new MvccSnapshotWithoutTxs(Long.MAX_VALUE, Long.MAX_VALUE, MVCC_READ_OP_CNTR, MVCC_COUNTER_NA);

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

        byte state;

        return cctx.kernalContext().coordinators().hasLocalTransaction(mvccCrd, mvccCntr) ||
            (state = state(cctx, mvccCrd, mvccCntr, 0)) != TxState.COMMITTED && state != TxState.ABORTED;
    }

    /**
     * @param cctx Cache context.
     * @param mvccCrd Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @return TxState
     * @see TxState
     */
    public static byte state(GridCacheContext cctx, long mvccCrd, long mvccCntr, int mvccOpCntr) {
        return state(cctx.kernalContext().coordinators(), mvccCrd, mvccCntr, mvccOpCntr);
    }

    /**
     * @param grp Cache group context.
     * @param mvccCrd Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @return TxState
     * @see TxState
     */
    public static byte state(CacheGroupContext grp, long mvccCrd, long mvccCntr, int mvccOpCntr) {
        return state(grp.shared().coordinators(), mvccCrd, mvccCntr, mvccOpCntr);
    }

    /**
     * @param proc Mvcc processor.
     * @param mvccCrd Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @return TxState
     * @see TxState
     */
    public static byte state(MvccProcessor proc, long mvccCrd, long mvccCntr, int mvccOpCntr) {
        if (compare(INITIAL_VERSION, mvccCrd, mvccCntr, mvccOpCntr) == 0)
            return TxState.COMMITTED; // Initial version is always committed;

        if ((mvccOpCntr & MVCC_HINTS_MASK) != 0)
            return (byte)(mvccOpCntr >>> MVCC_HINTS_BIT_OFF);

        MvccCoordinator crd = proc.currentCoordinator();

        byte state = proc.state(mvccCrd, mvccCntr);

        if ((state == TxState.NA || state == TxState.PREPARED)
            && (crd.unassigned() // Recovery from WAL.
            || (crd.initialized() && mvccCrd < crd.version()))) // Stale row.
            state = TxState.ABORTED;

        return state;
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
     * @param opCntrWithHints Operation counter.
     * @param useTxLog {@code True} if TxLog should be used.
     * @return {@code True} if visible.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean isVisible(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
        int opCntrWithHints, boolean useTxLog) throws IgniteCheckedException {
        int opCntr = opCntrWithHints & MVCC_OP_COUNTER_MASK;

        if (mvccCrd == MVCC_CRD_COUNTER_NA) {
            assert mvccCntr == MVCC_COUNTER_NA && opCntrWithHints == MVCC_OP_COUNTER_NA
                : "rowVer=" + mvccVersion(mvccCrd, mvccCntr, opCntrWithHints) + ", snapshot=" + snapshot;

            return false; // Unassigned version is always invisible
        }

        if (compare(INITIAL_VERSION, mvccCrd, mvccCntr, opCntr) == 0)
            return true; // Initial version is always visible

        long snapshotCrd = snapshot.coordinatorVersion();

        long snapshotCntr = snapshot.counter();
        int snapshotOpCntr = snapshot.operationCounter();

        assert (snapshotOpCntr & ~MVCC_OP_COUNTER_MASK) == 0 : snapshot;

        if (mvccCrd > snapshotCrd)
            return false; // Rows in the future are never visible.

        if (mvccCrd < snapshotCrd) {
            if (!useTxLog)
                return true; // The checking row is expected to be committed.

            byte state = state(cctx, mvccCrd, mvccCntr, opCntrWithHints);

            if (MVCC_MAX_SNAPSHOT.compareTo(snapshot) != 0 // Special version which sees all committed entries.
                && state != TxState.COMMITTED && state != TxState.ABORTED)
                throw unexpectedStateException(cctx, state, mvccCrd, mvccCntr, opCntrWithHints, snapshot);

            return state == TxState.COMMITTED;
        }

        if (mvccCntr > snapshotCntr) // we don't see future updates
            return false;

        // Basically we can make fast decision about visibility if found rows from the same transaction.
        // But we can't make such decision for read-only queries,
        // because read-only queries use last committed version in it's snapshot which could be actually aborted
        // (during transaction recovery we do not know whether recovered transaction was committed or aborted).
        if (mvccCntr == snapshotCntr && snapshotOpCntr != MVCC_READ_OP_CNTR) {
            assert opCntr <= snapshotOpCntr : "rowVer=" + mvccVersion(mvccCrd, mvccCntr, opCntrWithHints) + ", snapshot=" + snapshot;

            return opCntr < snapshotOpCntr; // we don't see own pending updates
        }

        if (snapshot.activeTransactions().contains(mvccCntr)) // we don't see of other transactions' pending updates
            return false;

        if (!useTxLog)
            return true; // The checking row is expected to be committed.

        byte state = state(cctx, mvccCrd, mvccCntr, opCntrWithHints);

        if (state != TxState.COMMITTED && state != TxState.ABORTED)
            throw unexpectedStateException(cctx, state, mvccCrd, mvccCntr, opCntrWithHints, snapshot);

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
    public static IgniteTxUnexpectedStateCheckedException unexpectedStateException(
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
    public static IgniteTxUnexpectedStateCheckedException unexpectedStateException(
        GridCacheContext cctx, byte state, long crd, long cntr,
        int opCntr, MvccSnapshot snapshot) {
        return unexpectedStateException(cctx.kernalContext(), state, crd, cntr, opCntr, snapshot);
    }

    /** */
    private static IgniteTxUnexpectedStateCheckedException unexpectedStateException(GridKernalContext ctx, byte state, long crd, long cntr,
        int opCntr, MvccSnapshot snapshot) {
        String msg = "Unexpected state: [state=" + state + ", rowVer=" + crd + ":" + cntr + ":" + opCntr;

        if (snapshot != null)
            msg += ", txVer=" + snapshot.coordinatorVersion() + ":" + snapshot.counter() + ":" + snapshot.operationCounter();

        msg += ", localNodeId=" + ctx.localNodeId() + "]";

        return new IgniteTxUnexpectedStateCheckedException(msg);
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
            || (cmp = Integer.compare(mvccOpCntrLeft & MVCC_OP_COUNTER_MASK, mvccOpCntrRight & MVCC_OP_COUNTER_MASK)) != 0)
            return cmp;

        return 0;
    }

    /**
     * Compares left version (xid_min) with the given version ignoring operation counter.
     *
     * @param left Version.
     * @param right Version.
     * @return Comparison result, see {@link Comparable}.
     */
    public static int compareIgnoreOpCounter(MvccVersion left, MvccVersion right) {
        return compare(left.coordinatorVersion(), left.counter(), 0, right.coordinatorVersion(), right.counter(), 0);
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
        return mvccVersionIsValid(crdVer, cntr) && (opCntr & MVCC_OP_COUNTER_MASK) != MVCC_OP_COUNTER_NA;
    }

    /**
     * @param crdVer Mvcc coordinator version.
     * @param cntr Counter.
     * @return {@code True} if version is valid.
     */
    public static boolean mvccVersionIsValid(long crdVer, long cntr) {
        return crdVer > MVCC_CRD_COUNTER_NA && cntr > MVCC_COUNTER_NA;
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

        int pageSize = pageMem.realPageSize(grpId);

        long pageId = pageId(link);
        int itemId = itemId(link);
        long page = pageMem.acquirePage(grpId, pageId);

        try {
            long pageAddr = pageMem.readLock(grpId, pageId, page);

            try {
                DataPageIO dataIo = DataPageIO.VERSIONS.forPage(pageAddr);

                return invoke(cctx, dataIo, pageAddr, itemId, pageSize, clo, snapshot);
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
     * @param cctx Cache context.
     * @param dataIo Data page IO.
     * @param pageAddr Page address.
     * @param itemId Item Id.
     * @param pageSize Page size.
     * @param clo Closure.
     * @param snapshot Mvcc snapshot.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private static <R> R invoke(GridCacheContext cctx, DataPageIO dataIo, long pageAddr, int itemId, int pageSize,
        MvccClosure<R> clo, MvccSnapshot snapshot) throws IgniteCheckedException {
        int offset = dataIo.getPayloadOffset(pageAddr, itemId, pageSize, MVCC_INFO_SIZE);

        long mvccCrd = dataIo.mvccCoordinator(pageAddr, offset);
        long mvccCntr = dataIo.mvccCounter(pageAddr, offset);
        int mvccOpCntr = dataIo.rawMvccOperationCounter(pageAddr, offset);

        assert mvccVersionIsValid(mvccCrd, mvccCntr, mvccOpCntr ) : mvccVersion(mvccCrd, mvccCntr, mvccOpCntr);

        long newMvccCrd = dataIo.newMvccCoordinator(pageAddr, offset);
        long newMvccCntr = dataIo.newMvccCounter(pageAddr, offset);
        int newMvccOpCntr = dataIo.rawNewMvccOperationCounter(pageAddr, offset);

        assert newMvccCrd == MVCC_CRD_COUNTER_NA || mvccVersionIsValid(newMvccCrd, newMvccCntr, newMvccOpCntr)
            : mvccVersion(newMvccCrd, newMvccCntr, newMvccOpCntr);

        return clo.apply(cctx, snapshot, mvccCrd, mvccCntr, mvccOpCntr, newMvccCrd, newMvccCntr, newMvccOpCntr);
    }

    /**
     * @param cctx Cache context.
     * @param snapshot Mvcc snapshot.
     * @param dataIo Data page IO.
     * @param pageAddr Page address.
     * @param itemId Item Id.
     * @param pageSize Page size.
     * @return {@code true} If the row is visible.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean isVisible(GridCacheContext cctx, MvccSnapshot snapshot, DataPageIO dataIo,
        long pageAddr, int itemId, int pageSize) throws IgniteCheckedException {
        return invoke(cctx, dataIo, pageAddr, itemId, pageSize, isVisible, snapshot);
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
    public static GridNearTxLocal checkActive(GridNearTxLocal tx) throws IgniteTxAlreadyCompletedCheckedException {
        if (tx != null && tx.state() != TransactionState.ACTIVE)
            throw new IgniteTxAlreadyCompletedCheckedException("Transaction is already completed.");

        return tx;
    }


    /**
     * @param ctx Grid kernal context.
     * @return Currently started user transaction, or {@code null} if none started.
     * @throws TransactionUnsupportedConcurrencyException If transaction mode is not supported when MVCC is enabled.
     * @throws TransactionMixedModeException If started transaction spans non MVCC caches.
     */
    @Nullable public static GridNearTxLocal tx(GridKernalContext ctx) {
        return tx(ctx, null);
    }

    /**
     * @param ctx Grid kernal context.
     * @param txId Transaction ID.
     * @return Currently started user transaction, or {@code null} if none started.
     * @throws TransactionUnsupportedConcurrencyException If transaction mode is not supported when MVCC is enabled.
     * @throws TransactionMixedModeException If started transaction spans non MVCC caches.
     */
    @Nullable public static GridNearTxLocal tx(GridKernalContext ctx, @Nullable GridCacheVersion txId) {
        IgniteTxManager tm = ctx.cache().context().tm();

        IgniteInternalTx tx0 = txId == null ? tm.tx() : tm.tx(txId);

        GridNearTxLocal tx = tx0 != null && tx0.user() ? (GridNearTxLocal)tx0 : null;

        if (tx != null) {
            if (!tx.pessimistic()) {
                tx.setRollbackOnly();

                throw new TransactionUnsupportedConcurrencyException("Only pessimistic transactions are supported when MVCC is enabled.");
            }

            if (!tx.isOperationAllowed(true)) {
                tx.setRollbackOnly();

                throw new TransactionMixedModeException("Operations on MVCC caches are not permitted in transactions spanning non MVCC caches.");
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
            TransactionConfiguration tcfg = CU.transactionConfiguration(cctx, ctx.config());

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
            null,
            false
        );

        tx.syncMode(FULL_SYNC);

        return tx;
    }

    /**
     * @param ctx Grid kernal context.
     * @return Whether MVCC is enabled or not.
     */
    public static boolean mvccEnabled(GridKernalContext ctx) {
        return ctx.coordinators().mvccEnabled();
    }

    /**
     * Initialises MVCC filter and returns MVCC query tracker if needed.
     * @param cctx Cache context.
     * @param autoStartTx Start transaction flag.
     * @return MVCC query tracker.
     * @throws IgniteCheckedException If failed.
     */
    @NotNull public static MvccQueryTracker mvccTracker(GridCacheContext cctx, boolean autoStartTx)
        throws IgniteCheckedException {
        assert cctx != null && cctx.mvccEnabled();

        GridNearTxLocal tx = tx(cctx.kernalContext());

        if (tx == null && autoStartTx)
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
        else
            tracker = new StaticMvccQueryTracker(cctx, requestSnapshot(tx));

        if (tracker.snapshot() == null)
            // TODO IGNITE-7388
            tracker.requestSnapshot().get();

        return tracker;
    }

    /**
     * @param tx Transaction.
     * @throws IgniteCheckedException If failed.
     * @return Mvcc snapshot.
     */
    public static MvccSnapshot requestSnapshot(@NotNull GridNearTxLocal tx) throws IgniteCheckedException {
        MvccSnapshot snapshot = tx.mvccSnapshot();

        if (snapshot == null)
            // TODO IGNITE-7388
            return tx.requestSnapshot().get();

        return snapshot;
    }

    /**
     * Throws atomicity modes compatibility validation exception.
     *
     * @param ccfg1 Config 1.
     * @param ccfg2 Config 2.
     */
    public static void throwAtomicityModesMismatchException(CacheConfiguration ccfg1, CacheConfiguration ccfg2) {
        throw new IgniteException("Caches with transactional_snapshot atomicity mode cannot participate in the same" +
            " transaction with caches having another atomicity mode. [cacheName=" + ccfg1.getName() +
            ", cacheMode=" + ccfg1.getAtomicityMode() + ", anotherCacheName=" + ccfg2.getName() +
            " anotherCacheMode=" + ccfg2.getAtomicityMode() + ']');
    }

    /** */
    private static MvccVersion mvccVersion(long crd, long cntr, int opCntr) {
        return new MvccVersionImpl(crd, cntr, opCntr);
    }

    /**
     * @param v1 First MVCC version.
     * @param v2 Second MVCC version.
     * @return {@code True} if compared versions belongs to the same transaction.
     */
    public static boolean belongToSameTx(MvccVersion v1, MvccVersion v2) {
        return v1.coordinatorVersion() == v2.coordinatorVersion() && v1.counter() == v2.counter();
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
