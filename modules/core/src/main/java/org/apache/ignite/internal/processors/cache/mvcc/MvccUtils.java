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
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_OP_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;

/**
 * Utils for MVCC.
 */
public class MvccUtils {
    /** */
    private static final MvccClosure<Boolean> isNewVisible = new IsNewVisible();

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
        long snapshotCrd = snapshot.coordinatorVersion();
        long snapshotCntr = snapshot.counter();
        long snapshotOpCntr = snapshot.operationCounter();

        if (mvccCrd < snapshotCrd)
            return isCommitted(cctx, mvccCrd, mvccCntr, useTxLog);
        else if (mvccCrd == snapshotCrd && mvccCntr < snapshotCntr)
            return !snapshot.activeTransactions().contains(mvccCntr) && isCommitted(cctx, mvccCrd, mvccCntr, useTxLog);
        else
            return mvccCrd == snapshotCrd && mvccCntr == snapshotCntr && opCntr < snapshotOpCntr; // we don't see own pending updates
    }

    /**
     * Checks if version is visible from the given snapshot.
     *
     * @param cctx Cache context.
     * @param snapshot Snapshot.
     * @param mvccCrd Mvcc coordinator.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation cunter.
     * @param newMvccCrd New mvcc coordinator.
     * @param newMvccCntr New mvcc counter.
     * @param newMvccOpCntr New mvcc operation counter.
     * @return {@code True} if visible.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean isVisible(GridCacheContext cctx, MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
        int mvccOpCntr, long newMvccCrd,
        long newMvccCntr, int newMvccOpCntr) throws IgniteCheckedException {
        return isVisible(cctx, snapshot, mvccCrd, mvccCntr, mvccOpCntr)
            && (newMvccCrd == 0 || !isVisible(cctx, snapshot, newMvccCrd, newMvccCntr, newMvccOpCntr));
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

                assert mvccVersionIsValid(mvccCrd, mvccCntr, mvccOpCntr);

                long newMvccCrd = dataIo.newMvccCoordinator(pageAddr, offset);
                long newMvccCntr = dataIo.newMvccCounter(pageAddr, offset);
                int newMvccOpCntr = dataIo.newMvccOperationCounter(pageAddr, offset);

                assert newMvccCrd == 0 || mvccVersionIsValid(newMvccCrd, newMvccCntr, newMvccOpCntr);

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
     * @param useTxLog Determines whether TxLog is used.
     * @return {@code True} in case the corresponding transaction is in {@code TxState.COMMITTED} state.
     * @throws IgniteCheckedException If failed.
     */
    private static boolean isCommitted(GridCacheContext cctx, long mvccCrd, long mvccCntr, boolean useTxLog) throws IgniteCheckedException {
        return !useTxLog || cctx.shared().coordinators().state(mvccCrd, mvccCntr) == TxState.COMMITTED;
    }

    /**
     * Throw an {@link UnsupportedOperationException} if this cache is transactional and MVCC is enabled with
     * appropriate message about corresponding operation type.
     * @param cctx Cache context.
     * @param opType operation type to mention in error message.
     */
    public static void verifyMvccOperationSupport(GridCacheContext<?, ?> cctx, String opType) {
        if (!cctx.atomic() && cctx.gridConfig().isMvccEnabled())
            throw new UnsupportedOperationException(opType + " operations are not supported on transactional " +
                "caches when MVCC is enabled.");
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
            return newMvccCrd != 0 && isVisible(cctx, snapshot, newMvccCrd, newMvccCntr, newMvccOpCntr);
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
}
