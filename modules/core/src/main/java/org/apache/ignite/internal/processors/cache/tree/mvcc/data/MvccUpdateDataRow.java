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

package org.apache.ignite.internal.processors.cache.tree.mvcc.data;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheInvokeResult;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtDetachedCacheEntry;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionImpl;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_HINTS_BIT_OFF;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_MASK;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compare;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isActive;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isVisible;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccVersionIsValid;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.unexpectedStateException;
import static org.apache.ignite.internal.processors.cache.tree.mvcc.data.ResultType.FILTERED;

/**
 *
 */
public class MvccUpdateDataRow extends MvccDataRow implements MvccUpdateResult, BPlusTree.TreeVisitorClosure<CacheSearchRow, CacheDataRow> {
    /** */
    private static final int FIRST = DIRTY << 1;

    /** */
    private static final int CHECK_VERSION = FIRST << 1;

    /** */
    private static final int LAST_COMMITTED_FOUND = CHECK_VERSION << 1;

    /** */
    private static final int CAN_CLEANUP = LAST_COMMITTED_FOUND << 1;

    /** */
    private static final int PRIMARY = CAN_CLEANUP << 1;

    /** */
    private static final int REMOVE_OR_LOCK = PRIMARY << 1;

    /** */
    private static final int NEED_HISTORY = REMOVE_OR_LOCK << 1;
    /**
     * During mvcc transaction processing conflicting row version could be met in storage.
     * Not all such cases should lead to transaction abort.
     * E.g. if UPDATE for a row meets concurrent INSERT for the same row
     * (and row did not exist before both operations) then it means that UPDATE does not see the row at all
     * and can proceed.
     * This flag enables such mode when conflicting version should not lead to abort immediately
     * but more versions should be checked.
     */
    private static final int FAST_UPDATE = NEED_HISTORY << 1;

    /** */
    private static final int FAST_MISMATCH = FAST_UPDATE << 1;

    /** */
    private static final int DELETED = FAST_MISMATCH << 1;

    /** Whether tx has overridden it's own update. */
    private static final int OWN_VALUE_OVERRIDDEN = DELETED << 1;

    /** Force read full entry instead of header only.  */
    private static final int NEED_PREV_VALUE = OWN_VALUE_OVERRIDDEN << 1;

    /** */
    @GridToStringExclude
    private final GridCacheContext cctx;

    /** */
    private ResultType res;

    /** */
    @GridToStringExclude
    private int state;

    /** */
    private List<MvccLinkAwareSearchRow> cleanupRows;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private CacheDataRow oldRow;

    /** */
    @GridToStringExclude
    private long resCrd;

    /** */
    @GridToStringExclude
    private long resCntr;

    /** */
    private List<MvccLinkAwareSearchRow> historyRows;

    /** */
    @GridToStringExclude
    private CacheEntryPredicate filter;

    /** */
    private CacheInvokeResult invokeRes;

    /**
     * @param cctx Cache context.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param part Partition.
     * @param expireTime Expire time.
     * @param mvccSnapshot MVCC snapshot.
     * @param newVer Update version.
     * @param primary Primary node flag.
     * @param lockOnly Whether no actual update should be done and the only thing to do is to acquire lock.
     * @param needHistory Whether to collect rows created or affected by the current tx.
     * @param fastUpdate Fast update visit mode.
     */
    public MvccUpdateDataRow(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        int part,
        long expireTime,
        MvccSnapshot mvccSnapshot,
        MvccVersion newVer,
        @Nullable CacheEntryPredicate filter,
        boolean primary,
        boolean lockOnly,
        boolean needHistory,
        boolean fastUpdate,
        boolean needPrevValue) {
        super(key,
            val,
            ver,
            part,
            expireTime,
            cctx.cacheId(),
            mvccSnapshot,
            newVer);

        this.mvccSnapshot = mvccSnapshot;
        this.cctx = cctx;
        this.filter = filter;
        this.keyAbsentBefore = primary; // True for primary and false for backup (backups do not use this flag).

        assert !lockOnly || val == null;

        int flags = FIRST;

        if (primary)
            flags |= PRIMARY | CHECK_VERSION;

        if (primary && (lockOnly || val == null))
            flags |= CAN_WRITE | REMOVE_OR_LOCK;

        if (needHistory)
            flags |= NEED_HISTORY;

        if (fastUpdate)
            flags |= FAST_UPDATE;

        if(needPrevValue)
            flags |= NEED_PREV_VALUE;

        setFlags(flags);
    }

    /** {@inheritDoc} */
    @Override public int visit(BPlusTree<CacheSearchRow, CacheDataRow> tree,
        BPlusIO<CacheSearchRow> io,
        long pageAddr,
        int idx,
        IgniteWriteAheadLogManager wal)
        throws IgniteCheckedException {
        unsetFlags(DIRTY);

        RowLinkIO rowIo = (RowLinkIO)io;

        // Check if entry is locked on primary node.
        if (isFlagsSet(PRIMARY | FIRST)) {
            long lockCrd = rowIo.getMvccLockCoordinatorVersion(pageAddr, idx);
            long lockCntr = rowIo.getMvccLockCounter(pageAddr, idx);

            // We cannot continue while entry is locked by another transaction.
            if ((lockCrd != mvccCrd || lockCntr != mvccCntr)
                && isActive(cctx, lockCrd, lockCntr, mvccSnapshot)) {
                resCrd = lockCrd;
                resCntr = lockCntr;

                res = ResultType.LOCKED;

                return setFlags(STOP);
            }
        }

        MvccDataRow row = (MvccDataRow)tree.getRow(io, pageAddr, idx, RowData.LINK_WITH_HEADER);

        // Check whether the row was updated by current transaction.
        // In this case the row is already locked by current transaction and visible to it.
        if (isFlagsSet(FIRST)) {
            boolean removed = row.newMvccCoordinatorVersion() != MVCC_CRD_COUNTER_NA;

            long rowCrd, rowCntr; int rowOpCntr;

            if (removed) {
                rowCrd = row.newMvccCoordinatorVersion();
                rowCntr = row.newMvccCounter();
                rowOpCntr = row.newMvccOperationCounter();
            }
            else {
                rowCrd = row.mvccCoordinatorVersion();
                rowCntr = row.mvccCounter();
                rowOpCntr = row.mvccOperationCounter();
            }

            if (compare(mvccSnapshot, rowCrd, rowCntr) == 0) {
                res = mvccOpCntr == rowOpCntr ? ResultType.VERSION_FOUND :
                    removed ? ResultType.PREV_NULL : ResultType.PREV_NOT_NULL;

                if (removed)
                    setFlags(DELETED);
                else {
                    // Actually, full row can be omitted for replace(k,newval) and putIfAbsent, but
                    // operation context is not available here and full row required if filter is set.
                    if (res == ResultType.PREV_NOT_NULL && (isFlagsSet(NEED_PREV_VALUE) || filter != null))
                        oldRow = tree.getRow(io, pageAddr, idx, RowData.FULL);
                    else
                        oldRow = row;
                }

                // TODO: IGNITE-9689: optimize filter usage here. See {@link org.apache.ignite.internal.processors.cache.CacheOperationFilter}.
                if(filter != null && !applyFilter(res == ResultType.PREV_NOT_NULL ? oldRow.value() : null))
                    res = FILTERED;

                setFlags(LAST_COMMITTED_FOUND | OWN_VALUE_OVERRIDDEN);

                // Copy new key flag from the previous row version if it was created by the current tx.
                if (isFlagsSet(PRIMARY))
                    keyAbsentBefore = row.isKeyAbsentBefore();
            }
        }

        long rowLink = row.link();

        long rowCrd = row.mvccCoordinatorVersion();
        long rowCntr = row.mvccCounter();

        // with hint bits
        int rowOpCntr = (row.mvccTxState() << MVCC_HINTS_BIT_OFF) | (row.mvccOperationCounter() & ~MVCC_OP_COUNTER_MASK);

        long rowNewCrd = row.newMvccCoordinatorVersion();
        long rowNewCntr = row.newMvccCounter();

        // with hint bits
        int rowNewOpCntr = (row.newMvccTxState() << MVCC_HINTS_BIT_OFF) | (row.newMvccOperationCounter() & ~MVCC_OP_COUNTER_MASK);

        // Search for youngest committed by another transaction row.
        if (!isFlagsSet(LAST_COMMITTED_FOUND)) {
            if (!(resCrd == rowCrd && resCntr == rowCntr)) { // It's possible it is a chain of aborted changes
                byte txState = MvccUtils.state(cctx, rowCrd, rowCntr, rowOpCntr);

                if (txState == TxState.COMMITTED) {
                    setFlags(LAST_COMMITTED_FOUND);

                    if (rowNewCrd != MVCC_CRD_COUNTER_NA) {
                        if (rowNewCrd == rowCrd && rowNewCntr == rowCntr)
                            // Row was deleted by the same Tx it was created
                            txState = TxState.COMMITTED;
                        else if (rowNewCrd == resCrd && rowNewCntr == resCntr)
                            // The row is linked to the previously checked aborted version;
                            txState = TxState.ABORTED;
                        else
                            // Check with TxLog if removed version is committed;
                            txState = MvccUtils.state(cctx, rowNewCrd, rowNewCntr, rowNewOpCntr);

                        if (!(txState == TxState.COMMITTED || txState == TxState.ABORTED))
                            throw unexpectedStateException(cctx, txState, rowNewCrd, rowNewCntr, rowNewOpCntr, mvccSnapshot);

                        if (txState == TxState.COMMITTED)
                            setFlags(DELETED);
                    }

                    if (isFlagsSet(DELETED))
                        res = ResultType.PREV_NULL;
                    else {
                        res = ResultType.PREV_NOT_NULL;

                        keyAbsentBefore = false;

                        // Actually, full row can be omitted for replace(k,newval) and putIfAbsent, but
                        // operation context is not available here and full row required if filter is set.
                        if( (isFlagsSet(NEED_PREV_VALUE) || filter != null))
                            oldRow = tree.getRow(io, pageAddr, idx, RowData.FULL);
                        else
                            oldRow = row;
                    }

                    if (isFlagsSet(CHECK_VERSION)) {
                        long crdVer, cntr; int opCntr;

                        if (isFlagsSet(DELETED)) {
                            crdVer = rowNewCrd;
                            cntr = rowNewCntr;
                            opCntr = rowNewOpCntr;
                        }
                        else {
                            crdVer = rowCrd;
                            cntr = rowCntr;
                            opCntr = rowOpCntr;
                        }

                        // If last committed row is not visible it is possible write conflict.
                        if (!isVisible(cctx, mvccSnapshot, crdVer, cntr, opCntr, false)) {
                            // In case when row is accessed without previous version check (FAST_UPDATE)
                            // it is possible that we should consider this row non existent for current transaction
                            // without signalling write conflict.
                            // To do this we need to find youngest visible version and if it is removed version
                            // or there is no visible version then there is no conflict.
                            if (isFlagsSet(FAST_UPDATE)
                                && !(isFlagsSet(DELETED)
                                    && isVisible(cctx, mvccSnapshot, rowCrd, rowCntr, rowOpCntr, false))) {
                                res = ResultType.PREV_NULL;

                                setFlags(FAST_MISMATCH);
                            }
                            else {
                                resCrd = crdVer;
                                resCntr = cntr;

                                res = ResultType.VERSION_MISMATCH; // Write conflict.

                                return setFlags(STOP);
                            }
                        }
                    }

                    // TODO: IGNITE-9689: optimize filter usage here. See {@link org.apache.ignite.internal.processors.cache.CacheOperationFilter}.
                    if(filter != null && !applyFilter(res == ResultType.PREV_NOT_NULL ? oldRow.value() : null))
                        res = FILTERED;

                    // Lock entry for primary partition if needed.
                    // If invisible row is found for FAST_UPDATE case we should not lock row.
                    if (!isFlagsSet(DELETED) && isFlagsSet(PRIMARY | REMOVE_OR_LOCK) && !isFlagsSet(FAST_MISMATCH)) {
                        rowIo.setMvccLockCoordinatorVersion(pageAddr, idx, mvccCrd);
                        rowIo.setMvccLockCounter(pageAddr, idx, mvccCntr);

                        // TODO Delta record IGNITE-7991

                        setFlags(DIRTY);
                    }

                    unsetFlags(CAN_WRITE); // No need to acquire write locks anymore
                }
                else if (txState == TxState.ABORTED) { // save aborted version to fast check new version of next row
                    resCrd = rowCrd;
                    resCntr = rowCntr;
                }
                else
                    throw unexpectedStateException(cctx, txState, rowCrd, rowCntr, rowOpCntr, mvccSnapshot);
            }
        }
        // Search for youngest visible row.
        // If we have not found any visible version then we does not see this row.
        else if (isFlagsSet(FAST_MISMATCH)) {
            assert !isFlagsSet(CAN_CLEANUP);
            assert mvccVersionIsValid(rowNewCrd, rowNewCntr, rowNewOpCntr);

            // Update version could be visible only if it is removal version,
            // previous create versions were already checked in previous step and are definitely invisible.
            // If we found visible removal version then we does not see this row.
            if (isVisible(cctx, mvccSnapshot, rowNewCrd, rowNewCntr, rowNewOpCntr, false))
                unsetFlags(FAST_MISMATCH);
            // If the youngest visible for current transaction version is not removal version then it is write conflict.
            else if (isVisible(cctx, mvccSnapshot, rowCrd, rowCntr, rowOpCntr, false)) {
                resCrd = rowCrd;
                resCntr = rowCntr;

                res = ResultType.VERSION_MISMATCH;

                return setFlags(STOP);
            }
        }

        long cleanupVer = mvccSnapshot.cleanupVersion();

        if (cleanupVer > MVCC_OP_COUNTER_NA // Do not clean if cleanup version is not assigned.
            && !isFlagsSet(CAN_CLEANUP)
            && isFlagsSet(LAST_COMMITTED_FOUND | DELETED)) {
            assert mvccVersionIsValid(rowNewCrd, rowNewCntr, rowNewOpCntr);

            // We can cleanup previous row only if it was deleted by another
            // transaction and delete version is less or equal to cleanup one
            if (rowNewCrd < mvccCrd || Long.compare(cleanupVer, rowNewCntr) >= 0)
                setFlags(CAN_CLEANUP);
        }

        if (isFlagsSet(CAN_CLEANUP)
            || !isFlagsSet(LAST_COMMITTED_FOUND)) { // can cleanup aborted versions
            if (cleanupRows == null)
                cleanupRows = new ArrayList<>();

            cleanupRows.add(new MvccLinkAwareSearchRow(cacheId, key, rowCrd, rowCntr, rowOpCntr & ~MVCC_OP_COUNTER_MASK, rowLink));
        }
        else {
            // Row obsoleted by current operation, all rows created or updated with current tx.
            if (isFlagsSet(NEED_HISTORY)
                && (row == oldRow
                    || (rowCrd == mvccCrd && rowCntr == mvccCntr)
                    || (rowNewCrd == mvccCrd && rowNewCntr == mvccCntr))) {
                if (historyRows == null)
                    historyRows = new ArrayList<>();

                historyRows.add(new MvccLinkAwareSearchRow(cacheId, key, rowCrd, rowCntr, rowOpCntr & ~MVCC_OP_COUNTER_MASK, rowLink));
            }

            if (cleanupVer > MVCC_OP_COUNTER_NA // Do not clean if cleanup version is not assigned.
                && !isFlagsSet(CAN_CLEANUP)
                && isFlagsSet(LAST_COMMITTED_FOUND)
                && (rowCrd < mvccCrd || Long.compare(cleanupVer, rowCntr) >= 0))
                // all further versions are guaranteed to be less than cleanup version
                setFlags(CAN_CLEANUP);
        }

        return unsetFlags(FIRST);
    }

    /**
     * Apply filter.
     *
     * @param val0 Previous value.
     * @return Filter result.
     */
    private boolean applyFilter(final CacheObject val0) {
        GridCacheEntryEx e = new GridDhtDetachedCacheEntry(cctx, key) {
            @Nullable @Override public CacheObject peekVisibleValue() {
                return val0;
            }
        };

        return filter.apply(e);
    }

    /** {@inheritDoc} */
    @Override public int state() {
        return state;
    }

    /**
     * @return Old row.
     */
    public CacheDataRow oldRow() {
        return oldRow;
    }

    /**
     * @return Result type.
     */
    @NotNull @Override public ResultType resultType() {
        return res == null ? defaultResult() : res;
    }

    /**
     * Evaluate default result type.
     *
     * @return Result type.
     */
    @NotNull private ResultType defaultResult() {
        assert res == null;

        if (filter != null && !applyFilter(null))
            res = FILTERED;
        else
            res = ResultType.PREV_NULL; // Default.

        return res;
    }

    /**
     * @return Rows which are safe to cleanup.
     */
    public List<MvccLinkAwareSearchRow> cleanupRows() {
        return cleanupRows;
    }

    /**
     * @return Result version.
     */
    @Override public MvccVersion resultVersion() {
        switch (resultType()) {
            case VERSION_FOUND:
            case PREV_NULL:

                return new MvccVersionImpl(mvccCrd, mvccCntr, mvccOpCntr);
            case PREV_NOT_NULL:

                return new MvccVersionImpl(oldRow.mvccCoordinatorVersion(), oldRow.mvccCounter(), oldRow.mvccOperationCounter());
            case LOCKED:
            case VERSION_MISMATCH:

                assert resCrd != MVCC_CRD_COUNTER_NA && resCntr != MVCC_COUNTER_NA;

                return new MvccVersionImpl(resCrd, resCntr, MVCC_OP_COUNTER_NA);
            default:

                throw new IllegalStateException("Unexpected result type: " + resultType());
        }
    }

    /** {@inheritDoc} */
    @Override public List<MvccLinkAwareSearchRow> history() {
        if (isFlagsSet(NEED_HISTORY) && historyRows == null)
            historyRows = new ArrayList<>();

        return historyRows;
    }

    /** {@inheritDoc} */
    @Override public boolean isOwnValueOverridden() {
        return isFlagsSet(OWN_VALUE_OVERRIDDEN);
    }

    /** */
    public void value(CacheObject val0) {
        val = val0;
    }

    /** */
    public void invokeResult(CacheInvokeResult invokeRes) {
        this.invokeRes = invokeRes;
    }

    /**
     * @return Invoke result.
     */
    @Override public CacheInvokeResult invokeResult(){
        return invokeRes;
    }

    /** */
    private boolean isFlagsSet(int flags) {
        return (state & flags) == flags;
    }

    /** */
    private int setFlags(int flags) {
        return state |= flags;
    }

    /** */
    private int unsetFlags(int flags) {
        return state &= (~flags);
    }

    /** */
    public void resultType(ResultType type) {
        res = type;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccUpdateDataRow.class, this, "super", super.toString());
    }
}
