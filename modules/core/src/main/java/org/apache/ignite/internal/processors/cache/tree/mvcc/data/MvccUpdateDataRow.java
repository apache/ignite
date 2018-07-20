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
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
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

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compare;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isActive;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isVisible;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.unexpectedStateException;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.MVCC_HINTS_BIT_OFF;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.MVCC_HINTS_MASK;

/**
 *
 */
public class MvccUpdateDataRow extends MvccDataRow implements MvccUpdateResult, BPlusTree.TreeVisitorClosure<CacheSearchRow, CacheDataRow> {
    /** */
    private static final int FIRST = DIRTY << 1;
    /** */
    private static final int CHECK_VERSION = FIRST << 1;
    /** */
    private static final int LAST_FOUND = CHECK_VERSION << 1;
    /** */
    private static final int CAN_CLEANUP = LAST_FOUND << 1;
    /** */
    private static final int PRIMARY = CAN_CLEANUP << 1;
    /** */
    private static final int REMOVE_OR_LOCK = PRIMARY << 1;
    /** */
    private static final int NEED_HISTORY = REMOVE_OR_LOCK << 1;
    /** */
    private static final int BACKUP_FLAGS_SET = FIRST;
    /** */
    private static final int PRIMARY_FLAGS_SET = FIRST | CHECK_VERSION | PRIMARY | CAN_WRITE;

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

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param expireTime Expire time.
     * @param mvccSnapshot MVCC snapshot.
     * @param newVer Update version.
     * @param part Partition.
     * @param primary Primary node flag.
     * @param lockOnly Whether no actual update should be done and the only thing to do is to acquire lock.
     * @param needHistory Whether to collect rows created or affected by the current tx.
     * @param cctx Cache context.
     */
    public MvccUpdateDataRow(
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccSnapshot mvccSnapshot,
        MvccVersion newVer,
        int part,
        boolean primary,
        boolean lockOnly,
        boolean needHistory,
        GridCacheContext cctx) {
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

        assert !lockOnly || val == null;

        setFlags(primary ? PRIMARY_FLAGS_SET : BACKUP_FLAGS_SET);

        if (primary && (lockOnly || val == null))
            setFlags(REMOVE_OR_LOCK);

        if (needHistory)
            setFlags(NEED_HISTORY);
    }

    /** {@inheritDoc} */
    @Override public int visit(BPlusTree<CacheSearchRow, CacheDataRow> tree,
        BPlusIO<CacheSearchRow> io,
        long pageAddr,
        int idx, IgniteWriteAheadLogManager wal)
        throws IgniteCheckedException
    {
        unsetFlags(DIRTY);

        RowLinkIO rowIo = (RowLinkIO)io;

        // Lock entry on primary node
        if (isFlagsSet(PRIMARY | FIRST)) {
            long lockCrd = rowIo.getMvccLockCoordinatorVersion(pageAddr, idx);
            long lockCntr = rowIo.getMvccLockCounter(pageAddr, idx);

            // may be already locked
            if (lockCrd != mvccCrd || lockCntr != mvccCntr) {
                if (isActive(cctx, lockCrd, lockCntr, mvccSnapshot)) {
                    resCrd = lockCrd;
                    resCntr = lockCntr;

                    res = ResultType.LOCKED;

                    return setFlags(STOP);
                }

                rowIo.setMvccLockCoordinatorVersion(pageAddr, idx, mvccCrd);
                rowIo.setMvccLockCounter(pageAddr, idx, mvccCntr);

                // TODO Delta record IGNITE-7991

                setFlags(DIRTY);
            }

            // In case it is a REMOVE or locked READ operation and the first entry is an aborted one
            // we have to write lock information to the first committed entry as well
            // because we about to delete all aborted entries before it
            if (!isFlagsSet(REMOVE_OR_LOCK))
                unsetFlags(CAN_WRITE);
        }

        MvccDataRow row = (MvccDataRow)tree.getRow(io, pageAddr, idx, RowData.LINK_WITH_HEADER);

        // Check whether the row was updated by current transaction
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

                if (res == ResultType.PREV_NOT_NULL)
                    oldRow = row;

                setFlags(LAST_FOUND);
            }
        }

        long rowLink = row.link();

        long rowCrd = row.mvccCoordinatorVersion();
        long rowCntr = row.mvccCounter();

        // with hint bits
        int rowOpCntr = (row.mvccTxState() << MVCC_HINTS_BIT_OFF) | (row.mvccOperationCounter() & ~MVCC_HINTS_MASK);

        long rowNewCrd = row.newMvccCoordinatorVersion();
        long rowNewCntr = row.newMvccCounter();

        // with hint bits
        int rowNewOpCntr = (row.newMvccTxState() << MVCC_HINTS_BIT_OFF) | (row.newMvccOperationCounter() & ~MVCC_HINTS_MASK);

        if (!isFlagsSet(LAST_FOUND)) {
            if (!(resCrd == rowCrd && resCntr == rowCntr)) { // It's possible it is a chain of aborted changes
                byte txState = MvccUtils.state(cctx, rowCrd, rowCntr, rowOpCntr);

                if (txState == TxState.COMMITTED) {
                    boolean removed = false;

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

                        removed = txState == TxState.COMMITTED;
                    }

                    if (removed)
                        res = ResultType.PREV_NULL;
                    else {
                        res = ResultType.PREV_NOT_NULL;

                        oldRow = row;
                    }

                    setFlags(LAST_FOUND);

                    if (isFlagsSet(CHECK_VERSION)) {
                        long crdVer, cntr; int opCntr;

                        if (removed) {
                            crdVer = rowNewCrd;
                            cntr = rowNewCntr;
                            opCntr = rowNewOpCntr;
                        }
                        else {
                            crdVer = rowCrd;
                            cntr = rowCntr;
                            opCntr = rowOpCntr;
                        }

                        if (!isVisible(cctx, mvccSnapshot, crdVer, cntr, opCntr, false)) {
                            resCrd = crdVer;
                            resCntr = cntr;

                            res = ResultType.VERSION_MISMATCH; // Write conflict.

                            return setFlags(STOP);
                        }

                        // no need to check further
                        unsetFlags(CHECK_VERSION);
                    }

                    if (isFlagsSet(PRIMARY | REMOVE_OR_LOCK) && cleanupRows != null) {
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

        long cleanupVer = mvccSnapshot.cleanupVersion();

        if (cleanupVer > MVCC_OP_COUNTER_NA // Do not clean if cleanup version is not assigned.
            && !isFlagsSet(CAN_CLEANUP) && isFlagsSet(LAST_FOUND) && res == ResultType.PREV_NULL) {
            // We can cleanup previous row only if it was deleted by another
            // transaction and delete version is less or equal to cleanup one
            if (rowNewCrd < mvccCrd || Long.compare(cleanupVer, rowNewCntr) >= 0)
                setFlags(CAN_CLEANUP);
        }

        if (isFlagsSet(CAN_CLEANUP) || !isFlagsSet(LAST_FOUND)) { // can cleanup aborted versions
            if (cleanupRows == null)
                cleanupRows = new ArrayList<>();

            cleanupRows.add(new MvccLinkAwareSearchRow(cacheId, key, rowCrd, rowCntr, rowOpCntr & ~MVCC_HINTS_MASK, rowLink));
        }
        else {
            // Row obsoleted by current operation, all rows created or updated with current tx.
            if (isFlagsSet(NEED_HISTORY) && (row == oldRow || (rowCrd == mvccCrd && rowCntr == mvccCntr) ||
                (rowNewCrd == mvccCrd && rowNewCntr == mvccCntr))) {
                if (historyRows == null)
                    historyRows = new ArrayList<>();

                historyRows.add(new MvccLinkAwareSearchRow(cacheId, key, rowCrd, rowCntr, rowOpCntr & ~MVCC_HINTS_MASK, rowLink));
            }

            if (cleanupVer > MVCC_OP_COUNTER_NA // Do not clean if cleanup version is not assigned.
                && !isFlagsSet(CAN_CLEANUP) && isFlagsSet(LAST_FOUND)
                && (rowCrd < mvccCrd || Long.compare(cleanupVer, rowCntr) >= 0))
                // all further versions are guaranteed to be less than cleanup version
                setFlags(CAN_CLEANUP);
        }

        return unsetFlags(FIRST);
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
     * @return {@code True} if previous value was non-null.
     */
    @Override public ResultType resultType() {
        return res == null ? ResultType.PREV_NULL : res;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccUpdateDataRow.class, this, "super", super.toString());
    }
}
