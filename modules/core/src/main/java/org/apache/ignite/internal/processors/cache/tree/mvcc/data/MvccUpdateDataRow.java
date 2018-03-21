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
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.isVisible;

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
    private static final int REMOVE = PRIMARY << 1;
    /** */
    private static final int FIRST_REMOVED = REMOVE << 1;

    /** */
    private final GridCacheContext cctx;

    /** */
    private ResultType res;

    /** */
    private int state;

    /** */
    private List<MvccLinkAwareSearchRow> cleanupRows;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private CacheDataRow oldRow;

    /** */
    private long resCrd;

    /** */
    private long resCntr;

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param expireTime Expire time.
     * @param mvccSnapshot MVCC snapshot.
     * @param newVer Update version.
     * @param part Partition.
     * @param primary Primary node flag.
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
        GridCacheContext cctx) {
        super(key,
            val,
            ver,
            part,
            expireTime,
            cctx.cacheId(),
            mvccSnapshot.coordinatorVersion(),
            mvccSnapshot.counter(),
            newVer);

        this.mvccSnapshot = mvccSnapshot;
        this.cctx = cctx;

        if (primary)
            setFlags(FIRST
                | CHECK_VERSION
                | PRIMARY
                | (val == null ? REMOVE : 0)
                | CAN_WRITE);
        else
            setFlags(FIRST);
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

            if (cctx.kernalContext().coordinators().isActive(lockCrd, lockCntr)) {
                resCrd = lockCrd;
                resCntr = lockCntr;

                res = ResultType.LOCKED;

                return setFlags(STOP);
            }

            rowIo.setMvccLockCoordinatorVersion(pageAddr, idx, mvccCrd);
            rowIo.setMvccLockCounter(pageAddr, idx, mvccCntr);

            // TODO Delta record IGNITE-7991

            setFlags(DIRTY);

            // In case it is a REMOVE operation and the first entry is an aborted one
            // we have to write lock information to the first committed entry as well
            // because we about to delete all aborted entries before it
            if (!isFlagsSet(REMOVE))
                unsetFlags(CAN_WRITE);
        }

        if (isFlagsSet(FIRST)) {
            oldRow = tree.getRow(io, pageAddr, idx, RowData.LINK_WITH_HEADER);

            boolean removed = oldRow.newMvccCoordinatorVersion() != 0;

            long rowCrd, rowCntr;

            if (removed) {
                rowCrd = oldRow.newMvccCoordinatorVersion();
                rowCntr = oldRow.newMvccCounter();
            }
            else {
                rowCrd = oldRow.mvccCoordinatorVersion();
                rowCntr = oldRow.mvccCounter();
            }

            // Check whether the row was updated by this transaction
            int cmp = Long.compare(mvccCrd, rowCrd);

            if (cmp == 0)
                cmp = Long.compare(mvccSnapshot.counter(), rowCntr);

            if (cmp == 0) {
                res = ResultType.VERSION_FOUND;

                assert !isFlagsSet(PRIMARY); // Currently can happen on backup node only

                setFlags(LAST_FOUND);
            }
        }

        long rowCrd = rowIo.getMvccCoordinatorVersion(pageAddr, idx);
        long rowCntr = rowIo.getMvccCounter(pageAddr, idx);

        if (!isFlagsSet(LAST_FOUND)) {
            byte txState = cctx.kernalContext().coordinators().state(rowCrd, rowCntr);

            if (txState == TxState.COMMITTED) {
                if (oldRow.link() != rowIo.getLink(pageAddr, idx))
                    oldRow = tree.getRow(io, pageAddr, idx, RowData.LINK_WITH_HEADER);

                boolean removed = false;

                if (oldRow.newMvccCoordinatorVersion() != 0) {
                    if (oldRow.newMvccCoordinatorVersion() == resCrd && oldRow.newMvccCounter() == resCntr)
                        // The row is linked to the previous aborted version;
                        txState = TxState.ABORTED;
                    else
                        // Double check with TxLog if removed version is committed;
                        txState = cctx.shared().coordinators().state(oldRow.newMvccCoordinatorVersion(), oldRow.newMvccCounter());

                    if (!(txState == TxState.COMMITTED || txState == TxState.ABORTED))
                        throw new IllegalStateException("Unexpected state: " + txState);

                    removed = txState == TxState.COMMITTED;
                }

                if (removed) {
                    res = ResultType.PREV_NULL;

                    setFlags(LAST_FOUND | FIRST_REMOVED);
                }
                else {
                    res = ResultType.PREV_NOT_NULL;

                    setFlags(LAST_FOUND);
                }

                if (isFlagsSet(CHECK_VERSION)) {
                    assert res == ResultType.PREV_NULL || res == ResultType.PREV_NOT_NULL;

                    long crdVer = removed ? oldRow.newMvccCoordinatorVersion() : oldRow.mvccCoordinatorVersion();
                    long cntr = removed ? oldRow.newMvccCounter() : oldRow.mvccCounter();

                    if (!isVisible(cctx, mvccSnapshot, crdVer, cntr, false)) {
                        resCrd = crdVer;
                        resCntr = cntr;

                        res = ResultType.VERSION_MISMATCH; // Write conflict.

                        return setFlags(STOP);
                    }

                    // no need to check further
                    unsetFlags(CHECK_VERSION);
                }

                if (cleanupRows != null && isFlagsSet(PRIMARY | REMOVE)) {
                    rowIo.setMvccLockCoordinatorVersion(pageAddr, idx, mvccCrd);
                    rowIo.setMvccLockCounter(pageAddr, idx, mvccCntr);

                    // TODO Delta record IGNITE-7991

                    setFlags(DIRTY);
                }

                unsetFlags(CAN_WRITE); // No need to acquire write locks anymore
            }
            else if (txState == TxState.ABORTED) {// save aborted version to fast check new version of next row
                resCrd = rowCrd;
                resCntr = rowCntr;
            }
            else
                throw new IllegalStateException("Unexpected state: " + txState);
        }

        if (isFlagsSet(FIRST_REMOVED)) {
            assert isFlagsSet(LAST_FOUND);
            assert !isFlagsSet(CAN_CLEANUP);

            long crdVer = oldRow.newMvccCoordinatorVersion();

            int cmp = crdVer < mvccCrd ? 1 : Long.compare(mvccSnapshot.cleanupVersion(), oldRow.newMvccCounter());

            if (cmp >= 0)
                setFlags(CAN_CLEANUP); // can cleanup older row in case it was removed;

            // all further rows cannot be removed
            unsetFlags(FIRST_REMOVED);
        }

        if (isFlagsSet(CAN_CLEANUP) || !isFlagsSet(LAST_FOUND)) { // can cleanup aborted versions
            if (cleanupRows == null)
                cleanupRows = new ArrayList<>();

            cleanupRows.add(new MvccLinkAwareSearchRow(cacheId, key, rowCrd, rowCntr, rowIo.getLink(pageAddr, idx)));
        }
        else if (!isFlagsSet(CAN_CLEANUP) && isFlagsSet(LAST_FOUND)) {
            int cmp = rowCrd < mvccCrd ? 1 : Long.compare(mvccSnapshot.cleanupVersion(), rowCntr);

            if (cmp >= 0)
                setFlags(CAN_CLEANUP); // can cleanup oder rows;
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

                return new MvccVersionImpl(mvccCrd, mvccCntr);
            case PREV_NOT_NULL:

                return new MvccVersionImpl(oldRow.mvccCoordinatorVersion(), oldRow.newMvccCounter());
            case LOCKED:
            case VERSION_MISMATCH:

                return new MvccVersionImpl(resCrd, resCntr);
            default:

                throw new IllegalStateException("Unexpected result type: " + resultType());
        }
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
