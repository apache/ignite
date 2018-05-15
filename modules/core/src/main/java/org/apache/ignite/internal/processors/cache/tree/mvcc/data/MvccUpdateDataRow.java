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
import org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor;
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

        if (primary && val == null && !lockOnly)
            setFlags(REMOVE);
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
            }

            // In case it is a REMOVE operation and the first entry is an aborted one
            // we have to write lock information to the first committed entry as well
            // because we about to delete all aborted entries before it
            if (!isFlagsSet(REMOVE))
                unsetFlags(CAN_WRITE);
        }

        // Check whether the row was updated by current transaction
        if (isFlagsSet(FIRST)) {
            oldRow = tree.getRow(io, pageAddr, idx, RowData.LINK_WITH_HEADER);

            boolean removed = oldRow.newMvccCoordinatorVersion() != 0;

            long rowCrd, rowCntr; int rowOpCntr;

            if (removed) {
                rowCrd = oldRow.newMvccCoordinatorVersion();
                rowCntr = oldRow.newMvccCounter();
                rowOpCntr = oldRow.newMvccOperationCounter();
            }
            else {
                rowCrd = oldRow.mvccCoordinatorVersion();
                rowCntr = oldRow.mvccCounter();
                rowOpCntr = oldRow.mvccOperationCounter();
            }

            if (MvccUtils.compare(mvccSnapshot, rowCrd, rowCntr) == 0) {
                res = mvccOpCntr == rowOpCntr ? ResultType.VERSION_FOUND :
                    removed ? ResultType.PREV_NULL : ResultType.PREV_NOT_NULL;

                setFlags(LAST_FOUND);
            }
        }

        long rowLink = rowIo.getLink(pageAddr, idx);
        long rowCrd = rowIo.getMvccCoordinatorVersion(pageAddr, idx);
        long rowCntr = rowIo.getMvccCounter(pageAddr, idx);
        int rowOpCntr = rowIo.getMvccOperationCounter(pageAddr, idx);

        if (!isFlagsSet(LAST_FOUND)) {
            if (!(resCrd == rowCrd && resCntr == rowCntr)) { // It's possible it is a chain of aborted changes
                byte txState = cctx.kernalContext().coordinators().state(rowCrd, rowCntr);

                if (txState == TxState.COMMITTED) {
                    if (oldRow.link() != rowLink)
                        oldRow = tree.getRow(io, pageAddr, idx, RowData.LINK_WITH_HEADER);

                    boolean removed = false;

                    if (oldRow.newMvccCoordinatorVersion() != 0) {
                        if (oldRow.newMvccCoordinatorVersion() == rowCrd && oldRow.newMvccCounter() == rowCntr)
                            // Row was deleted by the same Tx it was created
                            txState = TxState.COMMITTED;
                        else if (oldRow.newMvccCoordinatorVersion() == resCrd && oldRow.newMvccCounter() == resCntr)
                            // The row is linked to the previously checked aborted version;
                            txState = TxState.ABORTED;
                        else
                            // Check with TxLog if removed version is committed;
                            txState = cctx.shared().coordinators().state(oldRow.newMvccCoordinatorVersion(), oldRow.newMvccCounter());

                        if (!(txState == TxState.COMMITTED || txState == TxState.ABORTED))
                            throw new IllegalStateException("Unexpected state: " + txState);

                        removed = txState == TxState.COMMITTED;
                    }

                    res = removed ? ResultType.PREV_NULL : ResultType.PREV_NOT_NULL;

                    setFlags(LAST_FOUND);

                    if (isFlagsSet(CHECK_VERSION)) {
                        long crdVer = removed ? oldRow.newMvccCoordinatorVersion() : oldRow.mvccCoordinatorVersion();
                        long cntr = removed ? oldRow.newMvccCounter() : oldRow.mvccCounter();
                        int opCntr = removed ? oldRow.newMvccOperationCounter() : oldRow.mvccOperationCounter();

                        if (!isVisible(cctx, mvccSnapshot, crdVer, cntr, opCntr, false)) {
                            resCrd = crdVer;
                            resCntr = cntr;

                            res = ResultType.VERSION_MISMATCH; // Write conflict.

                            return setFlags(STOP);
                        }

                        // no need to check further
                        unsetFlags(CHECK_VERSION);
                    }

                    if (isFlagsSet(PRIMARY | REMOVE) && cleanupRows != null) {
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
                    throw new IllegalStateException("Unexpected state: " + txState + ", key=" + key +
                    ", rowMvcc=" + rowCntr + ", txMvcc=" + mvccSnapshot.counter() + ":" + mvccSnapshot.operationCounter());
            }
        }

        if (!isFlagsSet(CAN_CLEANUP) && isFlagsSet(LAST_FOUND)
            && oldRow.link() == rowLink && res == ResultType.PREV_NULL) {
            // We can cleanup previous row only if it was deleted by another
            // transaction and delete version is less or equal to cleanup one
            long rowNewCrd = oldRow.newMvccCoordinatorVersion();
            long cleanupVer = mvccSnapshot.cleanupVersion();
            long rowNewCntr = oldRow.newMvccCounter();

            if (rowNewCrd < mvccCrd || Long.compare(cleanupVer, rowNewCntr) >= 0)
                setFlags(CAN_CLEANUP);
        }

        if (isFlagsSet(CAN_CLEANUP) || !isFlagsSet(LAST_FOUND)) { // can cleanup aborted versions
            if (cleanupRows == null)
                cleanupRows = new ArrayList<>();

            cleanupRows.add(new MvccLinkAwareSearchRow(cacheId, key, rowCrd, rowCntr, rowOpCntr, rowLink));
        }
        else if (!isFlagsSet(CAN_CLEANUP) && isFlagsSet(LAST_FOUND)
            && (rowCrd < mvccCrd || Long.compare(mvccSnapshot.cleanupVersion(), rowCntr) >= 0))
                // all further versions are guaranteed to be less than cleanup version
                setFlags(CAN_CLEANUP);

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

                assert resCrd != 0 && resCntr != 0;

                return new MvccVersionImpl(resCrd, resCntr, MvccProcessor.MVCC_OP_COUNTER_NA);
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
