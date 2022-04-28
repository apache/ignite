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

package org.apache.ignite.internal.processors.cache.tree;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree.Result.FOUND;
import static org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree.Result.NOT_FOUND;
import static org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree.Result.RETRY;

/**
 *
 */
public class PendingEntriesTree extends BPlusTree<PendingRow, PendingRow> {
    /** */
    public static final Object WITHOUT_KEY = new Object();

    /** */
    private final @Nullable CacheGroupContext grp;

    /** */
    private final boolean sharedGrp;

    /** */
    private final PageHandler<Remove, Result> rmvFromLeaf;

    /**
     * @param grp Cache group.
     * @param name Tree name.
     * @param pageMem Page memory.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param initNew Initialize new index.
     * @param pageLockTrackerManager Page lock tracker manager.
     * @param pageFlag Default flag value for allocated pages.
     * @throws IgniteCheckedException If failed.
     */
    public PendingEntriesTree(
        CacheGroupContext grp,
        String name,
        PageMemory pageMem,
        long metaPageId,
        ReuseList reuseList,
        boolean initNew,
        PageLockTrackerManager pageLockTrackerManager,
        byte pageFlag
    ) throws IgniteCheckedException {
        this(name,
            grp.groupId(),
            grp.name(),
            pageMem,
            grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null,
            grp.offheap().globalRemoveId(),
            metaPageId,
            reuseList,
            grp.sharedGroup() ? CacheIdAwarePendingEntryInnerIO.VERSIONS : PendingEntryInnerIO.VERSIONS,
            grp.sharedGroup() ? CacheIdAwarePendingEntryLeafIO.VERSIONS : PendingEntryLeafIO.VERSIONS,
            pageFlag,
            grp.shared().kernalContext().failure(),
            pageLockTrackerManager,
            grp,
            initNew);
    }

    /** */
    public PendingEntriesTree(
        String name,
        int cacheGrpId,
        String cacheGrpName,
        PageMemory pageMem,
        @Nullable IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        long metaPageId,
        @Nullable ReuseList reuseList,
        IOVersions<? extends BPlusInnerIO<PendingRow>> innerIos,
        IOVersions<? extends BPlusLeafIO<PendingRow>> leafIos,
        byte pageFlag,
        @Nullable FailureProcessor failureProcessor,
        PageLockTrackerManager lockTrackerManager,
        @Nullable CacheGroupContext grp,
        boolean initNew
    ) throws IgniteCheckedException {
        super(
            name,
            cacheGrpId,
            cacheGrpName,
            pageMem,
            wal,
            globalRmvId,
            metaPageId,
            reuseList,
            innerIos,
            leafIos,
            pageFlag,
            failureProcessor,
            lockTrackerManager
        );

        this.grp = grp;

        sharedGrp = grp != null && grp.sharedGroup();

        assert grp == null ||
            !grp.dataRegion().config().isPersistenceEnabled() || grp.shared().database().checkpointLockIsHeldByThread();

        initTree(initNew);

        rmvFromLeaf = (PageHandler<Remove, Result>)wrap(this, new RemoveFromLeafX());
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<PendingRow> iox, long pageAddr, int idx, PendingRow row) {
        PendingRowIO io = (PendingRowIO)iox;

        int cmp;

        if (sharedGrp) {
            assert row.cacheId != CU.UNDEFINED_CACHE_ID : "Cache ID is not provided!";
            assert io.getCacheId(pageAddr, idx) != CU.UNDEFINED_CACHE_ID : "Cache ID is not stored!";

            cmp = Integer.compare(io.getCacheId(pageAddr, idx), row.cacheId);

            if (cmp != 0)
                return cmp;

//            if (row.expireTime == 0 && row.link == 0) {
//                // A search row with a cache ID only is used as a cache bound.
//                // The found position will be shifted until the exact cache bound is found;
//                // See for details:
//                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findLowerBound()
//                // o.a.i.i.p.c.database.tree.BPlusTree.ForwardCursor.findUpperBound()
//                return cmp;
//            }
        }

        long expireTime = io.getExpireTime(pageAddr, idx);

        cmp = Long.compare(expireTime, row.expireTime);

        if (cmp != 0)
            return cmp;

        if (row.link == 0L)
            return 0;

        long link = io.getLink(pageAddr, idx);

        return Long.compare(link, row.link);
    }

    /** {@inheritDoc} */
    @Override public PendingRow getRow(BPlusIO<PendingRow> io, long pageAddr, int idx, Object flag)
        throws IgniteCheckedException {
        PendingRow row = io.getLookupRow(this, pageAddr, idx);

        return flag == WITHOUT_KEY ? row : row.initKey(grp);
    }

    /**
     * @param lower Lower bound.
     * @param upper Upper bound.
     * @param limit Limit of processed entries by single call, {@code 0} for no limit.
     * @return Removed rows.
     * @throws IgniteCheckedException If failed.
     */
    public List<PendingRow> removeRange(PendingRow lower, PendingRow upper, int limit) throws IgniteCheckedException {
        List<PendingRow> rmvTotal = new ArrayList<>();

        PendingRow lowerRow = lower;

        do {
            RemoveRange rmvOp = new RemoveRange(lowerRow, upper, true, limit - rmvTotal.size());

            doRemove(rmvOp);

            List<PendingRow> rmvd = rmvOp.removed;

            rmvTotal.addAll(rmvd);

            if (rmvOp.finished)
                break;

            // todo may mean that we did not find the row on the page after it was locked.
            if (rmvd.isEmpty())
                break;

            lowerRow = rmvd.get(rmvd.size() - 1);
        }
        while (!Thread.currentThread().isInterrupted() && (limit == 0 || rmvTotal.size() < limit));

        return rmvTotal;
    }

    /** */
    protected class RemoveRange extends Remove {
        /** */
        private final List<PendingRow> removed = new ArrayList<>();

        /** */
        private final int limit;

        /** */
        private PendingRow upper;

        /** */
        private boolean finished;

        /** */
        protected void markFinished() {
            finished = true;
        }

        /** */
        protected boolean finished() {
            return finished;
        }

        /** {@inheritDoc} */
        @Override protected boolean range() {
            return true;
        }

        /**
         * @param row Found lower bound.
         */
        public void row(PendingRow row) {
            this.row = row;
        }

        /**
         * @param row     Row.
         * @param needOld {@code True} If need return old value.
         */
        protected RemoveRange(PendingRow row, PendingRow upper, boolean needOld, int limit) {
            super(row, needOld);

            this.upper = upper;
            this.limit = limit;
        }

        /** {@inheritDoc} */
        @Override protected Result doRemoveFromLeaf() throws IgniteCheckedException {
            assert page() != 0L;

            return write(pageId, page(), rmvFromLeaf, this, 0, RETRY, statisticsHolder());
        }

        /** @return Forward page ID. */
        protected long forwardId() {
            return fwdId;
        }

        /** @return Backward page ID. */
        protected long backId() {
            return backId;
        }

        /** @return Tail. */
        public Tail<PendingRow> tail() {
            return tail;
        }

        /** {@inheritDoc} */
        @Override protected Result lockForward(int lvl) throws IgniteCheckedException {
            return super.lockForward(lvl);
        }

        /**
         * @param t Tail.
         * @throws IgniteCheckedException If failed.
         */
        @Override protected void removeDataRowFromLeafTail(Tail<PendingRow> t) throws IgniteCheckedException {
            assert !isRemoved();

            Tail<PendingRow> leaf = getTail(t, 0);

            assert leaf.idx >= 0 : leaf.idx;

            //todo
            int highIdx = findInsertionPoint(leaf.lvl, leaf.io, leaf.buf, leaf.idx, leaf.getCount(), row(), 0);

            for (int i = highIdx; i >= leaf.idx; i--)
                removeDataRowFromLeaf(leaf.pageId, leaf.page, leaf.buf, leaf.walPlc, leaf.io, leaf.getCount(), i);
        }

        /** {@inheritDoc} */
        @Override protected Result tryRemoveFromLeaf(long pageId, long page, long backId, long fwdId, int lvl)
            throws IgniteCheckedException {
            // We must be at the bottom here, just need to remove row from the current page.
            assert lvl == 0 : lvl;

            Result res = removeFromLeaf(pageId, page, backId, fwdId);

            if (res == NOT_FOUND || (res == FOUND && tail() == null)) // Finish if we don't need to do any merges.
                finish();

            return res;
        }

        /**
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param walPlc Full page WAL record policy.
         * @param io IO.
         * @param cnt Count.
         * @param idx Index to remove.
         * @throws IgniteCheckedException If failed.
         */
        @Override protected void removeDataRowFromLeaf(long pageId, long page, long pageAddr, Boolean walPlc,
            BPlusIO<PendingRow> io, int cnt, int idx) throws IgniteCheckedException {
            assert idx >= 0 && idx < cnt : idx;
            assert io.isLeaf() : "inner";
//            assert !isRemoved() : "already removed";

            assert needOld;

            // Detach the row.
            rmvd = getRow(io, pageAddr, idx);

            removed.add(rmvd);

            doRemove(pageId, page, pageAddr, walPlc, io, cnt, idx);

            assert isRemoved();
        }
    }

    /**
     *
     */
    private class RemoveFromLeafX extends GetPageHandler<RemoveRange> {
        /** {@inheritDoc} */
        @Override public Result run0(long leafId, long leafPage, long leafAddr, BPlusIO<PendingRow> io, RemoveRange r, int lvl)
            throws IgniteCheckedException {
            assert lvl == 0 : lvl; // Leaf.

            // Check the triangle invariant.
            if (io.getForward(leafAddr) != r.forwardId())
                return RETRY;

            final int cnt = io.getCount(leafAddr);

            assert cnt <= Short.MAX_VALUE : cnt;

            int lowIdx = findInsertionPoint(lvl, io, leafAddr, 0, cnt, r.row(), 0);

            if (lowIdx < 0) {
                lowIdx = fix(lowIdx);

                // todo may mean that we did not find the row on the page after it was locked.
                if (lowIdx == cnt || compare(io, leafAddr, lowIdx, r.upper) > 0)
                    return NOT_FOUND; // We've found exact match on search but now it's gone.
            }

            assert lowIdx >= 0 && lowIdx < cnt : lowIdx;

            int highIdx = findInsertionPoint(lvl, io, leafAddr, lowIdx, cnt, r.upper, 0);

            if (highIdx < 0)
                highIdx = cnt - 1;

            if (r.limit > 0 && (r.limit + lowIdx) <= highIdx)
                highIdx = lowIdx + r.limit - 1;

            if (highIdx != cnt - 1)
                r.markFinished();

            assert highIdx >= lowIdx : "low=" + lowIdx + ", high=" + highIdx;

            r.upper = getRow(io, leafAddr, highIdx);

            // Need to do inner replace when we remove the rightmost element and the leaf have no forward page,
            // i.e. it is not the rightmost leaf of the tree.
            boolean needReplaceInner = canGetRowFromInner && highIdx == cnt - 1 && io.getForward(leafAddr) != 0;

            // !!! Before modifying state we have to make sure that we will not go for retry.

            // We may need to replace inner key or want to merge this leaf with sibling after the remove -> keep lock.
            if (needReplaceInner ||
                // We need to make sure that we have back or forward to be able to merge.
                ((r.forwardId() != 0 || r.backId() != 0) && mayMerge(cnt - 1, io.getMaxCount(leafAddr, pageSize())))) {
                // If we have backId then we've already locked back page, nothing to do here.
                if (r.forwardId() != 0 && r.backId() == 0) {
                    Result res = r.lockForward(0);

                    if (res != FOUND) {
                        assert r.tail() == null;

                        return res; // Retry.
                    }

                    assert r.tail() != null; // We've just locked forward page.
                }

//                // Retry must reset these fields when we release the whole branch without remove.
//                assert r.needReplaceInner == FALSE : "needReplaceInner";
//                assert r.needMergeEmptyBranch == FALSE : "needMergeEmptyBranch";

                if (cnt == 1 || (highIdx - lowIdx + 1) == cnt) { // It was the last element on the leaf.
                    r.markNeedMergeEmptyBranch();

                    r.row(r.upper);
                }

                if (needReplaceInner) {
                    r.markNeedReplaceInner();

                    r.row(r.upper);
                }

                Tail<PendingRow> t = r.addTail(leafId, leafPage, leafAddr, io, 0, Tail.EXACT);

                t.idx = (short)lowIdx;

                // We will do the actual remove only when we made sure that
                // we've locked the whole needed branch correctly.

                return FOUND;
            }

            for (int idx = highIdx; idx >= lowIdx; idx--)
                r.removeDataRowFromLeaf(leafId, leafPage, leafAddr, null, io, io.getCount(leafAddr), idx);

            return FOUND;
        }
    }
}
