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

package org.apache.ignite.internal.processors.cache.database.tree;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixCountRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.FixLeftmostChildRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InnerReplaceRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InsertRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MergeRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageAddRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageCutRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRootRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.NewRootInitRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RecycleRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.RemoveRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ReplaceRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.SplitExistingPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.SplitForwardPageRecord;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseBag;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.GridArrays;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridTreePrinter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Bool.DONE;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Bool.FALSE;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Bool.READY;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Bool.TRUE;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.FOUND;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.GO_DOWN;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.GO_DOWN_X;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.NOT_FOUND;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.RETRY;
import static org.apache.ignite.internal.processors.cache.database.tree.BPlusTree.Result.RETRY_ROOT;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.isWalDeltaRecordNeeded;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.readPage;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 * Abstract B+Tree.
 */
@SuppressWarnings({"RedundantThrowsDeclaration", "ConstantValueVariableUse"})
public abstract class BPlusTree<L, T extends L> {
    /** */
    public static Random rnd;

    /** */
    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    /** */
    private final String name;

    /** */
    private final int cacheId;

    /** */
    private final IgniteWriteAheadLogManager wal;

    /** */
    protected final PageMemory pageMem;

    /** */
    private final ReuseList reuseList;

    /** */
    private final float minFill;

    /** */
    private final float maxFill;

    /** */
    private final long metaPageId;

    /** */
    private final IOVersions<? extends BPlusInnerIO<L>> innerIos;

    /** */
    private final IOVersions<? extends BPlusLeafIO<L>> leafIos;

    /** */
    private final AtomicLong globalRmvId = new AtomicLong(U.currentTimeMillis() * 1000_000); // TODO init from WAL?

    /** */
    private final GridTreePrinter<Long> treePrinter = new GridTreePrinter<Long>() {
        /** */
        private boolean keys = true;

        @Override protected List<Long> getChildren(Long pageId) {
            if (pageId == null || pageId == 0L)
                return null;

            try (Page page = page(pageId)) {
                ByteBuffer buf = page.getForRead(); // No correctness guaranties.

                try {
                    BPlusIO io = io(buf);

                    if (io.isLeaf())
                        return null;

                    int cnt = io.getCount(buf);

                    assert cnt >= 0 : cnt;

                    List<Long> res;

                    if (cnt > 0) {
                        res = new ArrayList<>(cnt + 1);

                        for (int i = 0; i < cnt; i++)
                            res.add(inner(io).getLeft(buf, i));

                        res.add(inner(io).getRight(buf, cnt - 1));
                    }
                    else {
                        long left = inner(io).getLeft(buf, 0);

                        res = left == 0 ? Collections.<Long>emptyList() : Collections.singletonList(left);
                    }

                    return res;
                }
                finally {
                    page.releaseRead();
                }
            }
            catch (IgniteCheckedException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override protected String formatTreeNode(Long pageId) {
            if (pageId == null)
                return ">NPE<";

            if (pageId == 0L)
                return "<Zero>";

            try (Page page = page(pageId)) {
                ByteBuffer buf = page.getForRead(); // No correctness guaranties.

                try {
                    BPlusIO<L> io = io(buf);

                    return printPage(io, buf, keys);
                }
                finally {
                    page.releaseRead();
                }
            }
            catch (IgniteCheckedException e) {
                throw new IllegalStateException(e);
            }
        }
    };

    /** */
    private final GetPageHandler<Get> askNeighbor = new GetPageHandler<Get>() {
        @Override public Result run0(long pageId, Page page, ByteBuffer buf, BPlusIO<L> io, Get g, int isBack) {
            assert !io.isLeaf(); // Inner page.

            if (isBack == TRUE.ordinal()) {
                // Count can be 0 here if it is a routing page, in this case we have a single child.
                int idx = io.getCount(buf);

                // We need to do get the rightmost child: io.getRight(cnt - 1),
                // here io.getLeft(cnt) is the same, but handles negative index if count is 0.
                long res = inner(io).getLeft(buf, idx);

                assert res != 0 : "inner page with no route down: " + pageId;

                g.backId = res;
            }
            else {
                assert isBack == FALSE.ordinal() : isBack;

                // Leftmost child.
                long res = inner(io).getLeft(buf, 0);

                assert res != 0 : "inner page with no route down: " + pageId;

                g.fwdId = res;
            }

            return FOUND;
        }
    };

    /** */
    private final GetPageHandler<Get> search = new GetPageHandler<Get>() {
        @Override public Result run0(long pageId, Page page, ByteBuffer buf, BPlusIO<L> io, Get g, int lvl)
            throws IgniteCheckedException {
            boolean needBackIfRouting = g.backId != 0;

            g.backId = 0; // Usually we'll go left down and don't need it.

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, g.row, g.shift);

            boolean found = idx >= 0;

            if (found) { // Found exact match.
                assert g.getClass() != GetCursor.class;

                if (g.found(io, buf, idx, lvl))
                    return FOUND;

                assert !io.isLeaf();

                // Else we need to reach leaf page, go left down.
            }
            else {
                idx = -idx - 1;

                // If we are on the right edge, then check for expected forward page and retry of it does match.
                // It means that concurrent split happened. This invariant is referred as `triangle`.
                if (idx == cnt && io.getForward(buf) != g.fwdId)
                    return RETRY;

                if (g.notFound(io, buf, idx, lvl)) // No way down, stop here.
                    return NOT_FOUND;

                assert !io.isLeaf();
                assert lvl > 0 : lvl;
            }

            // If idx == cnt then we go right down, else left down.
            g.pageId = inner(io).getLeft(buf, idx);

            // If we see the tree in consistent state, then our right down page must be forward for our left down page.
            if (idx < cnt)
                g.fwdId = inner(io).getRight(buf, idx);
            else {
                assert idx == cnt;
                // Here child's forward is unknown to us (we either go right or it is an empty "routing" page),
                // need to ask our forward about the child's forward (it must be leftmost child of our forward page).
                // This is ok from the locking standpoint because we take all locks in the forward direction.
                long fwdId = io.getForward(buf);

                if (fwdId == 0)
                    g.fwdId = 0;
                else {
                    Result res = askNeighbor(fwdId, g, false);

                    if (res != FOUND)
                        return res; // Retry.
                }

                if (cnt != 0) // It is not a routing page and we are going to the right, can get backId here.
                    g.backId = inner(io).getLeft(buf, cnt - 1);
                else if (needBackIfRouting) {
                    // Can't get backId here because of possible deadlock and it is only needed for remove operation.
                    return GO_DOWN_X;
                }
            }

            return GO_DOWN;
        }
    };

    /** */
    private final GetPageHandler<Put> replace = new GetPageHandler<Put>() {
        @Override public Result run0(long pageId, Page page, ByteBuffer buf, BPlusIO<L> io, Put p, int lvl)
            throws IgniteCheckedException {
            assert p.btmLvl == 0 : "split is impossible with replace";

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, p.row, 0);

            if (idx < 0) // Not found, split or merge happened.
                return RETRY;

            // Replace link at idx with new one.
            // Need to read link here because `p.finish()` will clear row.
            L newRow = p.row;

            if (io.isLeaf()) { // Get old row in leaf page to reduce contention at upper level.
                assert p.oldRow == null;
                assert io.canGetRow();

                p.oldRow = getRow(io, buf, idx);

                p.finish();
                // We can't erase data page here because it can be referred from other indexes.
            }

            io.store(buf, idx, newRow, null);

            if (needWalDeltaRecord(page))
                wal.log(new ReplaceRecord<>(cacheId, page.id(), io, newRow, null, idx));

            return FOUND;
        }
    };

    /** */
    private final GetPageHandler<Put> insert = new GetPageHandler<Put>() {
        @Override public Result run0(long pageId, Page page, ByteBuffer buf, BPlusIO<L> io, Put p, int lvl)
            throws IgniteCheckedException {
            assert p.btmLvl == lvl : "we must always insert at the bottom level: " + p.btmLvl + " " + lvl;

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, p.row, 0);

            assert idx < 0 : "Duplicate row in index.";

            idx = -idx - 1;

            // Possible split or merge.
            if (idx == cnt && io.getForward(buf) != p.fwdId)
                return RETRY;

            // Do insert.
            L moveUpRow = p.insert(page, io, buf, idx, lvl);

            // Check if split happened.
            if (moveUpRow != null) {
                p.btmLvl++; // Get high.
                p.row = moveUpRow;

                // Here forward page can't be concurrently removed because we keep write lock on tail which is the only
                // page who knows about the forward page, because it was just produced by split.
                p.rightId = io.getForward(buf);
                p.tail(page);

                assert p.rightId != 0;
            }
            else
                p.finish();

            return FOUND;
        }
    };

    /** */
    private final GetPageHandler<Remove> removeFromLeaf = new GetPageHandler<Remove>() {
        @Override public Result run0(long leafId, Page leaf, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            assert lvl == 0 : lvl;
            assert r.removed == null;
            assert io.isLeaf();
            assert io.canGetRow();

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, r.row, 0);

            if (idx < 0) {
                if (!r.ceil) // We've found exact match on search but now it's gone.
                    return RETRY;

                idx = -idx - 1;

                if (idx == cnt) // We can not remove ceiling row here.
                    return NOT_FOUND;

                assert idx < cnt;
            }

            // Before modifying state we have to make sure that we will not go for retry.

            // We may need to replace inner key or want to merge this leaf with sibling after the remove -> keep lock.
            if (r.needReplaceInner == TRUE ||
                // We need to make sure that we have back or forward to be able to merge.
                ((r.fwdId != 0 || r.backId != 0) && mayMerge(cnt - 1, io.getMaxCount(buf)))) {
                // If we have backId then we've already locked back page, nothing to do here.
                if (r.fwdId != 0 && r.backId == 0) {
                    Result res = r.lockForward(0);

                    if (res != FOUND)
                        return res; // Retry.
                }

                if (cnt == 1) // It was the last item.
                    r.needMergeEmptyBranch = TRUE;

                r.addTail(leafId, leaf, buf, io, 0, Tail.EXACT, -1);
            }

            r.removed = getRow(io, buf, idx);

            long rmvId = 0;

            if (r.needReplaceInner == TRUE) {
                // We increment remove ID in write lock on leaf page, thus it is guaranteed that
                // any successor will get greater value than he had read at the beginning of the operation.
                // Thus he is guaranteed to do a retry from root. Since inner replace takes locks on the whole branch
                // and releases the locks only when the inner key is updated and the successor saw the updated removeId,
                // then after retry from root, he will see updated inner key.
                rmvId = globalRmvId.incrementAndGet();
            }

            r.doRemove(leaf, io, buf, cnt, idx, rmvId);

            return FOUND;
        }
    };

    /** */
    private final GetPageHandler<Remove> lockBackAndRemoveFromLeaf = new GetPageHandler<Remove>() {
        @Override protected Result run0(long backId, Page back, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            // Check that we have consistent view of the world.
            if (io.getForward(buf) != r.pageId)
                return RETRY;

            // Correct locking order: from back to forward.
            Result res = r.doRemoveFromLeaf();

            // Keep locks on back and leaf pages for subsequent merges.
            if (res == FOUND && r.tail != null)
                r.addTail(backId, back, buf, io, lvl, Tail.BACK, -1);

            return res;
        }
    };

    /** */
    private final GetPageHandler<Remove> lockBackAndTail = new GetPageHandler<Remove>() {
        @Override public Result run0(long backId, Page back, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            // Check that we have consistent view of the world.
            if (io.getForward(buf) != r.pageId)
                return RETRY;

            // Correct locking order: from back to forward.
            Result res = r.doLockTail(lvl);

            if (res == FOUND)
                r.addTail(backId, back, buf, io, lvl, Tail.BACK, -1);

            return res;
        }
    };

    /** */
    private final GetPageHandler<Remove> lockTailForward = new GetPageHandler<Remove>() {
        @Override protected Result run0(long pageId, Page page, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            r.addTail(pageId, page, buf, io, lvl, Tail.FORWARD, -1);

            return FOUND;
        }
    };

    /** */
    private final GetPageHandler<Remove> lockTail = new GetPageHandler<Remove>() {
        @Override public Result run0(long pageId, Page page, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            assert lvl > 0 : lvl; // We are not at the bottom.

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, r.row, 0);

            boolean found = idx >= 0;

            if (!found) {
                idx = -idx - 1;

                // Check that we have a correct view of the world.
                if (idx == cnt && io.getForward(buf) != r.fwdId)
                    return RETRY;
            }

            // Check that we have a correct view of the world.
            if (lvl != 0 && inner(io).getLeft(buf, idx) != r.getTail(lvl - 1).pageId) {
                assert !found;

                return RETRY;
            }

            // We don't have a back page, need to lock our forward and become a back for it.
            if (r.fwdId != 0 && r.backId == 0) {
                Result res = r.lockForward(lvl);

                if (res != FOUND)
                    return res; // Retry.
            }

            // Update state only when we are sure that we will not go for retry.
            if (found && io.canGetRow()) {
                // We can not miss the inner value on down move because of `triangle` invariant, thus it must be TRUE.
                assert r.needReplaceInner == TRUE : r.needReplaceInner;
                assert idx <= Short.MAX_VALUE : idx;

                r.innerIdx = (short)idx;

                r.needReplaceInner = READY;
            }

            r.addTail(pageId, page, buf, io, lvl, Tail.EXACT, idx);

            return FOUND;
        }
    };

    /** */
    private final PageHandler<Void, Void> cutRoot = new PageHandler<Void, Void>() {
        @Override public Void run(long metaId, Page meta, ByteBuffer buf, Void ignore, int lvl)
            throws IgniteCheckedException {
            BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(buf);

            assert lvl == io.getRootLevel(buf); // Can drop only root.

            io.cutRoot(buf);

            if (needWalDeltaRecord(meta))
                wal.log(new MetaPageCutRootRecord(cacheId, meta.id()));

            return null;
        }
    };

    /** */
    private final PageHandler<Long, Void> addRoot = new PageHandler<Long, Void>() {
        @Override public Void run(long metaId, Page meta, ByteBuffer buf, Long rootPageId, int lvl)
            throws IgniteCheckedException {
            assert rootPageId != null;

            BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(buf);

            assert lvl == io.getLevelsCount(buf);

            io.addRoot(buf, rootPageId);

            if (needWalDeltaRecord(meta))
                wal.log(new MetaPageAddRootRecord(cacheId, meta.id(), rootPageId));

            return null;
        }
    };

    /** */
    private final PageHandler<Long, Void> initRoot = new PageHandler<Long, Void>() {
        @Override public Void run(long metaId, Page meta, ByteBuffer buf, Long rootId, int lvl)
            throws IgniteCheckedException {
            assert rootId != null;

            BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(buf);

            io.initRoot(buf, rootId);

            if (needWalDeltaRecord(meta))
                wal.log(new MetaPageInitRootRecord(cacheId, meta.id(), rootId));

            return null;
        }
    };

    /**
     * @param name Tree name.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param innerIos Inner IO versions.
     * @param leafIos Leaf IO versions.
     */
    public BPlusTree(
        String name,
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        ReuseList reuseList,
        IOVersions<? extends BPlusInnerIO<L>> innerIos,
        IOVersions<? extends BPlusLeafIO<L>> leafIos
    ) throws IgniteCheckedException {
        assert name != null;

        this.name = name;

        // TODO make configurable: 0 <= minFill <= maxFill <= 1
        minFill = 0f; // Testing worst case when merge happens only on empty page.
        maxFill = 0f; // Avoiding random effects on testing.

        assert pageMem != null;
        assert innerIos != null;
        assert leafIos != null;

        this.innerIos = innerIos;
        this.leafIos = leafIos;
        this.pageMem = pageMem;
        this.cacheId = cacheId;
        this.metaPageId = metaPageId;
        this.reuseList = reuseList;
        this.wal = wal;
    }

    /**
     * @return Cache ID.
     */
    public final int getCacheId() {
        return cacheId;
    }

    /**
     * @return Tree name.
     */
    public final String getName() {
        return name;
    }

    /**
     * @param page Updated page.
     * @return {@code true} If we need to make a delta WAL record for the change in this page.
     */
    private boolean needWalDeltaRecord(Page page) {
        return isWalDeltaRecordNeeded(wal, page);
    }

    /**
     * Initialize new index.
     *
     * @throws IgniteCheckedException If failed.
     */
    protected final void initNew() throws IgniteCheckedException {
        // Allocate the first leaf page, it will be our root.
        long rootId = allocatePageForNew();

        try (Page root = page(rootId)) {
            writePage(rootId, root, PageHandler.NOOP, latestLeafIO(), wal, null, 0);
        }

        // Initialize meta page with new root page.
        try (Page meta = page(metaPageId)) {
            writePage(metaPageId, meta, initRoot, BPlusMetaIO.VERSIONS.latest(), wal, rootId, 0);
        }
    }

    /**
     * @return Root level.
     */
    private static int getRootLevel(Page meta) {
        ByteBuffer buf = meta.getForRead(); // Meta can't be removed.

        try {
            return BPlusMetaIO.VERSIONS.forPage(buf).getRootLevel(buf);
        }
        finally {
            meta.releaseRead();
        }
    }

    /**
     * @param meta Meta page.
     * @param lvl Level, if {@code 0} then it is a bottom level, if negative then root.
     * @return Page ID.
     */
    private static long getFirstPageId(Page meta, int lvl) {
        ByteBuffer buf = meta.getForRead(); // Meta can't be removed.

        try {
            BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(buf);

            if (lvl < 0)
                lvl = io.getRootLevel(buf);

            if (lvl >= io.getLevelsCount(buf))
                return 0;

            return io.getFirstPageId(buf, lvl);
        }
        finally {
            meta.releaseRead();
        }
    }

    /**
     * @param upper Upper bound.
     * @return Cursor.
     */
    private GridCursor<T> findLowerUnbounded(L upper) throws IgniteCheckedException {
        ForwardCursor cursor = new ForwardCursor(upper);

        long firstPageId;

        try (Page meta = page(metaPageId)) {
            firstPageId = getFirstPageId(meta, 0); // Level 0 is always at the bottom.
        }

        try (Page first = page(firstPageId)) {
            ByteBuffer buf = first.getForRead(); // We always merge pages backwards, the first page is never removed.

            try {
                cursor.fillFromBuffer(buf, io(buf), 0);
            }
            finally {
                first.releaseRead();
            }
        }

        return cursor;
    }

    /**
     * Check if the tree is getting destroyed.
     */
    private void checkDestroyed() {
        if (destroyed.get())
            throw new IllegalStateException("Tree is being concurrently destroyed: " + name);
    }

    /**
     * @param lower Lower bound inclusive or {@code null} if unbounded.
     * @param upper Upper bound inclusive or {@code null} if unbounded.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    public final GridCursor<T> find(L lower, L upper) throws IgniteCheckedException {
        checkDestroyed();

        try {
            if (lower == null)
                return findLowerUnbounded(upper);

            // Lower bound must be shifted to -1 for case when multiple rows are equal to this bound.
            GetCursor g = new GetCursor(lower, -1, new ForwardCursor(upper));

            doFind(g);

            return g.cursor;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on bounds: [lower=" + lower + ", upper=" + upper + "]", e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on bounds: [lower=" + lower + ", upper=" + upper + "]", e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on bounds: [lower=" + lower + ", upper=" + upper + "]", e);
        }
    }

    /**
     * @param row Lookup row for exact match.
     * @return Found row.
     */
    @SuppressWarnings("unchecked")
    public final T findOne(L row) throws IgniteCheckedException {
        checkDestroyed();

        try {
            GetOne g = new GetOne(row);

            doFind(g);

            return (T)g.row;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on lookup row: " + row, e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on lookup row: " + row, e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on lookup row: " + row, e);
        }
    }

    /**
     * @param g Get.
     */
    private void doFind(Get g) throws IgniteCheckedException {
        try {
            for (; ; ) { // Go down with retries.
                g.init();

                switch (findDown(g, g.rootId, 0L, g.rootLvl)) {
                    case RETRY:
                    case RETRY_ROOT:
                        checkInterrupted();

                        continue;

                    default:
                        return;
                }
            }
        }
        finally {
            g.releaseMeta();
        }
    }

    /**
     * @param g Get.
     * @param pageId Page ID.
     * @param fwdId Expected forward page ID.
     * @param lvl Level.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private Result findDown(final Get g, final long pageId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        Page page = page(pageId);

        try {
            for (; ; ) {
                // Init args.
                g.pageId = pageId;
                g.fwdId = fwdId;

                Result res = readPage(pageId, page, search, g, lvl);

                switch (res) {
                    case GO_DOWN:
                    case GO_DOWN_X:
                        assert g.pageId != pageId;
                        assert g.fwdId != fwdId || fwdId == 0;

                        // Go down recursively.
                        res = findDown(g, g.pageId, g.fwdId, lvl - 1);

                        switch (res) {
                            case RETRY:
                                checkInterrupted();

                                continue; // The child page got splitted, need to reread our page.

                            default:
                                return res;
                        }

                    case NOT_FOUND:
                        assert lvl == 0 : lvl;

                        g.row = null; // Mark not found result.

                        return res;

                    default:
                        return res;
                }
            }
        }
        finally {
            if (g.canRelease(pageId, page, lvl))
                page.close();
        }
    }

    /**
     * @param instance Instance name.
     * @param type Tree type.
     * @return Tree name.
     */
    public static String treeName(String instance, String type) {
        return instance + "##" + type;
    }

    /**
     * For debug.
     *
     * @return Tree as {@link String}.
     */
    @SuppressWarnings("unused")
    public final String printTree() {
        long rootPageId;

        try (Page meta = page(metaPageId)) {
            rootPageId = getFirstPageId(meta, -1);
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException(e);
        }

        return treePrinter.print(rootPageId);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public final void validateTree() throws IgniteCheckedException {
        long rootPageId;
        int rootLvl;

        try (Page meta = page(metaPageId)) {
            rootLvl = getRootLevel(meta);

            if (rootLvl < 0)
                fail("Root level: " + rootLvl);

            validateFirstPages(meta, rootLvl);

            rootPageId = getFirstPageId(meta, rootLvl);

            validateDown(meta, rootPageId, 0L, rootLvl);
        }
    }

    /**
     * @param meta Meta page.
     * @param rootLvl Root level.
     * @throws IgniteCheckedException If failed.
     */
    private void validateFirstPages(Page meta, int rootLvl) throws IgniteCheckedException {
        for (int lvl = rootLvl; lvl > 0; lvl--)
            validateFirstPage(meta, lvl);
    }

    /**
     * @param msg Message.
     */
    private static void fail(Object msg) {
        throw new AssertionError(msg);
    }

    /**
     * @param meta Meta page.
     * @param lvl Level.
     * @throws IgniteCheckedException If failed.
     */
    private void validateFirstPage(Page meta, int lvl) throws IgniteCheckedException {
        if (lvl == 0)
            fail("Leaf level: " + lvl);

        long pageId = getFirstPageId(meta, lvl);

        long leftmostChildId;

        try (Page page = page(pageId)) {
            ByteBuffer buf = page.getForRead(); // No correctness guaranties.

            try {
                BPlusIO<L> io = io(buf);

                if (io.isLeaf())
                    fail("Leaf.");

                leftmostChildId = inner(io).getLeft(buf, 0);
            }
            finally {
                page.releaseRead();
            }
        }

        long firstDownPageId = getFirstPageId(meta, lvl - 1);

        if (firstDownPageId != leftmostChildId)
            fail(new SB("First: meta ").appendHex(firstDownPageId).a(", child ").appendHex(leftmostChildId));
    }

    /**
     * @param meta Meta page.
     * @param pageId Page ID.
     * @param fwdId Forward ID.
     * @param lvl Level.
     * @throws IgniteCheckedException If failed.
     */
    private void validateDown(Page meta, long pageId, long fwdId, final int lvl) throws IgniteCheckedException {
        try (Page page = page(pageId)) {
            ByteBuffer buf = page.getForRead(); // No correctness guaranties.

            try {
                long realPageId = BPlusIO.getPageId(buf);

                if (realPageId != pageId)
                    fail(new SB("ABA on page ID: ref ").appendHex(pageId).a(", buf ").appendHex(realPageId));

                BPlusIO<L> io = io(buf);

                if (io.isLeaf() != (lvl == 0)) // Leaf pages only at the level 0.
                    fail("Leaf level mismatch: " + lvl);

                long actualFwdId = io.getForward(buf);

                if (actualFwdId != fwdId)
                    fail(new SB("Triangle: expected fwd ").appendHex(fwdId).a(", actual fwd ").appendHex(actualFwdId));

                int cnt = io.getCount(buf);

                if (cnt < 0)
                    fail("Negative count: " + cnt);

                if (io.isLeaf()) {
                    if (cnt == 0 && getRootLevel(meta) != 0)
                        fail("Empty leaf page.");
                }
                else {
                    // Recursively go down if we are on inner level.
                    for (int i = 0; i < cnt; i++)
                        validateDown(meta, inner(io).getLeft(buf, i), inner(io).getRight(buf, i), lvl - 1);

                    if (fwdId != 0) {
                        // For the rightmost child ask neighbor.
                        try (Page fwd = page(fwdId)) {
                            ByteBuffer fwdBuf = fwd.getForRead(); // No correctness guaranties.

                            try {
                                if (io(fwdBuf) != io)
                                    fail("IO on the same level must be the same");

                                fwdId = inner(io).getLeft(fwdBuf, 0);
                            }
                            finally {
                                fwd.releaseRead();
                            }
                        }
                    }

                    pageId = inner(io).getLeft(buf, cnt); // The same as io.getRight(cnt - 1) but works for routing pages.

                    validateDown(meta, pageId, fwdId, lvl - 1);
                }
            }
            finally {
                page.releaseRead();
            }
        }
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param keys Keys.
     * @return String.
     * @throws IgniteCheckedException If failed.
     */
    private String printPage(BPlusIO<L> io, ByteBuffer buf, boolean keys) throws IgniteCheckedException {
        StringBuilder b = new StringBuilder();

        b.append(formatPageId(PageIO.getPageId(buf)));

        b.append(" [ ");
        b.append(io.isLeaf() ? "L " : "I ");

        int cnt = io.getCount(buf);
        long fwdId = io.getForward(buf);

        b.append("cnt=").append(cnt).append(' ');
        b.append("fwd=").append(formatPageId(fwdId)).append(' ');

        if (!io.isLeaf()) {
            b.append("lm=").append(formatPageId(inner(io).getLeft(buf, 0))).append(' ');

            if (cnt > 0)
                b.append("rm=").append(formatPageId(inner(io).getRight(buf, cnt - 1))).append(' ');
        }

        if (keys)
            b.append("keys=").append(printPageKeys(io, buf)).append(' ');

        b.append(']');

        return b.toString();
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @return Keys as String.
     * @throws IgniteCheckedException If failed.
     */
    private String printPageKeys(BPlusIO<L> io, ByteBuffer buf) throws IgniteCheckedException {
        int cnt = io.getCount(buf);

        StringBuilder b = new StringBuilder();

        b.append('[');

        for (int i = 0; i < cnt; i++) {
            if (i != 0)
                b.append(',');

            b.append(io.canGetRow() ? getRow(io, buf, i) : io.getLookupRow(this, buf, i));
        }

        b.append(']');

        return b.toString();
    }

    /**
     * @param x Long.
     * @return String.
     */
    private static String formatPageId(long x) {
        return new SB().appendHex(x).toString();
    }

    /**
     * Check if interrupted.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private static void checkInterrupted() throws IgniteInterruptedCheckedException {
        if (Thread.currentThread().isInterrupted())
            throw new IgniteInterruptedCheckedException("Interrupted.");
    }

    /**
     * @param row Lookup row.
     * @param bag Reuse bag.
     * @return Removed row.
     * @throws IgniteCheckedException If failed.
     */
    public final T removeCeil(L row, ReuseBag bag) throws IgniteCheckedException {
        assert row != null;

        return doRemove(row, true, bag);
    }

    /**
     * @param row Lookup row.
     * @return Removed row.
     * @throws IgniteCheckedException If failed.
     */
    public final T remove(L row) throws IgniteCheckedException {
        assert row != null;

        return doRemove(row, false, null);
    }

    /**
     * @param row Lookup row.
     * @param ceil If we can remove ceil row when we can not find exact.
     * @param bag Reuse bag.
     * @return Removed row.
     * @throws IgniteCheckedException If failed.
     */
    private T doRemove(L row, boolean ceil, ReuseBag bag) throws IgniteCheckedException {
        checkDestroyed();

        Remove r = new Remove(row, ceil, bag);

        try {
            for (; ; ) {
                r.init();

                switch (removeDown(r, r.rootId, 0L, 0L, r.rootLvl)) {
                    case RETRY:
                    case RETRY_ROOT:
                        checkInterrupted();

                        continue;

                    default:
                        if (!r.isFinished())
                            r.finishTail();

                        assert r.isFinished();

                        return r.removed;
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on search row: " + row, e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on search row: " + row, e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on search row: " + row, e);
        }
        finally {
            r.releaseTail();
            r.releaseMeta();

            r.reuseFreePages();
        }
    }

    /**
     * @param r Remove operation.
     * @param pageId Page ID.
     * @param backId Expected backward page ID if we are going to the right.
     * @param fwdId Expected forward page ID.
     * @param lvl Level.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private Result removeDown(final Remove r, final long pageId, final long backId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        assert lvl >= 0 : lvl;

        if (r.isTail(pageId, lvl))
            return FOUND; // We've already locked this page, so return that we are ok.

        final Page page = page(pageId);

        try {
            for (; ; ) {
                // Init args.
                r.pageId = pageId;
                r.fwdId = fwdId;
                r.backId = backId;

                Result res = readPage(pageId, page, search, r, lvl);

                switch (res) {
                    case GO_DOWN_X:
                        assert backId != 0;

                        // We need to get backId here for our child page, it must be the last child of our back.
                        res = askNeighbor(backId, r, true);

                        if (res != FOUND)
                            return res; // Retry.

                        // Intentional fallthrough.
                    case GO_DOWN:
                        res = removeDown(r, r.pageId, r.backId, r.fwdId, lvl - 1);

                        switch (res) {
                            case RETRY:
                                checkInterrupted();

                                continue;

                            case RETRY_ROOT:
                                return res;
                        }

                        if (!r.isFinished() && !r.finishTail())
                            return r.lockTail(pageId, page, backId, fwdId, lvl);

                        return res;

                    case NOT_FOUND:
                        // We are at the bottom.
                        assert lvl == 0 : lvl;

                        if (!r.ceil) {
                            r.finish();

                            return res;
                        }

                        // Intentional fallthrough for ceiling remove.

                    case FOUND:
                        // We must be at the bottom here, just need to remove row from the current page.
                        assert lvl == 0 : lvl;
                        assert r.removed == null;

                        res = r.removeFromLeaf(pageId, page, backId, fwdId);

                        if (res == NOT_FOUND) {
                            assert r.ceil : "must be a retry if not a ceiling remove";

                            r.finish();
                        }
                        else if (res == FOUND && r.tail == null) {
                            // Finish if we don't need to do any merges.
                            r.finish();
                        }

                        return res;

                    default:
                        return res;
                }
            }
        }
        finally {
            r.page = null;

            if (r.canRelease(pageId, page, lvl))
                page.close();
        }
    }

    /**
     * @param cnt Count.
     * @param cap Capacity.
     * @return {@code true} If may merge.
     */
    private boolean mayMerge(int cnt, int cap) {
        int minCnt = (int)(minFill * cap);

        if (cnt <= minCnt) {
            assert cnt == 0; // TODO remove

            return true;
        }

        assert cnt > 0;

        int maxCnt = (int)(maxFill * cap);

        if (cnt > maxCnt)
            return false;

        assert false; // TODO remove

        // Randomization is for smoothing worst case scenarios. Probability of merge attempt
        // is proportional to free space in our page (discounted on fill factor).
        return randomInt(maxCnt - minCnt) >= cnt - minCnt;
    }

    /**
     * @param max Max.
     * @return Random value from {@code 0} (inclusive) to the given max value (exclusive).
     */
    public static int randomInt(int max) {
        Random rnd0 = rnd != null ? rnd : ThreadLocalRandom.current();

        return rnd0.nextInt(max);
    }

    /**
     * @return Root level.
     * @throws IgniteCheckedException If failed.
     */
    public final int rootLevel() throws IgniteCheckedException {
        checkDestroyed();

        try (Page meta = page(metaPageId)) {
            return getRootLevel(meta);
        }
    }

    /**
     * !!! For debug only! May produce wrong results on concurrent access.
     *
     * @return Size.
     * @throws IgniteCheckedException If failed.
     */
    public final long size() throws IgniteCheckedException {
        checkDestroyed();

        long pageId;

        try (Page meta = page(metaPageId)) {
            pageId = getFirstPageId(meta, 0); // Level 0 is always at the bottom.
        }

        BPlusIO<L> io = null;

        long cnt = 0;

        while (pageId != 0) {
            try (Page page = page(pageId)) {
                ByteBuffer buf = page.getForRead(); // No correctness guaranties.

                try {
                    if (io == null) {
                        io = io(buf);

                        assert io.isLeaf();
                    }

                    cnt += io.getCount(buf);

                    pageId = io.getForward(buf);
                }
                finally {
                    page.releaseRead();
                }
            }
        }

        return cnt;
    }

    /**
     * @param row Row.
     * @return Old row.
     * @throws IgniteCheckedException If failed.
     */
    public final T put(T row) throws IgniteCheckedException {
        return put(row, null);
    }

    /**
     * @param row Row.
     * @param bag Reuse bag.
     * @return Old row.
     * @throws IgniteCheckedException If failed.
     */
    public final T put(T row, ReuseBag bag) throws IgniteCheckedException {
        checkDestroyed();

        Put p = new Put(row, bag);

        try {
            for (; ; ) { // Go down with retries.
                p.init();

                Result result = putDown(p, p.rootId, 0L, p.rootLvl);

                switch (result) {
                    case RETRY:
                    case RETRY_ROOT:
                        checkInterrupted();

                        continue;

                    default:
                        assert p.isFinished() : result;

                        return p.oldRow;
                }
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Runtime failure on row: " + row, e);
        }
        catch (RuntimeException e) {
            throw new IgniteException("Runtime failure on row: " + row, e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion error on row: " + row, e);
        }
        finally {
            p.releaseMeta();
        }
    }

    /**
     * Destroys tree. This method is allowed to be invoked only when the tree is out of use (no concurrent operations
     * are trying to read or update the tree after destroy beginning).
     *
     * @return Number of pages recycled from this tree. If the tree was destroyed by someone else concurrently returns
     *     {@code 0}, otherwise it should return at least {@code 2} (for meta page and root page), unless this tree is
     *     used as metadata storage, or {@code -1} if we don't have a reuse list and did not do recycling at all.
     * @throws IgniteCheckedException If failed.
     */
    public long destroy() throws IgniteCheckedException {
        if (!markDestroyed())
            return 0;

        if (reuseList == null)
            return -1;

        DestroyBag bag = new DestroyBag();

        long pagesCnt = destroy(bag);

        reuseList.add(bag);

        assert bag.size() == 0 : bag.size();

        return pagesCnt;
    }

    /**
     * @param bag Destroy bag.
     * @return Number of recycled pages.
     * @throws IgniteCheckedException If failed.
     */
    protected final long destroy(DestroyBag bag) throws IgniteCheckedException {
        long pagesCnt = 0;

        try (Page meta = page(metaPageId)) {
            ByteBuffer metaBuf = meta.getForWrite(); // No checks, we must be out of use.

            try {
                for (long pageId : getFirstPageIds(metaBuf)) {

                    assert pageId != 0;

                    do {
                        try (Page page = page(pageId)) {
                            ByteBuffer buf = page.getForWrite(); // No checks, we must be out of use.

                            try {
                                BPlusIO<L> io = io(buf);

                                long fwdPageId = io.getForward(buf);

                                bag.addFreePage(recyclePage(pageId, page, buf));
                                pagesCnt++;

                                pageId = fwdPageId;
                            }
                            finally {
                                page.releaseWrite(true);
                            }
                        }

                        if (bag.size() == 128) {
                            reuseList.add(bag);

                            assert bag.isEmpty() : bag.size();
                        }
                    }
                    while (pageId != 0);
                }

                bag.addFreePage(recyclePage(metaPageId, meta, metaBuf));
                pagesCnt++;
            }
            finally {
                meta.releaseWrite(true);
            }
        }

        return pagesCnt;
    }

    /**
     * @return {@code True} if state was changed.
     */
    protected final boolean markDestroyed() {
        return destroyed.compareAndSet(false, true);
    }

    /**
     * @param metaBuf Meta page buffer.
     * @return First page IDs.
     */
    protected Iterable<Long> getFirstPageIds(ByteBuffer metaBuf) {
        List<Long> result = new ArrayList<>();

        BPlusMetaIO mio = BPlusMetaIO.VERSIONS.forPage(metaBuf);

        for (int lvl = mio.getRootLevel(metaBuf); lvl >= 0; lvl--) {
            result.add(mio.getFirstPageId(metaBuf, lvl));
        }

        return result;
    }

    /**
     * @param pageId Page ID.
     * @param page Page.
     * @param buf Buffer.
     * @return Recycled page ID.
     * @throws IgniteCheckedException If failed.
     */
    private long recyclePage(long pageId, Page page, ByteBuffer buf) throws IgniteCheckedException {
        // Rotate page ID to avoid concurrency issues with reused pages.
        pageId = PageIdUtils.rotatePageId(pageId);

        // Update page ID inside of the buffer, Page.id() will always return the original page ID.
        PageIO.setPageId(buf, pageId);

        if (needWalDeltaRecord(page))
            wal.log(new RecycleRecord(cacheId, page.id(), pageId));

        return pageId;
    }

    /**
     * @param io IO.
     * @param page Page to split.
     * @param buf Splitting buffer.
     * @param fwdId Forward page ID.
     * @param fwd Forward page.
     * @param fwdBuf Forward buffer.
     * @param idx Insertion index.
     * @return {@code true} The middle index was shifted to the right.
     * @throws IgniteCheckedException If failed.
     */
    private boolean splitPage(
        BPlusIO io,
        Page page,
        ByteBuffer buf,
        long fwdId,
        Page fwd,
        ByteBuffer fwdBuf,
        int idx
    ) throws IgniteCheckedException {
        int cnt = io.getCount(buf);
        int mid = cnt >>> 1;

        boolean res = false;

        if (idx > mid) { // If insertion is going to be to the forward page, keep more in the back page.
            mid++;

            res = true;
        }

        // Update forward page.
        io.splitForwardPage(buf, fwdId, fwdBuf, mid, cnt);

        // Here the order of records for pages is important because forward split requires
        // that existing page is still in initial state.
        if (needWalDeltaRecord(fwd))
            wal.log(new SplitForwardPageRecord(cacheId, fwd.id(), fwdId,
                io.getType(), io.getVersion(), page.id(), mid, cnt));

        // Update existing page.
        io.splitExistingPage(buf, mid, fwdId);

        if (needWalDeltaRecord(page))
            wal.log(new SplitExistingPageRecord(cacheId, page.id(), mid, fwdId));

        return res;
    }

    /**
     * @param page Page.
     */
    private static void writeUnlockAndClose(Page page) {
        assert page != null;

        try {
            page.releaseWrite(true);
        }
        finally {
            page.close();
        }
    }

    /**
     * @param pageId Inner page ID.
     * @param back Get back or forward page.
     * @return Operation result.
     */
    private Result askNeighbor(long pageId, Get g, boolean back) throws IgniteCheckedException {
        try (Page page = page(pageId)) {
            return readPage(pageId, page, askNeighbor, g,
                back ? TRUE.ordinal() : FALSE.ordinal());
        }
    }

    /**
     * @param p Put.
     * @param pageId Page ID.
     * @param fwdId Expected forward page ID.
     * @param lvl Level.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private Result putDown(final Put p, final long pageId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        assert lvl >= 0 : lvl;

        final Page page = page(pageId);

        try {
            for (; ; ) {
                // Init args.
                p.pageId = pageId;
                p.fwdId = fwdId;

                Result res = readPage(pageId, page, search, p, lvl);

                switch (res) {
                    case GO_DOWN:
                    case GO_DOWN_X:
                        assert lvl > 0 : lvl;
                        assert p.pageId != pageId;
                        assert p.fwdId != fwdId || fwdId == 0;

                        // Need to replace key in inner page. There is no race because we keep tail lock after split.
                        if (p.needReplaceInner == TRUE) {
                            p.needReplaceInner = FALSE; // Protect from retries.

                            res = writePage(pageId, page, replace, p, lvl);

                            if (res != FOUND)
                                return res; // Need to retry.

                            p.needReplaceInner = DONE; // We can have only single matching inner key.
                        }

                        // Go down recursively.
                        res = putDown(p, p.pageId, p.fwdId, lvl - 1);

                        if (res == RETRY_ROOT || p.isFinished())
                            return res;

                        if (res == RETRY)
                            checkInterrupted();

                        continue; // We have to insert split row to this level or it is a retry.

                    case FOUND: // Do replace.
                        assert lvl == 0 : "This replace can happen only at the bottom level.";

                        // Init args.
                        p.pageId = pageId;
                        p.fwdId = fwdId;

                        return writePage(pageId, page, replace, p, lvl);

                    case NOT_FOUND: // Do insert.
                        assert lvl == p.btmLvl : "must insert at the bottom level";
                        assert p.needReplaceInner == FALSE : p.needReplaceInner + " " + lvl;

                        // Init args.
                        p.pageId = pageId;
                        p.fwdId = fwdId;

                        return writePage(pageId, page, insert, p, lvl);

                    default:
                        return res;
                }
            }
        }
        finally {
            if (p.canRelease(pageId, page, lvl))
                page.close();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BPlusTree.class, this);
    }

    /**
     * Get operation.
     */
    private abstract class Get {
        /** */
        protected long rmvId;

        /** Starting point root level. May be outdated. Must be modified only in {@link Get#init()}. */
        protected int rootLvl;

        /** Starting point root ID. May be outdated. Must be modified only in {@link Get#init()}. */
        protected long rootId;

        /** Meta page. Initialized by {@link Get#init()}, released by {@link Get#releaseMeta()}. */
        protected Page meta;

        /** */
        protected L row;

        /** In/Out parameter: Page ID. */
        protected long pageId;

        /** In/Out parameter: expected forward page ID. */
        protected long fwdId;

        /** In/Out parameter: in case of right turn this field will contain backward page ID for the child. */
        protected long backId;

        /** */
        int shift;

        /**
         * @param row Row.
         */
        Get(L row) {
            assert row != null;

            this.row = row;
        }

        /**
         * Initialize operation.
         *
         * !!! Symmetrically with this method must be called {@link Get#releaseMeta()} in {@code finally} block.
         *
         * @throws IgniteCheckedException If failed.
         */
        final void init() throws IgniteCheckedException {
            if (meta == null)
                meta = page(metaPageId);

            int rootLvl;
            long rootId;

            ByteBuffer buf = meta.getForRead(); // Meta can't be removed.

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(buf);

                rootLvl = io.getRootLevel(buf);
                rootId = io.getFirstPageId(buf, rootLvl);
            }
            finally {
                meta.releaseRead();
            }

            restartFromRoot(rootId, rootLvl, globalRmvId.get());
        }

        /**
         * @param rootId Root page ID.
         * @param rootLvl Root level.
         * @param rmvId Remove ID to be afraid of.
         */
        final void restartFromRoot(long rootId, int rootLvl, long rmvId) {
            this.rootId = rootId;
            this.rootLvl = rootLvl;
            this.rmvId = rmvId;
        }

        /**
         * @param io IO.
         * @param buf Buffer.
         * @param idx Index of found entry.
         * @param lvl Level.
         * @return {@code true} If we need to stop.
         * @throws IgniteCheckedException If failed.
         */
        abstract boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException;

        /**
         * @param io IO.
         * @param buf Buffer.
         * @param idx Insertion point.
         * @param lvl Level.
         * @return {@code true} If we need to stop.
         * @throws IgniteCheckedException If failed.
         */
        boolean notFound(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            assert lvl >= 0;

            return lvl == 0; // We are not at the bottom.
        }

        /**
         * Release meta page.
         */
        final void releaseMeta() {
            if (meta != null) {
                meta.close();
                meta = null;
            }
        }

        /**
         * @param pageId Page ID.
         * @param page Page.
         * @param lvl Level.
         * @return {@code true} If we can release the given page.
         */
        boolean canRelease(long pageId, Page page, int lvl) {
            return page != null;
        }
    }

    /**
     * Get a single entry.
     */
    private final class GetOne extends Get {
        /**
         * @param row Row.
         */
        private GetOne(L row) {
            super(row);
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            if (!io.canGetRow()) {
                assert !io.isLeaf();

                return false;
            }

            row = getRow(io, buf, idx);

            return true;
        }
    }

    /**
     * Get a cursor for range.
     */
    private final class GetCursor extends Get {
        /** */
        private ForwardCursor cursor;

        /**
         * @param lower Lower bound.
         * @param shift Shift.
         * @param cursor Cursor.
         */
        GetCursor(L lower, int shift, ForwardCursor cursor) {
            super(lower);

            this.shift = shift;
            this.cursor = cursor;
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            if (!io.isLeaf())
                return false;

            cursor.fillFromBuffer(buf, io, idx);

            return true;
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            assert lvl >= 0 : lvl;

            if (lvl != 0)
                return false;

            assert io.isLeaf();

            cursor.fillFromBuffer(buf, io, idx);

            return true;
        }
    }

    /**
     * Put operation.
     */
    private final class Put extends Get {
        /** Right child page ID for split row. */
        private long rightId;

        /** Replaced row if any. */
        private T oldRow;

        /**
         * This page is kept locked after split until insert to the upper level will not be finished.
         * It is needed because split row will be "in flight" and if we'll release tail, remove on
         * split row may fail.
         */
        private Page tail;

        /**
         * Bottom level for insertion (insert can't go deeper). Will be incremented on split on each level.
         */
        private short btmLvl;

        /** */
        Bool needReplaceInner = FALSE;

        /** */
        private ReuseBag bag;

        /**
         * @param row Row.
         * @param bag Reuse bag.
         */
        private Put(T row, ReuseBag bag) {
            super(row);

            this.bag = bag;
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) {
            if (io.isLeaf())
                return true;

            // If we can get full row from the inner page, we must do inner replace to update full row info here.
            if (io.canGetRow() && needReplaceInner == FALSE)
                needReplaceInner = TRUE;

            return false;
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) {
            assert btmLvl >= 0 : btmLvl;
            assert lvl >= btmLvl : lvl;

            return lvl == btmLvl;
        }

        /**
         * @param tail Tail page.
         */
        private void tail(Page tail) {
            if (this.tail != null)
                writeUnlockAndClose(this.tail);

            this.tail = tail;
        }

        /** {@inheritDoc} */
        @Override boolean canRelease(long pageId, Page page, int lvl) {
            return page != null && tail != page;
        }

        /**
         * Finish put.
         */
        private void finish() {
            row = null;
            rightId = 0;
            tail(null);
        }

        /**
         * @return {@code true} If finished.
         */
        private boolean isFinished() {
            return row == null;
        }

        /**
         * @param page Page.
         * @param io IO.
         * @param buf Buffer.
         * @param idx Index.
         * @param lvl Level.
         * @return Move up row.
         * @throws IgniteCheckedException If failed.
         */
        private L insert(Page page, BPlusIO<L> io, ByteBuffer buf, int idx, int lvl)
            throws IgniteCheckedException {
            int maxCnt = io.getMaxCount(buf);
            int cnt = io.getCount(buf);

            if (cnt == maxCnt) // Need to split page.
                return insertWithSplit(page, io, buf, idx, lvl);

            insertSimple(page, io, buf, idx);

            return null;
        }

        /**
         * @param page Page.
         * @param io IO.
         * @param buf Buffer.
         * @param idx Index.
         * @throws IgniteCheckedException If failed.
         */
        private void insertSimple(Page page, BPlusIO<L> io, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            io.insert(buf, idx, row, null, rightId);

            if (needWalDeltaRecord(page))
                wal.log(new InsertRecord<>(cacheId, page.id(), io, idx, row, null, rightId));
        }

        /**
         * @param page Page.
         * @param io IO.
         * @param buf Buffer.
         * @param idx Index.
         * @param lvl Level.
         * @return Move up row.
         * @throws IgniteCheckedException If failed.
         */
        private L insertWithSplit(Page page, BPlusIO<L> io, final ByteBuffer buf, int idx, int lvl)
            throws IgniteCheckedException {
            long fwdId = allocatePage(bag);

            try (Page fwd = page(fwdId)) {
                // Need to check this before the actual split, because after the split we will have new forward page here.
                boolean hadFwd = io.getForward(buf) != 0;

                ByteBuffer fwdBuf = fwd.getForWrite(); // Initial write, no need to check for concurrent modification.

                try {
                    // Never write full forward page, because it is known to be new.
                    fwd.fullPageWalRecordPolicy(Boolean.FALSE);

                    boolean midShift = splitPage(io, page, buf, fwdId, fwd, fwdBuf, idx);

                    // Do insert.
                    int cnt = io.getCount(buf);

                    if (idx < cnt || (idx == cnt && !midShift)) { // Insert into back page.
                        insertSimple(page, io, buf, idx);

                        // Fix leftmost child of forward page, because newly inserted row will go up.
                        if (idx == cnt && !io.isLeaf()) {
                            inner(io).setLeft(fwdBuf, 0, rightId);

                            if (needWalDeltaRecord(fwd)) // Rare case, we can afford separate WAL record to avoid complexity.
                                wal.log(new FixLeftmostChildRecord(cacheId, fwd.id(), rightId));
                        }
                    }
                    else // Insert into newly allocated forward page.
                        insertSimple(fwd, io, fwdBuf, idx - cnt);

                    // Do move up.
                    cnt = io.getCount(buf);

                    // Last item from backward row goes up.
                    L moveUpRow = io.getLookupRow(BPlusTree.this, buf, cnt - 1);

                    if (!io.isLeaf()) { // Leaf pages must contain all the links, inner pages remove moveUpLink.
                        io.setCount(buf, cnt - 1);

                        if (needWalDeltaRecord(page)) // Rare case, we can afford separate WAL record to avoid complexity.
                            wal.log(new FixCountRecord(cacheId, page.id(), cnt - 1));
                    }

                    if (!hadFwd && lvl == getRootLevel(meta)) { // We are splitting root.
                        long newRootId = allocatePage(bag);

                        try (Page newRoot = page(newRootId)) {
                            if (io.isLeaf())
                                io = latestInnerIO();

                            ByteBuffer newRootBuf = newRoot.getForWrite(); // Initial write, no concurrent modification.

                            try {
                                // Never write full new root page, because it is known to be new.
                                newRoot.fullPageWalRecordPolicy(Boolean.FALSE);

                                long pageId = PageIO.getPageId(buf);

                                inner(io).initNewRoot(newRootBuf, newRootId, pageId, moveUpRow, null, fwdId);

                                if (needWalDeltaRecord(newRoot))
                                    wal.log(new NewRootInitRecord<>(cacheId, newRoot.id(), newRootId,
                                        inner(io), pageId, moveUpRow, null, fwdId));
                            }
                            finally {
                                newRoot.releaseWrite(true);
                            }
                        }

                        writePage(metaPageId, meta, addRoot, newRootId, lvl + 1);

                        return null; // We've just moved link up to root, nothing to return here.
                    }

                    // Regular split.
                    return moveUpRow;
                }
                finally {
                    fwd.releaseWrite(true);
                }
            }
        }
    }

    /**
     * Remove operation.
     */
    private final class Remove extends Get implements ReuseBag {
        /** */
        private boolean ceil;

        /** We may need to lock part of the tree branch from the bottom to up for multiple levels. */
        private Tail<L> tail;

        /** */
        Bool needReplaceInner = FALSE;

        /** */
        Bool needMergeEmptyBranch = FALSE;

        /** Removed row. */
        private T removed;

        /** Current page. */
        private Page page;

        /** */
        private short innerIdx = Short.MIN_VALUE;

        /** */
        private Object freePages;

        /** */
        private ReuseBag bag;

        /**
         * @param row Row.
         * @param ceil If we can remove ceil row when we can not find exact.
         */
        private Remove(L row, boolean ceil, ReuseBag bag) {
            super(row);

            this.ceil = ceil;
            this.bag = bag;
        }

        /**
         * @return Reuse bag.
         */
        private ReuseBag bag() {
            return bag != null ? bag : this;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public long pollFreePage() {
            assert bag == null;

            if (freePages == null)
                return 0;

            if (freePages.getClass() == GridLongList.class) {
                GridLongList list = ((GridLongList)freePages);

                return list.isEmpty() ? 0 : list.remove();
            }

            long res = (long)freePages;

            freePages = null;

            return res;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void addFreePage(long pageId) {
            assert pageId != 0;
            assert bag == null;

            if (freePages == null)
                freePages = pageId;
            else {
                GridLongList list;

                if (freePages.getClass() == GridLongList.class)
                    list = (GridLongList)freePages;
                else {
                    list = new GridLongList(4);

                    list.add((Long)freePages);
                    freePages = list;
                }

                list.add(pageId);
            }
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) {
            if (io.isLeaf())
                return true;

            // If we can get full row from the inner page, we must do inner replace to update full row info here.
            if (io.canGetRow() && needReplaceInner == FALSE)
                needReplaceInner = TRUE;

            return false;
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) {
            if (io.isLeaf()) {
                assert tail == null;

                return true;
            }

            return false;
        }

        /**
         * Finish the operation.
         */
        private void finish() {
            assert tail == null;

            row = null;
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void mergeEmptyBranch() throws IgniteCheckedException {
            assert needMergeEmptyBranch == TRUE;

            Tail<L> t = tail;

            assert t.getCount() > 0;

            // Find empty branch beginning.
            for (Tail<L> t0 = t.down; t0 != null; t0 = t0.down) {
                assert t0.type == Tail.EXACT : t0.type;

                if (t0.getCount() != 0)
                    t = t0;
            }

            while (t.lvl != 0) { // If we've found empty branch, merge it top down.
                boolean res = merge(t);

                assert res : needMergeEmptyBranch;

                if (needMergeEmptyBranch == TRUE)
                    needMergeEmptyBranch = READY; // Need to mark that we've already done the first iteration.

                t = t.down;
            }

            assert t.lvl == 0 : t.lvl;
        }

        /**
         * @param t Tail.
         * @return {@code true} If merged successfully or end reached.
         * @throws IgniteCheckedException If failed.
         */
        private boolean mergeBottomUp(Tail<L> t) throws IgniteCheckedException {
            assert needMergeEmptyBranch == FALSE || needMergeEmptyBranch == DONE : needMergeEmptyBranch;

            if (t.down == null)
                return true;

            if (t.down.sibling == null) // We've merged something there.
                return false;

            return mergeBottomUp(t.down) && merge(t);
        }

        /**
         * Process tail and finish.
         *
         * @return {@code false} If failed to finish and we need to lock more pages up.
         * @throws IgniteCheckedException If failed.
         */
        private boolean finishTail() throws IgniteCheckedException {
            assert !isFinished();
            assert tail.type == Tail.EXACT;

            boolean mergedBranch = false;

            if (needMergeEmptyBranch == TRUE) {
                // We can't merge empty branch if tail in routing page.
                if (tail.down == null || tail.getCount() == 0)
                    return false; // Lock the whole branch up to the first non-empty.

                mergeEmptyBranch();

                mergedBranch = true;
                needMergeEmptyBranch = DONE;
            }

            mergeBottomUp(tail);

            if (needReplaceInner == READY) {
                // If we've merged empty branch right now, then the inner key was dropped.
                if (!mergedBranch)
                    replaceInner(); // Replace inner key with new max key for the left subtree.

                needReplaceInner = DONE;
            }
            else if (needReplaceInner == TRUE)
                return false; // Lock the whole branch up to the inner key page.

            if (tail.getCount() == 0 && tail.lvl != 0 && getRootLevel(meta) == tail.lvl) {
                // Free root if it became empty after merge.
                cutRoot(tail.lvl);
                freePage(tail.pageId, tail.page, tail.buf, false);
            }
            else if (tail.sibling != null &&
                tail.getCount() + tail.sibling.getCount() < tail.io.getMaxCount(tail.buf)) {
                // Release everything lower than tail, we've already merged this path.
                doReleaseTail(tail.down);

                tail.down = null;

                return false; // Lock and merge one level more.
            }

            releaseTail();
            finish();

            return true;
        }

        /**
         * @param leafId Leaf page ID.
         * @param leaf Leaf page.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result removeFromLeaf(long leafId, Page leaf, long backId, long fwdId) throws IgniteCheckedException {
            // Init parameters.
            pageId = leafId;
            page = leaf;
            this.backId = backId;
            this.fwdId = fwdId;

            if (backId == 0)
                return doRemoveFromLeaf();

            // Lock back page before the remove, we'll need it for merges.
            Page back = page(backId);

            try {
                return writePage(backId, back, lockBackAndRemoveFromLeaf, this, 0);
            }
            finally {
                if (canRelease(backId, back, 0))
                    back.close();
            }
        }

        /**
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result doRemoveFromLeaf() throws IgniteCheckedException {
            assert page != null;

            return writePage(pageId, page, removeFromLeaf, this, 0);
        }

        /**
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result doLockTail(int lvl) throws IgniteCheckedException {
            assert page != null;

            return writePage(pageId, page, lockTail, this, lvl);
        }

        /**
         * @param pageId Page ID.
         * @param page Page.
         * @param backId Back page ID.
         * @param fwdId Expected forward page ID.
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result lockTail(long pageId, Page page, long backId, long fwdId,
            int lvl) throws IgniteCheckedException {
            assert tail != null;

            // Init parameters for the handlers.
            this.pageId = pageId;
            this.page = page;
            this.fwdId = fwdId;
            this.backId = backId;

            if (backId == 0) // Back page ID is provided only when the last move was to the right.
                return doLockTail(lvl);

            Page back = page(backId);

            try {
                return writePage(backId, back, lockBackAndTail, this, lvl);
            }
            finally {
                if (canRelease(backId, back, lvl))
                    back.close();
            }
        }

        /**
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private Result lockForward(int lvl) throws IgniteCheckedException {
            assert fwdId != 0;
            assert backId == 0;

            return writePage(fwdId, page(fwdId), lockTailForward, this, lvl);
        }

        /**
         * @param page Page.
         * @param io IO.
         * @param buf Buffer.
         * @param cnt Count.
         * @param idx Index to remove.
         * @param rmvId Remove ID or {@code 0} to ignore.
         * @throws IgniteCheckedException If failed.
         */
        private void doRemove(Page page, BPlusIO io, ByteBuffer buf, int cnt, int idx, long rmvId)
            throws IgniteCheckedException {
            assert cnt > 0 : cnt;
            assert idx >= 0 && idx < cnt : idx + " " + cnt;

            io.remove(buf, idx, cnt, rmvId);

            if (needWalDeltaRecord(page))
                wal.log(new RemoveRecord(cacheId, page.id(), idx, cnt, rmvId));
        }

        /**
         * @param prnt Parent tail.
         * @param left Left child tail.
         * @param right Right child tail.
         * @return {@code true} If merged successfully.
         * @throws IgniteCheckedException If failed.
         */
        private boolean doMerge(Tail<L> prnt, Tail<L> left, Tail<L> right)
            throws IgniteCheckedException {
            assert right.io == left.io; // Otherwise incompatible.
            assert left.io.getForward(left.buf) == right.pageId;

            int prntCnt = prnt.getCount();

            assert prntCnt > 0 || needMergeEmptyBranch == READY : prntCnt;

            boolean emptyBranch = needMergeEmptyBranch == TRUE || needMergeEmptyBranch == READY;

            // Fix index for the right move: remove last item.
            int prntIdx = prnt.idx == prntCnt ? prntCnt - 1 : prnt.idx;

            if (!left.io.merge(prnt.io, prnt.buf, prntIdx, left.buf, right.buf, emptyBranch))
                return false;

            if (needWalDeltaRecord(left.page))
                wal.log(new MergeRecord<>(cacheId, left.page.id(), prnt.page.id(), prntIdx,
                    right.page.id(), emptyBranch));

            // Remove split key from parent. If we are merging empty branch then remove only on the top iteration.
            if (needMergeEmptyBranch != READY)
                doRemove(prnt.page, prnt.io, prnt.buf, prntCnt, prntIdx, 0L);

            // Forward page is now empty and has no links, can free and release it right away.
            freePage(right.pageId, right.page, right.buf, true);

            return true;
        }

        /**
         * @param pageId Page ID.
         * @param page Page.
         * @param buf Buffer.
         * @param release Release write lock and release page.
         * @throws IgniteCheckedException If failed.
         */
        private void freePage(long pageId, Page page, ByteBuffer buf, boolean release)
            throws IgniteCheckedException {
            pageId = recyclePage(pageId, page, buf);

            if (release)
                writeUnlockAndClose(page);

            bag().addFreePage(pageId);
        }

        /**
         * @param lvl Expected root level.
         * @throws IgniteCheckedException If failed.
         */
        private void cutRoot(int lvl) throws IgniteCheckedException {
            writePage(metaPageId, meta, cutRoot, null, lvl);
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void reuseFreePages() throws IgniteCheckedException {
            // If we have a bag, then it will be processed at the upper level.
            if (reuseList != null && bag == null)
                reuseList.add(this);
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void replaceInner() throws IgniteCheckedException {
            assert needReplaceInner == READY : needReplaceInner;
            assert tail.lvl > 0 : "leaf";
            assert innerIdx >= 0 : innerIdx;

            Tail<L> leaf = getTail(0);
            Tail<L> inner = tail;

            assert inner.type == Tail.EXACT : inner.type;

            int innerCnt = inner.getCount();
            int leafCnt = leaf.getCount();

            assert leafCnt > 0 : leafCnt; // Leaf must be merged at this point already if it was empty.

            if (innerIdx < innerCnt) {
                int leafIdx = leafCnt - 1; // Last leaf item.

                // Update inner key with the new biggest key of left subtree.
                inner.io.store(inner.buf, innerIdx, leaf.io, leaf.buf, leafIdx);

                if (needWalDeltaRecord(inner.page))
                    wal.log(new InnerReplaceRecord<>(cacheId, inner.page.id(),
                        innerIdx, leaf.page.id(), leafIdx));
            }
            else {
                // If after leaf merge parent have lost inner key, we don't need to update it anymore.
                assert innerIdx == innerCnt;
                assert inner(inner.io).getLeft(inner.buf, innerIdx) == leaf.pageId;
            }
        }

        /**
         * @param prnt Parent for merge.
         * @return {@code true} If merged, {@code false} if not (because of insufficient space or empty parent).
         * @throws IgniteCheckedException If failed.
         */
        private boolean merge(Tail<L> prnt) throws IgniteCheckedException {
            // If we are merging empty branch this is acceptable because even if we merge
            // two routing pages, one of them is effectively dropped in this merge, so just
            // keep a single routing page.
            if (prnt.getCount() == 0 && needMergeEmptyBranch != READY)
                return false; // Parent is an empty routing page, child forward page will have another parent.

            Tail<L> right = prnt.down;
            Tail<L> left = right.sibling;

            assert right.type == Tail.EXACT;
            assert left != null : "we must have a partner to merge with";

            if (left.type != Tail.BACK) { // Flip if it was actually FORWARD but not BACK.
                assert left.type == Tail.FORWARD : left.type;

                left = right;
                right = right.sibling;
            }

            assert right.io == left.io : "must always be the same"; // Otherwise can be not compatible.

            if (!doMerge(prnt, left, right))
                return false;

            // left from BACK becomes EXACT.
            if (left.type == Tail.BACK) {
                assert left.sibling == null;

                left.down = right.down;
                left.type = Tail.EXACT;
                prnt.down = left;
            }
            else { // left is already EXACT.
                assert left.type == Tail.EXACT : left.type;
                assert left.sibling != null;

                left.sibling = null;
            }

            return true;
        }

        /**
         * @return {@code true} If finished.
         */
        private boolean isFinished() {
            return row == null;
        }

        /**
         * Release pages for all locked levels at the tail.
         */
        private void releaseTail() {
            doReleaseTail(tail);

            tail = null;
        }

        /**
         * @param t Tail.
         */
        private void doReleaseTail(Tail<L> t) {
            while (t != null) {
                writeUnlockAndClose(t.page);

                if (t.sibling != null)
                    writeUnlockAndClose(t.sibling.page);

                t = t.down;
            }
        }

        /** {@inheritDoc} */
        @Override boolean canRelease(long pageId, Page page, int lvl) {
            return page != null && !isTail(pageId, lvl);
        }

        /**
         * @param pageId Page ID.
         * @param lvl Level.
         * @return {@code true} If the given page is in tail.
         */
        private boolean isTail(long pageId, int lvl) {
            Tail t = tail;

            while (t != null) {
                if (t.lvl < lvl)
                    return false;

                if (t.lvl == lvl) {
                    if (t.pageId == pageId)
                        return true;

                    t = t.sibling;

                    return t != null && t.pageId == pageId;
                }

                t = t.down;
            }

            return false;
        }

        /**
         * @param pageId Page ID.
         * @param page Page.
         * @param buf Buffer.
         * @param io IO.
         * @param lvl Level.
         * @param type Type.
         * @param idx Insertion index or negative flag describing if the page is primary in this tail branch.
         */
        private void addTail(long pageId, Page page, ByteBuffer buf, BPlusIO<L> io, int lvl, byte type, int idx) {
            Tail<L> t = new Tail<>(pageId, page, buf, io, type, lvl, idx);

            if (tail == null)
                tail = t;
            else if (tail.lvl == lvl) { // Add on the same level.
                assert tail.sibling == null; // Only two siblings on a single level.

                if (type == Tail.EXACT) {
                    assert tail.type != Tail.EXACT;

                    if (tail.down != null) { // Take down from sibling, EXACT must own down link.
                        t.down = tail.down;
                        tail.down = null;
                    }

                    t.sibling = tail;
                    tail = t;
                }
                else {
                    assert tail.type == Tail.EXACT : tail.type;

                    tail.sibling = t;
                }
            }
            else if (tail.lvl == lvl - 1) { // Add on top of existing level.
                t.down = tail;
                tail = t;
            }
            else
                throw new IllegalStateException();
        }

        /**
         * @param lvl Level.
         * @return Tail of {@link Tail#EXACT} type at the given level.
         */
        private Tail<L> getTail(int lvl) {
            assert tail != null;
            assert lvl >= 0 && lvl <= tail.lvl : lvl;

            Tail<L> t = tail;

            while (t.lvl != lvl)
                t = t.down;

            assert t.type == Tail.EXACT : t.type; // All the down links must be of EXACT type.

            return t;
        }
    }

    /**
     * Tail for remove.
     */
    private static final class Tail<L> {
        /** */
        static final byte BACK = 0;

        /** */
        static final byte EXACT = 1;

        /** */
        static final byte FORWARD = 2;

        /** */
        private final long pageId;

        /** */
        private final Page page;

        /** */
        private final ByteBuffer buf;

        /** */
        private final BPlusIO<L> io;

        /** */
        private byte type;

        /** */
        private final byte lvl;

        /** */
        private final short idx;

        /** Only {@link #EXACT} tail can have either {@link #BACK} or {@link #FORWARD} sibling.*/
        private Tail<L> sibling;

        /** Only {@link #EXACT} tail can point to {@link #EXACT} tail of lower level. */
        private Tail<L> down;

        /**
         * @param pageId Page ID.
         * @param page Write locked page.
         * @param buf Buffer.
         * @param io IO.
         * @param type Type.
         * @param lvl Level.
         * @param idx Insertion index.
         */
        private Tail(long pageId, Page page, ByteBuffer buf, BPlusIO<L> io, byte type, int lvl, int idx) {
            assert type == BACK || type == EXACT || type == FORWARD : type;
            assert idx == -1 || (idx >= 0 && idx <= Short.MAX_VALUE) : idx;
            assert lvl >= 0 && lvl <= Byte.MAX_VALUE : lvl;
            assert page != null;
            assert pageId != 0;

            this.pageId = pageId;
            this.page = page;
            this.buf = buf;
            this.io = io;
            this.type = type;
            this.lvl = (byte)lvl;
            this.idx = (short)idx;
        }

        /**
         * @return Count.
         */
        private int getCount() {
            return io.getCount(buf);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return new SB("Tail[").a("pageId=").appendHex(pageId).a(", cnt= ").a(getCount())
                .a(", lvl=" + lvl).a(", sibling=").a(sibling).a("]").toString();
        }
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param cnt Row count.
     * @param row Lookup row.
     * @return Insertion point as in {@link Arrays#binarySearch(Object[], Object, Comparator)}.
     */
    private int findInsertionPoint(BPlusIO<L> io, ByteBuffer buf, int cnt, L row, int shift)
        throws IgniteCheckedException {
        assert row != null;

        int low = 0;
        int high = cnt - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            int cmp = compare(io, buf, mid, row);

            if (cmp == 0)
                cmp = -shift; // We need to fix the case when search row matches multiple data rows.

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // Found.
        }

        return -(low + 1);  // Not found.
    }

    /**
     * @param buf Buffer.
     * @return IO.
     */
    private BPlusIO<L> io(ByteBuffer buf) {
        assert buf != null;

        int type = PageIO.getType(buf);
        int ver = PageIO.getVersion(buf);

        if (innerIos.getType() == type)
            return innerIos.forVersion(ver);

        if (leafIos.getType() == type)
            return leafIos.forVersion(ver);

        throw new IllegalStateException("Unknown page type: " + type + " pageId: " + U.hexLong(PageIO.getPageId(buf)));
    }

    /**
     * @param io IO.
     * @return Inner page IO.
     */
    private static <L> BPlusInnerIO<L> inner(BPlusIO<L> io) {
        assert !io.isLeaf();

        return (BPlusInnerIO<L>)io;
    }

    /**
     * @param pageId Page ID.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page page(long pageId) throws IgniteCheckedException {
        if (PageIdUtils.flag(pageId) == PageIdAllocator.FLAG_IDX)
            pageId = PageIdUtils.maskPartId(pageId);

        return pageMem.page(cacheId, pageId);
    }

    /**
     * @param bag Reuse bag.
     * @return Allocated page.
     */
    private long allocatePage(ReuseBag bag) throws IgniteCheckedException {
        long pageId = bag != null ? bag.pollFreePage() : 0;

        if (pageId == 0 && reuseList != null)
            pageId = reuseList.take(this, bag);

        if (pageId == 0)
            pageId = allocatePageNoReuse();

        assert pageId != 0;

        return pageId;
    }

    /**
     * Allocates page for new BPlus tree.
     *
     * @return New page ID.
     * @throws IgniteCheckedException If failed.
     */
    protected long allocatePageForNew() throws IgniteCheckedException {
        return allocatePage(null);
    }

    /**
     * @return Page ID of newly allocated page.
     * @throws IgniteCheckedException If failed.
     */
    protected final long allocatePageNoReuse() throws IgniteCheckedException {
        return pageMem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_IDX);
    }

    /**
     * @return Latest version of inner page IO.
     */
    private BPlusInnerIO<L> latestInnerIO() {
        return innerIos.latest();
    }

    /**
     * @return Latest version of leaf page IO.
     */
    private BPlusLeafIO<L> latestLeafIO() {
        return leafIos.latest();
    }

    /**
     * @param buf Buffer.
     * @param idx Index of row in the given buffer.
     * @param row Lookup row.
     * @return Comparison result as in {@link Comparator#compare(Object, Object)}.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract int compare(BPlusIO<L> io, ByteBuffer buf, int idx, L row) throws IgniteCheckedException;

    /**
     * This method can be called only if {@link BPlusIO#canGetRow()} returns {@code true}.
     *
     * @param io IO.
     * @param buf Buffer.
     * @param idx Index.
     * @return Full data row.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract T getRow(BPlusIO<L> io, ByteBuffer buf, int idx) throws IgniteCheckedException;

    /**
     * Forward cursor.
     */
    private final class ForwardCursor implements GridCursor<T> {
        /** */
        private T[] rows;

        /** */
        private int row = -1;

        /** */
        private long nextPageId;

        /** */
        private final L upperBound;

        /**
         * @param upperBound Upper bound.
         */
        ForwardCursor(L upperBound) {
            this.upperBound = upperBound;
        }

        /**
         * @param buf Buffer.
         * @param io IO.
         * @param cnt Number of rows in the buffer.
         * @return Corrected number of rows with respect to upper bound.
         * @throws IgniteCheckedException If failed.
         */
        private int findUpperBound(ByteBuffer buf, BPlusIO<L> io, int cnt) throws IgniteCheckedException {
            // Compare with the last row on the page.
            int cmp = compare(io, buf, cnt - 1, upperBound);

            if (cmp > 0) {
                int idx = findInsertionPoint(io, buf, cnt, upperBound, 1);

                assert idx < 0;

                cnt = -idx - 1;

                nextPageId = 0; // The End.
            }

            return cnt;
        }

        /**
         * @param buf Buffer.
         * @param io IO.
         * @param startIdx Start index.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void fillFromBuffer(ByteBuffer buf, BPlusIO<L> io, int startIdx) throws IgniteCheckedException {
            assert buf != null;
            assert io.isLeaf();
            assert startIdx >= 0 : startIdx;

            checkDestroyed();

            nextPageId = io.getForward(buf);
            int cnt = io.getCount(buf);

            assert cnt >= startIdx;

            if (upperBound != null && startIdx != cnt)
                cnt = findUpperBound(buf, io, cnt);

            cnt -= startIdx;

            if (cnt > 0) {
                if (rows == null)
                    rows = (T[])new Object[cnt];

                for (int i = 0; i < cnt; i++) {
                    T r = getRow(io, buf, startIdx + i);

                    rows = GridArrays.set(rows, i, r);
                }

                GridArrays.clearTail(rows, cnt);
            }
            else {
                assert nextPageId == 0;

                rows = null;
            }
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SimplifiableIfStatement")
        @Override public boolean next() throws IgniteCheckedException {
            if (rows == null)
                return false;

            if (++row < rows.length && rows[row] != null) {
                clearLastRow();

                return true;
            }

            return nextPage();
        }

        /**
         * @return Cleared last row.
         */
        private T clearLastRow() {
            if (row == 0)
                return null;

            int last = row - 1;

            T r = rows[last];

            assert r != null;

            rows[last] = null;

            return r;
        }

        /**
         * @param lastRow Last row.
         * @throws IgniteCheckedException If failed.
         */
        private void reinitialize(T lastRow) throws IgniteCheckedException {
            assert lastRow != null;

            // Here we have shift 1 because otherwise we can return the same row twice.
            doFind(new GetCursor(lastRow, 1, this));
        }

        /**
         * @return {@code true} If we have rows to return after reading the next page.
         * @throws IgniteCheckedException If failed.
         */
        private boolean nextPage() throws IgniteCheckedException {
            if (nextPageId == 0) {
                rows = null;

                return false;
            }

            T lastRow = clearLastRow();

            boolean reinitialize = false;

            try (Page next = page(nextPageId)) {
                ByteBuffer buf = next.getForRead(); // Doing explicit page ID check.

                try {
                    // If concurrent merge occurred we have to reinitialize cursor from the last returned row.
                    if (PageIO.getPageId(buf) != nextPageId)
                        reinitialize = true;
                    else
                        fillFromBuffer(buf, io(buf), 0);
                }
                finally {
                    next.releaseRead();
                }
            }

            if (reinitialize) // Reinitialize when `next` page is released.
                reinitialize(lastRow);

            row = 0;

            return rows != null;
        }

        /** {@inheritDoc} */
        @Override public T get() {
            T r = rows[row];

            assert r != null;

            return r;
        }
    }

    /**
     * Page handler for basic {@link Get} operation.
     */
    private abstract class GetPageHandler<G extends Get> extends PageHandler<G, Result> {
        /** {@inheritDoc} */
        @Override public final Result run(long pageId, Page page, ByteBuffer buf, G g, int lvl)
            throws IgniteCheckedException {
            // The page was merged and removed.
            if (PageIO.getPageId(buf) != pageId)
                return RETRY;

            BPlusIO<L> io = io(buf);

            // In case of intersection with inner replace remove operation
            // we need to restart our operation from the root.
            if (g.rmvId < io.getRemoveId(buf))
                return RETRY_ROOT;

            return run0(pageId, page, buf, io, g, lvl);
        }

        /**
         * @param pageId Page ID.
         * @param page Page.
         * @param buf Buffer.
         * @param io IO.
         * @param g Operation.
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        protected abstract Result run0(long pageId, Page page, ByteBuffer buf, BPlusIO<L> io, G g, int lvl)
            throws IgniteCheckedException;

        /** {@inheritDoc} */
        @Override public final boolean releaseAfterWrite(long pageId, Page page, G g, int lvl) {
            return g.canRelease(pageId, page, lvl);
        }
    }

    /**
     * Reuse bag for destroy.
     */
    protected static final class DestroyBag extends GridLongList implements ReuseBag {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Default constructor for {@link Externalizable}.
         */
        public DestroyBag() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void addFreePage(long pageId) {
            add(pageId);
        }

        /** {@inheritDoc} */
        @Override public long pollFreePage() {
            return isEmpty() ? 0 : remove();
        }
    }

    /**
     * Operation result.
     */
    enum Result {
        GO_DOWN, GO_DOWN_X, FOUND, NOT_FOUND, RETRY, RETRY_ROOT
    }

    /**
     * Four state boolean.
     */
    enum Bool {
        TRUE, FALSE, READY, DONE
    }
}
