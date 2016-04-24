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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridTreePrinter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.readPage;
import static org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler.writePage;

/**
 * Abstract B+Tree.
 */
public abstract class BPlusTree<L, T extends L> {
    /** */
    private static final byte FALSE = 0;

    /** */
    private static final byte TRUE = 1;

    /** */
    private static final byte READY = 2;

    /** */
    private static final byte DONE = 3;

    /** */
    protected final int cacheId;

    /** */
    private final PageMemory pageMem;

    /** */
    private final ReuseList reuseList;

    /** */
    private final float minFill;

    /** */
    private final float maxFill;

    /** */
    private final long metaPageId;

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
                ByteBuffer buf = page.getForRead();

                try {
                    BPlusIO io = io(buf);

                    if (io.isLeaf())
                        return null;

                    List<Long> res;

                    int cnt = io.getCount(buf);

                    assert cnt >= 0: cnt;

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
                ByteBuffer buf = page.getForRead();

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
    private final PageHandler<Get> search = new GetPageHandler<Get>() {
        @Override public int run0(Page page, ByteBuffer buf, BPlusIO<L> io, Get g, int lvl)
            throws IgniteCheckedException {
            g.backId = 0; // Usually we'll go left down.

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, g.row);

            boolean found = idx >= 0;

            if (found) { // Found exact match.
                if (g.found(io, buf, idx, lvl))
                    return Get.FOUND;

                assert !io.isLeaf();

                // Else we need to reach leaf page, go left down.
            }
            else {
                idx = -idx - 1;

                // If we are on the right edge, then check for expected forward page and retry of it does match.
                // It means that concurrent split happened. This invariant is referred as `triangle`.
                if (idx == cnt && io.getForward(buf) != g.fwdId)
                    return Get.RETRY;

                if (g.notFound(io, buf, idx, lvl)) // No way down, stop here.
                    return Get.NOT_FOUND;

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
                // need to ask our forward about the child's forward (it must be leftmost child or our forward page).
                // This is ok from the locking standpoint because we take all locks in the forward direction.
                long fwdId = io.getForward(buf);

                g.fwdId = fwdId == 0 ? 0 : getLeftmostChild(fwdId);

                if (cnt != 0) // If empty, it is a routing page and we go to the left, otherwise we go to the right.
                    g.backId = inner(io).getLeft(buf, cnt - 1);
            }

            return Get.GO_DOWN;
        }
    };

    /** */
    private final PageHandler<Put> replace = new GetPageHandler<Put>() {
        @Override public int run0(Page page, ByteBuffer buf, BPlusIO<L> io, Put p, int lvl)
            throws IgniteCheckedException {
            assert p.btmLvl == 0 : "split is impossible with replace";

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, p.row);

            if (idx < 0) { // Not found, split or merge happened.
                long fwdId = io.getForward(buf);

                assert fwdId != 0;

                p.pageId = fwdId;

                return Put.RETRY;
            }

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

            io.store(buf, idx, newRow);

            return Put.FOUND;
        }
    };

    /** */
    private final PageHandler<Put> insert = new GetPageHandler<Put>() {
        @Override public int run0(Page page, ByteBuffer buf, BPlusIO<L> io, Put p, int lvl)
            throws IgniteCheckedException {
            assert p.btmLvl == lvl: "we must always insert at the bottom level: " + p.btmLvl + " " + lvl;

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, p.row);

            assert idx < 0: "Duplicate row in index.";

            idx = -idx - 1;

            // Possible split or merge.
            if (idx == cnt && io.getForward(buf) != p.fwdId)
                return Put.RETRY;

            // Do insert.
            L moveUpRow = insert(p.meta, io, buf, p.row, idx, p.rightId, lvl);

            // Check if split happened.
            if (moveUpRow != null) {
                p.btmLvl++; // Get high.
                p.row = moveUpRow;

                // Here `forward` can't be concurrently removed because we keep `tail` which is the only
                // page who knows about the `forward` page, because it was just produced by split.
                p.rightId = io.getForward(buf);
                p.tail(page);

                assert p.rightId != 0;
            }
            else
                p.finish();

            return Put.FOUND;
        }
    };

    /** */
    private final PageHandler<Remove> removeFromLeaf = new GetPageHandler<Remove>() {
        @Override public int run0(Page leaf, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            assert lvl == 0: lvl;
            assert r.removed == null;
            assert io.isLeaf();
            assert io.canGetRow();

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, r.row);

            if (idx < 0) {
                if (!r.ceil) // We've found exact match on search but now it's gone.
                    return Remove.RETRY;

                idx = -idx - 1;

                if (idx == cnt) // We can not remove ceiling row here.
                    return Remove.NOT_FOUND;

                assert idx < cnt;
            }

            r.removed = getRow(io, buf, idx);

            r.doRemove(io, leaf, buf, cnt, idx, 0, false);

            // We may need to replace inner key or want to merge this leaf with sibling after the remove -> keep lock.
            if (r.needReplaceInner == TRUE ||
                // We need to make sure that we have back or forward to be able to merge.
                ((r.fwdId != 0 || r.backId != 0) && mayMerge(cnt - 1, io.getMaxCount(buf)))) {
                // If we have backId then we've already locked back page, nothing to do here.
                if (r.fwdId != 0 && r.backId == 0)
                    r.lockForward(0);

                r.addTail(leaf, buf, io, 0, Tail.EXACT, Integer.MIN_VALUE);

                if (r.needReplaceInner == FALSE)
                    r.needMerge = TRUE;
            }

            return Remove.FOUND;
        }
    };

    /** */
    private final PageHandler<Remove> lockBackAndRemoveFromLeaf = new GetPageHandler<Remove>() {
        @Override protected int run0(Page back, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            // Check that we have consistent view of the world.
            if (io.getForward(buf) != r.pageId)
                return Remove.RETRY;

            // Correct locking order: from back to forward.
            int res = r.doRemoveFromLeaf();

            // If we need to do more tricks, then we have to keep locks on back and leaf pages.
            if (res == Remove.FOUND && (r.needMerge == TRUE || r.needReplaceInner == TRUE)) {
                assert r.needMerge == TRUE ^ r.needReplaceInner == TRUE: "we can do only one thing at once";

                r.addTail(back, buf, io, lvl, Tail.BACK, Integer.MIN_VALUE);
            }

            return res;
        }
    };

    /** */
    private final PageHandler<Remove> lockBackAndTail = new GetPageHandler<Remove>() {
        @Override public int run0(Page back, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            // Check that we have consistent view of the world.
            if (io.getForward(buf) != r.pageId)
                return Remove.RETRY;

            // Correct locking order: from back to forward.
            int res = r.doLockTail(lvl);

            if (res == Remove.FOUND)
                r.addTail(back, buf, io, lvl, Tail.BACK, Integer.MIN_VALUE);

            return res;
        }
    };

    /** */
    private final PageHandler<Remove> lockTailForward = new GetPageHandler<Remove>() {
        @Override protected int run0(Page page, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {

            r.addTail(page, buf, io, lvl, Tail.FORWARD, Integer.MIN_VALUE);

            return Remove.FOUND;
        }
    };

    /** */
    private final PageHandler<Remove> lockTail = new GetPageHandler<Remove>() {
        @Override public int run0(Page page, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            assert lvl > 0: lvl; // We are not at the bottom.

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, r.row);

            boolean found = idx >= 0;

            if (found) {
                // We can not miss the inner value on down move because of `triangle` invariant, thus it must be TRUE.
                assert r.needReplaceInner == TRUE: r.needReplaceInner;
                assert idx <= Short.MAX_VALUE : idx;

                r.innerIdx = (short)idx;

                r.needReplaceInner = READY;
            }
            else {
                idx = -idx - 1;

                // Check that we have a correct view of the world.
                if (idx == cnt && io.getForward(buf) != r.fwdId)
                    return Remove.RETRY;
            }

            // Check that we have a correct view of the world.
            if (lvl != 0 && inner(io).getLeft(buf, idx) != r.getTail(lvl - 1).page.id()) {
                assert !found;

                return Remove.RETRY;
            }

            // We don't have a back page, need to lock our forward and become a back for it.
            // If found then we are on inner replacement page, it will be a top parent, no need to lock forward.
            boolean noBack = !found && r.fwdId != 0 && r.backId == 0;

            if (noBack)
                r.lockForward(lvl);

            r.addTail(page, buf, io, lvl, Tail.EXACT, idx);

            if (r.needMerge == TRUE) {
                assert r.needReplaceInner == FALSE;
                assert r.tail.down.type == Tail.EXACT;

                r.needMerge = READY;
            }

            return Remove.FOUND;
        }
    };

    /** */
    private final PageHandler<Long> updateLeftmost = new PageHandler<Long>() {
        @Override public int run(Page page, ByteBuffer buf, Long pageId, int lvl) throws IgniteCheckedException {
            assert pageId != null;

            BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(buf);

            assert io.getLevelsCount(buf) > lvl;

            io.setLeftmostPageId(buf, lvl, pageId);

            if (pageId == 0) {
                assert lvl == io.getRootLevel(buf);

                io.setLevelsCount(buf, lvl); // Decrease tree height.
            }

            return TRUE;
        }
    };

    /** */
    private final PageHandler<Long> updateRoot = new PageHandler<Long>() {
        @Override public int run(Page page, ByteBuffer buf, Long rootPageId, int lvl) throws IgniteCheckedException {
            BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(buf);

            int cnt = io.getLevelsCount(buf);

            if (rootPageId != null) {
                io.setLevelsCount(buf, cnt + 1);
                io.setLeftmostPageId(buf, cnt, rootPageId);
            }
            else
                io.setLevelsCount(buf, cnt - 1);

            return TRUE;
        }
    };

    /**
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @throws IgniteCheckedException If failed.
     */
    public BPlusTree(int cacheId, PageMemory pageMem, FullPageId metaPageId, ReuseList reuseList)
        throws IgniteCheckedException {
        // TODO make configurable: 0 <= minFill <= maxFill <= 1
        minFill = 0f; // Testing worst case when merge happens only on empty page.
        maxFill = 0f; // Avoiding random effects on testing.

        assert pageMem != null;

        this.pageMem = pageMem;
        this.cacheId = cacheId;
        this.metaPageId = metaPageId.pageId();
        this.reuseList = reuseList;
    }

    /**
     * @return Cache ID.
     */
    public int getCacheId() {
        return cacheId;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initNew() throws IgniteCheckedException {
        try (Page meta = page(metaPageId)) {
            ByteBuffer buf = meta.getForInitialWrite();

            BPlusMetaIO io = BPlusMetaIO.VERSIONS.latest();

            io.initNewPage(buf, metaPageId);

            try (Page root = allocatePage()) {
                latestLeafIO().initNewPage(root.getForInitialWrite(), root.id());

                io.setLevelsCount(buf, 1);
                io.setLeftmostPageId(buf, 0, root.id());
            }
        }
    }

    /**
     * @return Root level.
     */
    private int getRootLevel(Page meta) {
        ByteBuffer buf = meta.getForRead();

        try {
            return BPlusMetaIO.VERSIONS.forPage(buf).getRootLevel(buf);
        }
        finally {
            meta.releaseRead();
        }
    }

    /**
     * @param meta Meta page.
     * @param lvl Level, if {@code 0} then it is a bottom level, if {@link Integer#MIN_VALUE}, then root.
     * @return Page ID.
     */
    private long getLeftmostPageId(Page meta, int lvl) {
        ByteBuffer buf = meta.getForRead();

        try {
            BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(buf);

            if (lvl == Integer.MIN_VALUE)
                lvl = io.getRootLevel(buf);

            if (lvl >= io.getLevelsCount(buf))
                return 0;

            return io.getLeftmostPageId(buf, lvl);
        }
        finally {
            meta.releaseRead();
        }
    }

    /**
     * @param upper Upper bound.
     * @return Cursor.
     */
    private GridCursor<T> findNoLower(L upper) throws IgniteCheckedException {
        ForwardCursor cursor = new ForwardCursor(upper);

        long firstPageId;

        try (Page meta = page(metaPageId)) {
            firstPageId = getLeftmostPageId(meta, 0); // Level 0 is always at the bottom.
        }

        try (Page first = page(firstPageId)) {
            ByteBuffer buf = first.getForRead();

            try {
                cursor.bootstrap(buf, 0);
            }
            finally {
                first.releaseRead();
            }
        }

        return cursor;
    }

    /**
     * @param lower Lower bound.
     * @param upper Upper bound.
     * @return Cursor.
     * @throws IgniteCheckedException If failed.
     */
    public final GridCursor<T> find(L lower, L upper) throws IgniteCheckedException {
        if (lower == null)
            return findNoLower(upper);

        GetCursor g = new GetCursor(lower, upper);

        doFind(g);

        return g.cursor;
    }

    /**
     * @param row Lookup row for exact match.
     * @return Found row.
     */
    @SuppressWarnings("unchecked")
    public final T findOne(L row) throws IgniteCheckedException {
        GetOne g = new GetOne(row);

        doFind(g);

        return (T)g.row;
    }

    /**
     * @param g Get.
     */
    private void doFind(Get g) throws IgniteCheckedException {
        try {
            for (;;) { // Go down with retries.
                g.initOperation();

                switch (findDown(g, g.rootId, 0L, g.rootLvl)) {
                    case Get.RETRY:
                    case Get.RETRY_ROOT:
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
    private int findDown(final Get g, final long pageId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        Page page = page(pageId);

        try {
            for (;;) {
                // Init args.
                g.pageId = pageId;
                g.fwdId = fwdId;

                int res = readPage(page, search, g, lvl, Get.RETRY);

                switch (res) {
                    case Get.GO_DOWN:
                        assert g.pageId != pageId;
                        assert g.fwdId != fwdId || fwdId == 0;

                        // Go down recursively.
                        res = findDown(g, g.pageId, g.fwdId, lvl - 1);

                        switch (res) {
                            case Get.RETRY:
                                checkInterrupted();

                                continue; // The child page got splitted, need to reread our page.

                            default:
                                return res;
                        }

                    case Get.NOT_FOUND:
                        assert lvl == 0: lvl;

                        g.row = null; // Mark not found result.

                        return res;

                    default:
                        return res;
                }
            }
        }
        finally {
            if (g.canRelease(page, lvl))
                page.close();
        }
    }

    /**
     * For debug.
     *
     * @return Tree as {@link String}.
     */
    @SuppressWarnings("unused")
    public String printTree() {
        long rootPageId;

        try (Page meta = page(metaPageId)) {
            rootPageId = getLeftmostPageId(meta, Integer.MIN_VALUE);
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException(e);
        }

        return treePrinter.print(rootPageId);
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
            b.append("lm=").append(inner(io).getLeft(buf, 0)).append(' ');

            if (cnt > 0)
                b.append("rm=").append(inner(io).getRight(buf, cnt - 1)).append(' ');
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
        return Long.toString(x); //'x' + Long.toHexString(x).toUpperCase();
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
     * @return Removed row.
     * @throws IgniteCheckedException If failed.
     */
    public final T removeCeil(L row) throws IgniteCheckedException {
        assert row != null;

        return doRemove(row, true);
    }

    /**
     * @param row Lookup row.
     * @return Removed row.
     * @throws IgniteCheckedException If failed.
     */
    public final T remove(L row) throws IgniteCheckedException {
        assert row != null;

        return doRemove(row, false);
    }

    /**
     * @param row Lookup row.
     * @param ceil If we can remove ceil row when we can not find exact.
     * @return Removed row.
     * @throws IgniteCheckedException If failed.
     */
    public final T doRemove(L row, boolean ceil) throws IgniteCheckedException {
        Remove r = new Remove(row, ceil);

        try {
            for (;;) {
                r.initOperation();

                switch (removeDown(r, r.rootId, 0L, 0L, r.rootLvl)) {
                    case Remove.RETRY:
                    case Remove.RETRY_ROOT:
                        checkInterrupted();

                        continue;

                    default:
                        if (!r.isFinished())
                            r.finishTail(true);

                        assert r.isFinished();

                        return r.removed;
                }
            }
        }
        finally {
            r.releaseTail();
            r.releaseMeta();

            r.reuseEmptyPages();
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
    private int removeDown(final Remove r, final long pageId, final long backId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        assert lvl >= 0 : lvl;

        if (r.isTail(pageId, lvl))
            return Remove.FOUND; // We've already locked this page, so return that we are ok.

        final Page page = page(pageId);

        try {
            for (;;) {
                // Init args.
                r.pageId = pageId;
                r.fwdId = fwdId;

                int res = readPage(page, search, r, lvl, Remove.RETRY);

                switch (res) {
                    case Remove.GO_DOWN:
                        res = removeDown(r, r.pageId, r.backId, r.fwdId, lvl - 1);

                        switch (res) {
                            case Remove.RETRY:
                                checkInterrupted();

                                continue;

                            case Remove.RETRY_ROOT:
                                return res;
                        }

                        if (!r.isFinished() && !r.finishTail(false))
                            return r.lockTail(page, backId, fwdId, lvl);

                        return res;

                    case Remove.NOT_FOUND:
                        // We are at the bottom.
                        assert lvl == 0: lvl;

                        if (!r.ceil) {
                            r.finish();

                            return res;
                        }

                        // Intentional fallthrough for ceiling remove.

                    case Remove.FOUND:
                        // We must be at the bottom here, just need to remove row from the current page.
                        assert lvl == 0 : lvl;
                        assert r.removed == null;

                        res = r.removeFromLeaf(page, backId, fwdId);

                        if (res == Remove.NOT_FOUND) {
                            assert r.ceil: "must be a retry if not a ceiling remove";

                            r.finish(); // TODO may be try to remove from forward
                        }
                        else if (res == Remove.FOUND && r.needReplaceInner == FALSE && r.needMerge == FALSE) {
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

            if (r.canRelease(page, lvl))
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
     public int randomInt(int max) {
         return ThreadLocalRandom.current().nextInt(max);
     }

    /**
     * @param cur Current tail element.
     * @param fwdCnt Count in forward page.
     * @return Count after merge or {@code -1} if merge is impossible.
     */
    private int countAfterMerge(Tail cur, int fwdCnt) {
        int cnt = cur.io.getCount(cur.buf);

        int newCnt = cnt + fwdCnt;

        if (cur.lvl != 0)
            newCnt++; // We have to move down split key in inner pages.

        if (newCnt <= cur.io.getMaxCount(cur.buf))
            return newCnt;

        return -1;
    }

    /**
     * @param row Row.
     * @return Old row.
     * @throws IgniteCheckedException If failed.
     */
    public final T put(T row) throws IgniteCheckedException {
        Put p = new Put(row);

        try {
            for (;;) { // Go down with retries.
                p.initOperation();

                switch (putDown(p, p.rootId, 0L, p.rootLvl)) {
                    case Put.RETRY:
                    case Put.RETRY_ROOT:
                        checkInterrupted();

                        continue;

                    default:
                        assert p.isFinished();

                        return p.oldRow;
                }
            }
        }
        finally {
            p.releaseMeta();
        }
    }

    /**
     * @param io IO.
     * @param buf Splitting buffer.
     * @param fwdBuf Forward buffer.
     * @throws IgniteCheckedException If failed.
     */
    private void splitPage(BPlusIO io, ByteBuffer buf, ByteBuffer fwdBuf)
        throws IgniteCheckedException {
        int cnt = io.getCount(buf);
        int mid = 1 + (cnt >>> 1);

        cnt -= mid;

        io.copyItems(buf, fwdBuf, mid, 0, cnt, true);

        // Setup counts.
        io.setCount(fwdBuf, cnt);
        io.setCount(buf, mid);

        // Setup forward-backward refs.
        io.setForward(fwdBuf, io.getForward(buf));
        io.setForward(buf, PageIO.getPageId(fwdBuf));
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param row Row.
     * @param idx Index.
     * @param rightId Right page ID.
     * @throws IgniteCheckedException If failed.
     */
    private void insertSimple(BPlusIO<L> io, ByteBuffer buf, L row, int idx, long rightId)
        throws IgniteCheckedException {
        int cnt = io.getCount(buf);

        // Move right all the greater elements to make a free slot for a new row link.
        io.copyItems(buf, buf, idx, idx + 1, cnt - idx, false);

        io.setCount(buf, cnt + 1);
        io.store(buf, idx, row);

        if (!io.isLeaf()) // Setup reference to the right page on split.
            inner(io).setRight(buf, idx, rightId);
    }

    /**
     * @param meta Meta page.
     * @param io IO.
     * @param buf Buffer.
     * @param row Row.
     * @param idx Index.
     * @param rightId Right page ID after split.
     * @param lvl Level.
     * @return Move up row.
     * @throws IgniteCheckedException If failed.
     */
    private L insertWithSplit(Page meta, BPlusIO<L> io, final ByteBuffer buf, L row,
        int idx, long rightId, int lvl) throws IgniteCheckedException {
        try (Page fwd = allocatePage()) {
            // Need to check this before the actual split, because after the split we will have new forward page here.
            boolean hadFwd = io.getForward(buf) != 0;

            ByteBuffer fwdBuf = fwd.getForInitialWrite();
            io.initNewPage(fwdBuf, fwd.id());

            splitPage(io, buf, fwdBuf);

            // Do insert.
            int cnt = io.getCount(buf);

            if (idx <= cnt) {
                insertSimple(io, buf, row, idx, rightId);

                // Fix leftmost child of forward page, because newly inserted row will go up.
                if (idx == cnt && !io.isLeaf())
                    inner(io).setLeft(fwdBuf, 0, rightId);
            }
            else
                insertSimple(io, fwdBuf, row, idx - cnt, rightId);

            // Do move up.
            cnt = io.getCount(buf);

            L moveUpRow = io.getLookupRow(this, buf, cnt - 1); // Last item from backward row goes up.

            if (!io.isLeaf()) // Leaf pages must contain all the links, inner pages remove moveUpLink.
                io.setCount(buf, cnt - 1);

            if (!hadFwd && lvl == getRootLevel(meta)) { // We are splitting root.
                long newRootId;

                try (Page newRoot = allocatePage()) {
                    newRootId = newRoot.id();

                    if (io.isLeaf())
                        io = latestInnerIO();

                    ByteBuffer newRootBuf = newRoot.getForInitialWrite();

                    io.initNewPage(newRootBuf, newRoot.id());

                    io.setCount(newRootBuf, 1);
                    inner(io).setLeft(newRootBuf, 0, PageIO.getPageId(buf));
                    io.store(newRootBuf, 0, moveUpRow);
                    inner(io).setRight(newRootBuf, 0, fwd.id());
                }

                int res = writePage(meta, updateRoot, newRootId, lvl + 1, FALSE);

                assert res == TRUE : "failed to update meta page";

                return null; // We've just moved link up to root, nothing to return here.
            }

            // Regular split.
            return moveUpRow;
        }
    }

    /**
     * @param meta Meta page.
     * @param io IO.
     * @param buf Buffer.
     * @param row Row.
     * @param idx Index.
     * @param rightId Right ID.
     * @param lvl Level.
     * @return Move up row.
     * @throws IgniteCheckedException If failed.
     */
    private L insert(Page meta, BPlusIO<L> io, ByteBuffer buf, L row, int idx, long rightId, int lvl)
        throws IgniteCheckedException {
        int maxCnt = io.getMaxCount(buf);
        int cnt = io.getCount(buf);

        if (cnt == maxCnt) // Need to split page.
            return insertWithSplit(meta, io, buf, row, idx, rightId, lvl);

        insertSimple(io, buf, row, idx, rightId);

        return null;
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
     * @return Leftmost child page ID.
     */
    private long getLeftmostChild(long pageId) throws IgniteCheckedException {
        try (Page page = page(pageId)) {
            assert page != null : "we've locked back page, forward can't be merged";

            ByteBuffer buf = page.getForRead();

            try {
                BPlusIO<L> io = io(buf);

                assert io.getCount(buf) >= 0; // Count can be 0 if it is a routing page.

                long res = inner(io).getLeft(buf, 0);

                assert res != 0: "inner page with no route down: " + page.fullId();

                return res;
            }
            finally {
                page.releaseRead();
            }
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
    private int putDown(final Put p, final long pageId, final long fwdId, final int lvl)
        throws IgniteCheckedException {
        assert lvl >= 0 : lvl;

        final Page page = page(pageId);

        try {
            for (;;) {
                // Init args.
                p.pageId = pageId;
                p.fwdId = fwdId;

                int res = readPage(page, search, p, lvl, Put.RETRY);

                switch (res) {
                    case Put.GO_DOWN:
                        assert lvl > 0 : lvl;
                        assert p.pageId != pageId;
                        assert p.fwdId != fwdId || fwdId == 0;

                        if (p.foundInner == TRUE) { // Need to replace ref in inner page.
                            p.foundInner = FALSE; // Protect from retries.

                            res = writePage(page, replace, p, lvl, Put.RETRY);

                            if (res != Put.FOUND)
                                return res; // Need to retry.

                            p.foundInner = DONE; // We can have only single matching inner key.
                        }

                        // Go down recursively.
                        res = putDown(p, p.pageId, p.fwdId, lvl - 1);

                        if (res == Put.RETRY_ROOT || p.isFinished())
                            return res;

                        checkInterrupted();

                        continue; // We have to insert split row to this level or it is a retry.

                    case Put.FOUND: // Do replace.
                        assert lvl == 0 : "This replace can happen only at the bottom level.";

                        // Init args.
                        p.pageId = pageId;
                        p.fwdId = fwdId;

                        return writePage(page, replace, p, lvl, Put.RETRY);

                    case Put.NOT_FOUND: // Do insert.
                        assert lvl == p.btmLvl : "must insert at the bottom level";
                        assert p.foundInner == FALSE: p.foundInner;

                        // Init args.
                        p.pageId = pageId;
                        p.fwdId = fwdId;

                        return writePage(page, insert, p, lvl, Put.RETRY);

                    default:
                        return res;
                }
            }
        }
        finally{
            if (p.canRelease(page, lvl))
                page.close();
        }
    }

    /**
     * Get operation.
     */
    private abstract class Get {
        /** */
        static final int GO_DOWN = 1;

        /** */
        static final int RETRY = 2;

        /** */
        static final int RETRY_ROOT = 3;

        /** */
        static final int NOT_FOUND = 4;

        /** */
        static final int FOUND = 5;

        /** */
        long rmvId;

        /** Starting point root level. May be outdated. Must be modified only in {@link Get#initOperation()}. */
        int rootLvl;

        /** Starting point root ID. May be outdated. Must be modified only in {@link Get#initOperation()}. */
        long rootId;

        /** Meta page. Initialized by {@link Get#initOperation()}, released by {@link Get#releaseMeta()}. */
        Page meta;

        /** */
        L row;

        /** In/Out parameter: Page ID. */
        long pageId;

        /** In/Out parameter: expected forward page ID. */
        long fwdId;

        /** In/Out parameter: in case of right turn this field will contain backward page ID for the child. */
        long backId;

        /**
         * @param row Row.
         */
        public Get(L row) {
            assert row != null;

            this.row = row;
        }

        /**
         * Initialize the given operation.
         *
         * !!! Symmetrically with this method must be called {@link Get#releaseMeta()} in {@code finally} block.
         *
         * @throws IgniteCheckedException If failed.
         */
        final void initOperation() throws IgniteCheckedException {
            if (meta == null)
                meta = page(metaPageId);

            int rootLvl;
            long rootId;

            ByteBuffer buf = meta.getForRead();

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(buf);

                rootLvl = io.getRootLevel(buf);
                rootId = io.getLeftmostPageId(buf, rootLvl);
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
         * @param page Page.
         * @param lvl Level.
         * @return {@code true} If we can release the given page.
         */
        boolean canRelease(Page page, int lvl) {
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
        ForwardCursor cursor;

        /**
         * @param lower Lower bound.
         * @param upper Upper bound.
         */
        private GetCursor(L lower, L upper) {
            super(lower);

            cursor = new ForwardCursor(upper);
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            if (!io.isLeaf())
                return false;

            cursor.bootstrap(buf, idx);

            return true;
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            assert lvl >= 0 : lvl;

            if (lvl != 0)
                return false;

            assert io.isLeaf();

            cursor.bootstrap(buf, idx);

            return true;
        }
    }

    /**
     * Put operation.
     */
    private final class Put extends Get {
        /** Right child page ID for split row. */
        long rightId;

        /** Replaced row if any. */
        T oldRow;

        /**
         * This page is kept locked after split until insert to the upper level will not be finished.
         * It is needed because split row will be "in flight" and if we'll release tail, remove on
         * split row may fail.
         */
        Page tail;

        /**
         * Bottom level for insertion (insert can't go deeper). Will be incremented on split on each level.
         */
        short btmLvl;

        /** */
        byte foundInner = FALSE;

        /**
         * @param row Row.
         */
        private Put(T row) {
            super(row);
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            if (io.isLeaf())
                return true;

            // If we can get full row from the inner page, we must do inner replace to update full row info here.
            if (io.canGetRow() && foundInner == FALSE)
                foundInner = TRUE;

            return false;
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            assert btmLvl >= 0 : btmLvl;
            assert lvl >= btmLvl : lvl;

            return lvl == btmLvl;
        }

        /**
         * @param tail Tail lock.
         */
        private void tail(Page tail) {
            if (this.tail != null)
                writeUnlockAndClose(this.tail);

            this.tail = tail;
        }

        /** {@inheritDoc} */
        @Override boolean canRelease(Page page, int lvl) {
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
    }

    /**
     * Remove operation.
     */
    private final class Remove extends Get {
        /** */
        boolean ceil;

        /** We may need to lock part of the tree branch from the bottom to up for multiple levels. */
        Tail<L> tail;

        /** */
        List<FullPageId> emptyPages; // TODO May be use Object for single empty page

        /** */
        byte needReplaceInner = FALSE;

        /** */
        byte needMerge = FALSE;

        /** Removed row. */
        T removed;

        /** Current page. */
        Page page;

        /** */
        short innerIdx = Short.MIN_VALUE;

        /**
         * @param row Row.
         * @param ceil If we can remove ceil row when we can not find exact.
         */
        private Remove(L row, boolean ceil) {
            super(row);

            this.ceil = ceil;
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            if (io.isLeaf())
                return true;

            // If we can get full row from the inner page, we must do inner replace to update full row info here.
            if (io.canGetRow() && needReplaceInner == FALSE)
                needReplaceInner = TRUE;

            return false;
        }

        /** {@inheritDoc} */
        @Override boolean notFound(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
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
         * Process tail and finish.
         *
         * @param ignoreMergeMore Ignore the attempt to merge more pages up.
         * @return {@code false} If failed to finish and we need to lock more pages up.
         * @throws IgniteCheckedException If failed.
         */
        private boolean finishTail(boolean ignoreMergeMore) throws IgniteCheckedException {
            assert !isFinished();
            assert needMerge != FALSE || needReplaceInner != FALSE;
            assert tail != null;

            if (needReplaceInner == READY) {
                assert needMerge == FALSE: needMerge;
                assert getTail(0) != null: "we must keep lock on the leaf page";

                // We increment remove ID in write lock on leaf page, thus it is guaranteed that
                // any successor will get greater value than he had read at the beginning of the operation.
                // Thus it will be guaranteed to do a retry from root.
                globalRmvId.incrementAndGet(); // TODO replace with pageId bit patching

                // Need to replace inner key with new max key for the left subtree.
                doReplaceInner();

                needReplaceInner = DONE;
            }
            else if (needMerge == READY) {
                assert needReplaceInner == FALSE: needReplaceInner;
                assert tail.down != null;

                boolean needMergeMore = merge(tail.lvl - 1, true);

                if (needMergeMore && !ignoreMergeMore) {
                    needMerge = TRUE;

                    return false;
                }

                needMerge = DONE;
            }
            else
                return false;

            releaseTail();
            finish();

            return true;
        }

        /**
         * @param leaf Leaf page.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private int removeFromLeaf(Page leaf, long backId, long fwdId) throws IgniteCheckedException {
            // Init parameters.
            this.pageId = leaf.id();
            this.page = leaf;
            this.backId = backId;
            this.fwdId = fwdId;

            if (backId == 0)
                return doRemoveFromLeaf();

            // Lock back page before the remove, we'll need it for merges.
            Page back = page(backId);

            try {
                return writePage(back, lockBackAndRemoveFromLeaf, this, 0, Remove.RETRY);
            }
            finally {
                if (canRelease(back, 0))
                    back.close();
            }
        }


        /**
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private int doRemoveFromLeaf() throws IgniteCheckedException {
            assert page != null;

            return writePage(page, removeFromLeaf, this, 0, Remove.RETRY);
        }

        /**
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private int doLockTail(int lvl) throws IgniteCheckedException {
            assert page != null;

            return writePage(page, lockTail, this, lvl, Remove.RETRY);
        }

        /**
         * @param page Page.
         * @param backId Back page ID.
         * @param fwdId Expected forward page ID.
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        private int lockTail(Page page, long backId, long fwdId, int lvl) throws IgniteCheckedException {
            assert needMerge == TRUE ^ needReplaceInner == TRUE: "we can do only one thing at once";

            // Init parameters for the handlers.
            this.pageId = page.id();
            this.page = page;
            this.fwdId = fwdId;
            this.backId = backId;

            if (backId == 0) // Back page ID is provided only when the last move was to the right.
                return doLockTail(lvl);

            Page back = page(backId);

            try {
                return writePage(back, lockBackAndTail, this, lvl, Remove.RETRY);
            }
            finally {
                if (canRelease(back, lvl))
                    back.close();
            }
        }

        /**
         * @param lvl Level.
         * @throws IgniteCheckedException If failed.
         */
        private void lockForward(int lvl) throws IgniteCheckedException {
            assert fwdId != 0;
            assert backId == 0;

            int res = writePage(page(fwdId), lockTailForward, this, lvl, Remove.RETRY);

            // Must always be called from lock on back page, thus we should never fail here.
            assert res == Remove.FOUND: res;
        }

        /**
         * @param io IO.
         * @param buf Buffer.
         * @param cnt Count.
         * @param idx Index to remove.
         * @param lvl Level.
         * @param kickLeftChild If we are dropping left child instead of the right one.
         * @throws IgniteCheckedException If failed.
         */
        private void doRemove(BPlusIO io, Page page, ByteBuffer buf, int cnt, int idx, int lvl, boolean kickLeftChild)
            throws IgniteCheckedException {
            assert cnt > 0;
            assert idx >= 0;
            assert idx <= cnt;

            if (idx == cnt) {
                idx--; // This may happen in case of right turn, we need to remove the rightmost ref and link.

                assert !kickLeftChild: "right child must be dropped here";
            }

            cnt--;

            io.copyItems(buf, buf, idx + 1, idx, cnt - idx, kickLeftChild);
            io.setCount(buf, cnt);

            if (cnt == 0 && lvl != 0 && getRootLevel(meta) == lvl)
                freePage(page, buf, io, lvl); // Free root.
        }

        /**
         * @param prnt Parent tail.
         * @param cur Current tail.
         * @param fwd Forward tail.
         * @throws IgniteCheckedException If failed.
         */
        private boolean mergePages(Tail<L> prnt, Tail<L> cur, Tail<L> fwd) throws IgniteCheckedException {
            assert fwd.io == cur.io; // Otherwise incompatible.

            int cnt = cur.io.getCount(cur.buf);
            int fwdCnt = fwd.io.getCount(fwd.buf);
            int newCnt = countAfterMerge(cur, fwdCnt);

            if (newCnt == -1) // Not enough space.
                return false;

            cur.io.setCount(cur.buf, newCnt);

            int prntCnt = prnt.io.getCount(prnt.buf);

            // Move down split key in inner pages.
            if (cur.lvl != 0) {
                int prntIdx = prnt.idx;

                if (prntIdx == prntCnt) // It was a right turn.
                    prntIdx--;

                // We can be sure that we have enough free space to store split key here,
                // because we've done remove already and did not release child locks.
                inner(cur.io).store(cur.buf, cnt, prnt.io, prnt.buf, prntIdx);

                cnt++;
            }

            cur.io.copyItems(fwd.buf, cur.buf, 0, cnt, fwdCnt, true);
            cur.io.setForward(cur.buf, fwd.io.getForward(fwd.buf));

            assert prntCnt > 0: prntCnt;

            // Remove split key from parent. If parent is root and becomes empty, it will be freed by doRemove.
            doRemove(prnt.io, prnt.page, prnt.buf, prntCnt, prnt.idx, prnt.lvl, false);

            // Forward page is now empty and has no links.
            freePage(fwd.page, fwd.buf, fwd.io, fwd.lvl);

            return true;
        }

        /**
         * @param page Page.
         * @param buf Buffer.
         * @param io IO.
         * @param lvl Level.
         * @throws IgniteCheckedException If failed.
         */
        private void freePage(Page page, ByteBuffer buf, BPlusIO io, int lvl)
            throws IgniteCheckedException {
            if (getLeftmostPageId(meta, lvl) == page.id()) {
                // This logic will handle root as well.
                long fwdId = io.getForward(buf);

                writePage(meta, updateLeftmost, fwdId, lvl, FALSE);
            }

            // Mark removed.
            io.setRemoveId(buf, Long.MAX_VALUE);

            if (reuseList == null)
                return; // We are not allowed to reuse pages.

            // We will reuse empty page.
            if (emptyPages == null)
                emptyPages = new ArrayList<>(4);

            emptyPages.add(page.fullId());
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void reuseEmptyPages() throws IgniteCheckedException {
            if (emptyPages != null) {
                for (int i = 0; i < emptyPages.size(); i++)
                    reuseList.put(BPlusTree.this, emptyPages.get(i));
            }
        }

        /**
         * @param inner Inner replace page.
         * @throws IgniteCheckedException If failed.
         */
        private void dropEmptyBranch(Tail inner) throws IgniteCheckedException {
            int cnt = inner.io.getCount(inner.buf);

            assert cnt > 0: cnt;

            // We need to check if the branch we are going to drop goes to the left or to the right.
            boolean kickLeft = inner.down.page.id() == inner(inner.io).getLeft(inner.buf, inner.idx);

            assert kickLeft || inner.down.page.id() == inner(inner.io).getRight(inner.buf, inner.idx);

            // Remove found inner key from inner page.
            doRemove(inner.io, inner.page, inner.buf, cnt, inner.idx, inner.lvl, kickLeft);

            // If inner page was root and became empty, it was handled in doRemove.
            // Otherwise we can be sure that inner page was not freed, at lead it must become
            // an empty routing page. Thus always starting from inner.down here.
            for (Tail t = inner.down; t != null; t = t.down) {
                // TODO merge branch but not just free it

                assert t.io.getCount(t.buf) == 0: row;

                freePage(t.page, t.buf, t.io, t.lvl);
            }
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void doReplaceInner() throws IgniteCheckedException {
            assert needReplaceInner == READY: needReplaceInner;
            assert tail.lvl > 0;
            assert innerIdx >= 0;

            Tail<L> leaf = getTail(0);
            Tail<L> inner = tail;

            assert inner.type == Tail.EXACT: inner.type;
            assert innerIdx < inner.io.getCount(inner.buf);

            int cnt = leaf.io.getCount(leaf.buf);

            if (cnt == 0) { // Merge empty leaf page.
                if (!merge(0, false)) {
                    // For leaf pages this can happen only when parent is empty -> drop the whole branch.
                    dropEmptyBranch(inner);

                    return;
                }

                // Need to handle possible tail restructuring after merge.
                leaf = getTail(0);

                cnt = leaf.io.getCount(leaf.buf);

                // If any leaf becomes empty we have to either merge it or drop the whole empty branch.
                assert cnt > 0: "leaf can't be empty after successful merge";
            }

            // If after leaf merge parent have lost inner key, we don't need to update it anymore.
            if (innerIdx < inner.io.getCount(inner.buf)) {
                inner(inner.io).store(inner.buf, innerIdx, leaf.io, leaf.buf, cnt - 1);
                leaf.io.setRemoveId(leaf.buf, globalRmvId.get());
            }
            else {
                assert innerIdx == inner.io.getCount(inner.buf);
                assert inner(inner.io).getLeft(inner.buf, innerIdx) == leaf.page.id();
            }

            // We can't merge the whole branch up here because of locking rules.
            // Here we've already locked the whole branch from the bottom to the top.
        }

        /**
         * @param lvl Level.
         * @return {@code true} If merged, {@code false} if not (because of insufficient space).
         * @throws IgniteCheckedException If failed.
         */
        private boolean merge(int lvl, boolean releaseMerged) throws IgniteCheckedException {
            assert tail.lvl > lvl;

            Tail<L> prnt = getTail(lvl + 1);

            if (prnt.io.getCount(prnt.buf) == 0)
                return false; // Parent is an empty routing page, child forward page will have another parent.

            Tail<L> cur = getTail(lvl);
            Tail<L> back = cur.sibling;

            assert prnt.down == cur : prnt.down;
            assert back != null: "we must have a partner to merge with";

            if (back.type != Tail.BACK) { // Flip if it was actually FORWARD but not BACK.
                assert back.type == Tail.FORWARD: back.type;

                back = cur; // Current goes back.
                cur = cur.sibling; // Forward becomes current.
            }

            assert cur.io == back.io: "must always be the same"; // Otherwise can be not compatible.

            if (!mergePages(prnt, back, cur))
                return false;

            // BACK becomes EXACT.
            if (back.type == Tail.BACK) {
                assert back.sibling == null;

                back.down = cur.down;
                back.type = Tail.EXACT;
                prnt.down = back;
            }
            else { // back is already EXACT
                assert back.type == Tail.EXACT: back.type;

                back.sibling = null;
            }

            // Always unlock and release current because we made it invisible for further code.
            writeUnlockAndClose(cur.page);

            if (releaseMerged) {
                prnt.down = null;

                writeUnlockAndClose(back.page);
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
            Tail t = tail;

            tail = null;

            while (t != null) {
                writeUnlockAndClose(t.page);

                if (t.sibling != null)
                    writeUnlockAndClose(t.sibling.page);

                t = t.down;
            }
        }

        /** {@inheritDoc} */
        @Override boolean canRelease(Page page, int lvl) {
            return page != null && !isTail(page.id(), lvl);
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
                    if (t.page.id() == pageId)
                        return true;

                    t = t.sibling;

                    return t != null && t.page.id() == pageId;
                }

                t = t.down;
            }

            return false;
        }

        /**
         * @param page Page.
         * @param buf Buffer.
         * @param io IO.
         * @param lvl Level.
         * @param type Type.
         * @param idx Insertion index or negative flag describing if the page is primary in this tail branch.
         */
        private void addTail(Page page, ByteBuffer buf, BPlusIO<L> io, int lvl, byte type, int idx) {
            Tail<L> t = new Tail<>(page, buf, io, type, lvl, idx);

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
                    assert tail.type == Tail.EXACT: tail.type;

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
            assert lvl >= 0 && lvl <= tail.lvl: lvl;

            Tail<L> t = tail;

            while (t.lvl != lvl)
                t = t.down;

            assert t.type == Tail.EXACT: t.type; // All the down links must be of EXACT type.

            return t;
        }
    }

    /**
     * Tail for remove.
     */
    private static class Tail<L> {
        /** */
        static final byte BACK = 0;

        /** */
        static final byte EXACT = 1;

        /** */
        static final byte FORWARD = 2;

        /** */
        final Page page;

        /** */
        final ByteBuffer buf;

        /** */
        final BPlusIO<L> io;

        /** */
        byte type;

        /** */
        final byte lvl;

        /** */
        final short idx;

        /** Only {@link #EXACT} tail can have either {@link #BACK} or {@link #FORWARD} sibling.*/
        Tail<L> sibling;

        /** Only {@link #EXACT} tail can point to {@link #EXACT} tail of lower level. */
        Tail<L> down;

        /**
         * @param page Write locked page.
         * @param buf Buffer.
         * @param io IO.
         * @param type Type.
         * @param lvl Level.
         * @param idx Insertion index.
         */
        private Tail(Page page, ByteBuffer buf, BPlusIO<L> io, byte type, int lvl, int idx) {
            assert type == BACK || type == EXACT || type == FORWARD: type;
            assert idx == Integer.MIN_VALUE || (idx >= 0 && idx <= Short.MAX_VALUE): idx ;
            assert lvl >= 0 && lvl <= Byte.MAX_VALUE: lvl;
            assert page != null;

            this.page = page;
            this.buf = buf;
            this.io = io;
            this.type = type;
            this.lvl = (byte)lvl;
            this.idx = (short)idx;
        }
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param cnt Row count.
     * @param row Lookup row.
     * @return Insertion point as in {@link Arrays#binarySearch(Object[], Object, Comparator)}.
     */
    private int findInsertionPoint(BPlusIO<L> io, ByteBuffer buf, int cnt, L row)
        throws IgniteCheckedException {
        assert row != null;

        int low = 0;
        int high = cnt - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            int cmp = compare(io, buf, mid, row);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // found
        }

        return -(low + 1);  // not found
    }

    /**
     * @param buf Buffer.
     * @return IO.
     */
    private BPlusIO<L> io(ByteBuffer buf) {
        assert buf != null;

        int type = PageIO.getType(buf);
        int ver = PageIO.getVersion(buf);

        return io(type, ver);
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
        return pageMem.page(new FullPageId(pageId, cacheId));
    }

    /**
     * @return Allocated page.
     */
    private Page allocatePage() throws IgniteCheckedException {
        FullPageId pageId = null;

        if (reuseList != null)
            pageId = reuseList.take(this);

        if (pageId == null)
            pageId = pageMem.allocatePage(cacheId, -1, PageIdAllocator.FLAG_IDX);

        return pageMem.page(pageId);
    }

    /**
     * @param type Page type.
     * @param ver Page version.
     * @return IO.
     */
    protected abstract BPlusIO<L> io(int type, int ver);

    /**
     * @return Latest version of inner page IO.
     */
    protected abstract BPlusInnerIO<L> latestInnerIO();

    /**
     * @return Latest version of leaf page IO.
     */
    protected abstract BPlusLeafIO<L> latestLeafIO();

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
    protected class ForwardCursor implements GridCursor<T> {
        /** */
        private List<T> rows;

        /** */
        private int row;

        /** */
        private Page page;

        /** */
        private ByteBuffer buf;

        /** */
        private final L upperBound;

        /**
         * @param upperBound Upper bound.
         */
        protected ForwardCursor(L upperBound) {
            this.upperBound = upperBound;
        }

        /**
         * @param buf Buffer.
         * @param startIdx Start index.
         */
        void bootstrap(ByteBuffer buf, int startIdx) throws IgniteCheckedException {
            assert buf != null;

            row = -1;

            this.buf = buf;

            fillFromBuffer(startIdx);
        }

        /**
         * @param startIdx Start index.
         */
        private boolean fillFromBuffer(int startIdx) throws IgniteCheckedException {
            if (buf == null)
                return false;

            for (;;) {
                BPlusIO<L> io = io(buf);

                assert io.isLeaf();
                assert io.canGetRow();

                int cnt = io.getCount(buf);
                long fwdId = io.getForward(buf);

                if (cnt > 0) {
                    if (upperBound != null) {
                        int cmp = compare(io, buf, cnt - 1, upperBound);

                        if (cmp > 0) {
                            cnt = findInsertionPoint(io, buf, cnt, upperBound) + 1;

                            fwdId = 0; // The End.
                        }
                    }
                }

                if (cnt > startIdx) {
                    if (rows == null)
                        rows = new ArrayList<>();

                    for (int i = startIdx; i < cnt; i++)
                        rows.add(getRow(io, buf, i));
                }

                Page prevPage = page;

                if (fwdId != 0) { // Lock next page.
                    page = page(fwdId);
                    buf = page.getForRead();
                }
                else { // Clear.
                    page = null;
                    buf = null;
                }

                if (prevPage != null) { // Release previous page.
                    try {
                        prevPage.releaseRead();
                    }
                    finally {
                        prevPage.close();
                    }
                }

                if (F.isEmpty(rows)) {
                    if (buf == null)
                        return false;

                    continue;
                }

                return true;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean next() throws IgniteCheckedException {
            if (rows == null)
                return false;

            if (++row < rows.size())
                return true;

            row = 0;
            rows.clear();

            return fillFromBuffer(0);
        }

        /** {@inheritDoc} */
        @Override public T get() {
            return rows.get(row);
        }
    }

    /**
     * Page handler for basic {@link Get} operation.
     */
    private abstract class GetPageHandler<G extends Get> extends PageHandler<G> {
        /** {@inheritDoc} */
        @Override public final int run(Page page, ByteBuffer buf, G g, int lvl) throws IgniteCheckedException {
            BPlusIO<L> io = io(buf);

            // In case of intersection with inner replace remove operation
            // we need to restart our operation from the root.
            if (g.rmvId < io.getRemoveId(buf))
                return Get.RETRY_ROOT;

            return run0(page, buf, io, g, lvl);
        }

        /**
         * @param page Page.
         * @param buf Buffer.
         * @param io IO.
         * @param g Operation.
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteCheckedException If failed.
         */
        protected abstract int run0(Page page, ByteBuffer buf, BPlusIO<L> io, G g, int lvl)
            throws IgniteCheckedException;

        /** {@inheritDoc} */
        @Override public final boolean releaseAfterWrite(Page page, G g, int lvl) {
            return g.canRelease(page, lvl);
        }
    }
}
