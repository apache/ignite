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

package org.apache.ignite.internal.processors.query.h2.database;

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
import org.apache.ignite.internal.processors.query.h2.database.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.database.io.BPlusIOInner;
import org.apache.ignite.internal.processors.query.h2.database.io.BPlusIOLeaf;
import org.apache.ignite.internal.processors.query.h2.database.io.BPlusIOMeta;
import org.apache.ignite.internal.processors.query.h2.database.io.PageIO;
import org.apache.ignite.internal.processors.query.h2.database.util.PageHandler;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridTreePrinter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.query.h2.database.util.PageHandler.readPage;
import static org.apache.ignite.internal.processors.query.h2.database.util.PageHandler.writePage;

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
    private final float minFill;

    /** */
    private final float maxFill;

    /** */
    private final long metaPageId;

    /** */
    private final AtomicLong globalRmvId = new AtomicLong(U.currentTimeMillis() * 1000_000); // TODO init from WAL?

    /** */
    private final DataStore<T> dataStore;

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
            T newRow = p.row();

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
            T moveUpRow = insert(p.meta, io, buf, p.row(), idx, p.rightId, lvl);

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

            if (idx < 0)
                return Remove.RETRY;

            r.removed = getRow(io, buf, idx);

            doRemove(io, leaf, buf, cnt, idx, r.meta, lvl, false);

            // We may need to replace inner key or want to merge this leaf with sibling after the remove -> keep lock.
            if (r.needReplaceInner == TRUE ||
                // We need to make sure that we have back or forward to be able to merge.
                ((r.fwdId != 0 || r.backId != 0) && mayMerge(--cnt, io.getMaxCount(buf)))) {
                r.addTail(leaf, buf, io, 0, false, Integer.MIN_VALUE);

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
            int res = doRemoveFromLeaf(r);

            // If we need to do more tricks, then we have to keep locks on back and leaf pages.
            if (res == Remove.FOUND && (r.needMerge == TRUE || r.needReplaceInner == TRUE)) {
                assert r.needMerge == TRUE ^ r.needReplaceInner == TRUE: "we can do only one thing at once";

                r.addTail(back, buf, io, lvl, true, Integer.MIN_VALUE);
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
            int res = doLockTail(r, lvl);

            if (res == Remove.FOUND)
                r.addTail(back, buf, io, lvl, true, Integer.MIN_VALUE);

            return res;
        }
    };

    /** */
    private final PageHandler<Remove> lockTail = new GetPageHandler<Remove>() {
        @Override public int run0(Page page, ByteBuffer buf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, r.row);

            boolean found = idx >= 0;

            if (found) {
                if (lvl != 0) {
                    assert r.needReplaceInner == TRUE : r.needReplaceInner;
                    assert idx <= Short.MAX_VALUE : idx;

                    r.innerIdx = (short)idx;

                    r.needReplaceInner = READY;
                }
            }
            else {
                idx = -idx - 1;

                // Check that we have a correct view of the world.
                if (idx == cnt && io.getForward(buf) != r.fwdId)
                    return Remove.RETRY;
            }

            // Check that we have a correct view of the world.
            if (lvl != 0 && inner(io).getLeft(buf, idx) != r.nonBackTailPage().id()) {
                assert !found;

                return Remove.RETRY;
            }

            r.addTail(page, buf, io, lvl, false, idx);

            if (r.needMerge == TRUE) {
                assert r.needReplaceInner == FALSE;
                assert r.tail.down != null;

                r.needMerge = READY;
            }

            return Remove.FOUND;
        }
    };

    /** */
    private final PageHandler<Remove> mergePages = new GetPageHandler<Remove>() {
        @Override protected int run0(Page fwd, ByteBuffer fwdBuf, BPlusIO<L> io, Remove r, int lvl)
            throws IgniteCheckedException {
            Tail<L> prnt = r.getTail(lvl + 1, false);

            assert prnt != null;

            Tail<L> t = r.getTail(lvl, false);

            assert t.io == io : "must be the same"; // Otherwise may be not compatible.

            return mergePages(r.meta, prnt, t, fwd, fwdBuf) ? TRUE : FALSE;
        }
    };

    /** */
    private final PageHandler<Long> updateLeftmost = new PageHandler<Long>() {
        @Override public int run(Page page, ByteBuffer buf, Long pageId, int lvl) throws IgniteCheckedException {
            assert pageId != null;

            BPlusIOMeta io = BPlusIOMeta.VERSIONS.forPage(buf);

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
            BPlusIOMeta io = BPlusIOMeta.VERSIONS.forPage(buf);

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
     * @param dataStore Data store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public BPlusTree(
        DataStore<T> dataStore,
        FullPageId metaPageId,
        boolean initNew
    ) throws IgniteCheckedException {
        assert dataStore != null;

        this.dataStore = dataStore;

        // TODO make configurable: 0 <= minFill <= maxFill <= 1
        minFill = 0f; // Testing worst case when merge happens only on empty page.
        maxFill = 0f; // Avoiding random effects on testing.

        this.metaPageId = metaPageId.pageId();

        if (initNew) { // Init new index.
            try (Page meta = page(this.metaPageId)) {
                ByteBuffer buf = meta.getForInitialWrite();

                BPlusIOMeta io = BPlusIOMeta.VERSIONS.latest();

                io.initNewPage(buf, metaPageId.pageId());

                try (Page root = allocatePage()) {
                    latestLeafIO().initNewPage(root.getForInitialWrite(), root.id());

                    io.setLevelsCount(buf, 1);
                    io.setLeftmostPageId(buf, 0, root.id());
                }
            }
        }
    }

    /**
     * @return Root level.
     */
    private int getRootLevel(Page meta) {
        ByteBuffer buf = meta.getForRead();

        try {
            return BPlusIOMeta.VERSIONS.forPage(buf).getRootLevel(buf);
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
            BPlusIOMeta io = BPlusIOMeta.VERSIONS.forPage(buf);

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
    public GridCursor<T> find(L lower, L upper) throws IgniteCheckedException {
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
    public T findOne(L row) throws IgniteCheckedException {
        GetOne g = new GetOne(row);

        doFind(g);

        return (T)g.row;
    }

    /**
     * Initialize the given operation.
     *
     * !!! Symmetrically with this method must be called {@link Get#releaseMeta()} in {@code finally} block.
     *
     * @param g Operation.
     */
    private void initOperation(Get g) throws IgniteCheckedException {
        if (g.meta == null)
            g.meta = page(metaPageId);

        int rootLvl;
        long rootId;

        ByteBuffer buf = g.meta.getForRead();

        try {
            BPlusIOMeta io = BPlusIOMeta.VERSIONS.forPage(buf);

            rootLvl = io.getRootLevel(buf);
            rootId = io.getLeftmostPageId(buf, rootLvl);
        }
        finally {
            g.meta.releaseRead();
        }

        g.restartFromRoot(rootId, rootLvl, globalRmvId.get());
    }

    /**
     * @param g Get.
     */
    private void doFind(Get g) throws IgniteCheckedException {
        try {
            for (;;) { // Go down with retries.
                initOperation(g);

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
    private String printTree() {
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

        if (io.canGetRow()) {
            for (int i = 0; i < cnt; i++) {
                if (i != 0)
                    b.append(',');

                b.append(getRow(io, buf, i));
            }
        }
        else
            b.append("<can't get keys>");

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
    public T remove(L row) throws IgniteCheckedException {
        Remove r = new Remove(row);

        try {
            for (;;) {
                initOperation(r);

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

//            if ("_key_PK".equals(getName()) && row.getValue(0).getInt() < 900) {
//                X.println("row= " + row);
//                X.println("rmv= " + r.removed);
//                X.println("idx= " + getName());
//                X.println(printTree());
//                X.println("======================================");
//            }
        }
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param cnt Count.
     * @param idx Index to remove.
     * @param meta Meta page.
     * @param lvl Level.
     * @param kickLeftChild If we are dropping left child instead of the right one.
     * @throws IgniteCheckedException If failed.
     */
    private void doRemove(BPlusIO io, Page page, ByteBuffer buf, int cnt, int idx, Page meta, int lvl,
        boolean kickLeftChild) throws IgniteCheckedException {
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
            freePage(page, buf, io, meta, lvl); // Free root.
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
                            return lockTail(r, page, backId, fwdId, lvl);

                        return res;

                    case Remove.FOUND:
                        // We must be at the bottom here, just need to remove row from the current page.
                        assert lvl == 0 : lvl;
                        assert r.removed == null;

                        res = removeFromLeaf(r, page, backId, fwdId);

                        // Finish if we don't need to do any merges.
                        if (res == Remove.FOUND && r.needReplaceInner == FALSE && r.needMerge == FALSE)
                            r.finish();

                        return res;

                    case Remove.NOT_FOUND:
                        // We are at the bottom.
                        assert lvl == 0: lvl;

                        r.finish();

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
     * @param r Remove operation.
     * @param leaf Leaf page.
     * @param backId Back page ID.
     * @param fwdId Forward ID.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private int removeFromLeaf(Remove r, Page leaf, long backId, long fwdId) throws IgniteCheckedException {
        r.pageId = leaf.id();
        r.page = leaf;
        r.backId = backId;
        r.fwdId = fwdId;

        if (backId == 0)
            return doRemoveFromLeaf(r); // Fast path.

        // Lock back page before the remove, we'll need it for merges.
        Page back = page(backId);

        try {
            return writePage(back, lockBackAndRemoveFromLeaf, r, 0, Remove.RETRY);
        }
        finally {
            if (r.canRelease(back, 0))
                back.close();
        }
    }

    /**
     * @param r Remove operation.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private int doRemoveFromLeaf(Remove r) throws IgniteCheckedException {
        assert r.page != null;

        return writePage(r.page, removeFromLeaf, r, 0, Remove.RETRY);
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
        return ThreadLocalRandom.current().nextInt(maxCnt - minCnt) >= cnt - minCnt;
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
     * @param prnt Parent tail.
     * @param cur Current tail.
     * @param fwd Forward page.
     * @param fwdBuf Forward buffer.
     * @throws IgniteCheckedException If failed.
     */
    private boolean mergePages(Page meta, Tail<L> prnt, Tail<L> cur, Page fwd, ByteBuffer fwdBuf)
        throws IgniteCheckedException {
        assert io(fwdBuf) == cur.io;

        int cnt = cur.io.getCount(cur.buf);
        int fwdCnt = cur.io.getCount(fwdBuf);
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

            cur.io.store(cur.buf, cnt, prnt.io, prnt.buf, prntIdx);

            cnt++;
        }

        cur.io.copyItems(fwdBuf, cur.buf, 0, cnt, fwdCnt, true);
        cur.io.setForward(cur.buf, cur.io.getForward(fwdBuf));

        // Update parent.
        assert prntCnt > 0: prntCnt;

        doRemove(prnt.io, prnt.page, prnt.buf, prntCnt, prnt.idx, meta, prnt.lvl, false);

        // Forward page is now empty and has no links.
        freePage(fwd, fwdBuf, cur.io, meta, cur.lvl);

        return true;
    }

    /**
     * @param meta Meta.
     * @param inner Inner replace page.
     * @throws IgniteCheckedException If failed.
     */
    private void dropEmptyBranch(Page meta, Tail inner) throws IgniteCheckedException {
        int cnt = inner.io.getCount(inner.buf);

        assert cnt > 0: cnt;
        assert inner.fwd == null: "if we've found our inner key in this page it can't be a back page";

        // We need to check if the branch we are going to drop is from the left or right.
        boolean kickLeft = inner.down.page.id() == inner(inner.io).getLeft(inner.buf, inner.idx);
        assert kickLeft || inner.down.page.id() == inner(inner.io).getRight(inner.buf, inner.idx);

        doRemove(inner.io, inner.page, inner.buf, cnt, inner.idx, meta, inner.lvl, kickLeft);

        for (Tail t = inner.down; t != null; t = t.down) {
            if (t.fwd != null)
                t = t.fwd;

            assert t.io.getCount(t.buf) == 0;

            freePage(t.page, t.buf, t.io, meta, t.lvl);
        }
    }

    /**
     * @param page Page.
     * @param buf Buffer.
     * @param io IO.
     * @param meta Meta page.
     * @param lvl Level.
     * @throws IgniteCheckedException If failed.
     */
    private void freePage(Page page, ByteBuffer buf, BPlusIO io, Page meta, int lvl)
        throws IgniteCheckedException {
        if (getLeftmostPageId(meta, lvl) == page.id()) {
            // This logic will handle root as well.
            long fwdId = io.getForward(buf);

            writePage(meta, updateLeftmost, fwdId, lvl, FALSE);
        }

        // Currently only emulate free.
        io.setRemoveId(buf, Long.MAX_VALUE);
        // TODO do real free page: getForRead() and getForWrite() must return null for a free page.
    }

    /**
     * @param r Remove.
     * @param page Page.
     * @param backId Back page ID.
     * @param fwdId Expected forward page ID.
     * @param lvl Level.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private int lockTail(Remove r, Page page, long backId, long fwdId, int lvl) throws IgniteCheckedException {
        assert r.needMerge == TRUE ^ r.needReplaceInner == TRUE: "we can do only one thing at once";

        // Init parameters for the handlers.
        r.pageId = page.id();
        r.page = page;
        r.fwdId = fwdId;
        r.backId = backId;

        if (backId == 0) // Back page ID is provided only when last move was to the right.
            return doLockTail(r, lvl);

        Page back = page(backId);

        try {
            return writePage(back, lockBackAndTail, r, lvl, Remove.RETRY);
        }
        finally {
            if (r.canRelease(back, lvl))
                back.close();
        }
    }

    /**
     * @param r Remove operation.
     * @param lvl Level.
     * @return Result code.
     * @throws IgniteCheckedException If failed.
     */
    private int doLockTail(Remove r, int lvl) throws IgniteCheckedException {
        assert r.page != null;

        return writePage(r.page, lockTail, r, lvl, Remove.RETRY);
    }

    /**
     * @param row Row.
     * @return Old row.
     * @throws IgniteCheckedException If failed.
     */
    public T put(T row) throws IgniteCheckedException {
        Put p = new Put(row);

        try {
            for (;;) { // Go down with retries.
                initOperation(p);

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

//        if (p.split) {
//            X.println(getName() + ": " + p.oldRow + " -> " + row);
//            X.println("============new==========");
//            X.println(printTree());
//            X.println("=========================");
//        }
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
    private void insertSimple(BPlusIO<L> io, ByteBuffer buf, T row, int idx, long rightId)
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
    private T insertWithSplit(Page meta, BPlusIO<L> io, final ByteBuffer buf, T row,
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

            assert io.canGetRow(); // TODO refactor to use io.store for move up, this assert is wrong in general case
            T moveUpRow = getRow(io, buf, cnt - 1); // Last item from backward row goes up.

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
                    io.store(newRootBuf, 0, moveUpRow); // TODO refactor
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
    private T insert(Page meta, BPlusIO<L> io, ByteBuffer buf, T row, int idx, long rightId, int lvl)
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

                assert io.getCount(buf) > 0;

                return inner(io).getLeft(buf, 0);
            }
            finally {
                page.releaseRead();
            }
        }
    }

    /**
     * @param page Page.
     * @return Number of row links in the given index page.
     */
    private int getLinksCount(Page page) throws IgniteCheckedException {
        assert page != null;

        ByteBuffer buf = page.getForRead();

        try {
            BPlusIO<L> io = io(buf);

            return io.getCount(buf);
        }
        finally {
            page.releaseRead();
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
                            p.foundInner = FALSE;

                            res = writePage(page, replace, p, lvl, Put.RETRY);

                            if (res != Put.FOUND)
                                return res; // Need to retry.

                            p.foundInner = DONE; // We can have only single matching inner key, no more attempts.
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
     * @param io IO.
     * @param buf Buffer.
     * @param idx Index.
     * @return Full data row.
     * @throws IgniteCheckedException If failed.
     */
    protected final T getRow(BPlusIO<L> io, ByteBuffer buf, int idx) throws IgniteCheckedException {
        return dataStore.getRow(io, buf, idx);
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

        /** Starting point root level. May be outdated. Must be modified only in {@link #initOperation(Get)}. */
        int rootLvl;

        /** Starting point root ID. May be outdated. Must be modified only in {@link #initOperation(Get)}. */
        long rootId;

        /** Meta page. Initialized by {@link #initOperation(Get)}, released by {@link Get#releaseMeta()}. */
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
         * @param rootId Root page ID.
         * @param rootLvl Root level.
         * @param rmvId Remove ID to be afraid of.
         */
        void restartFromRoot(long rootId, int rootLvl, long rmvId) {
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
        void releaseMeta() {
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
    private class GetOne extends Get {
        /**
         * @param row Row.
         */
        public GetOne(L row) {
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
    private class GetCursor extends Get {
        /** */
        private ForwardCursor cursor;

        /**
         * @param lower Lower bound.
         * @param upper Upper bound.
         */
        public GetCursor(L lower, L upper) {
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
    private class Put extends Get {
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
        Put(T row) {
            super(row);
        }

        /**
         * @return Row.
         */
        @SuppressWarnings("unchecked")
        T row() {
            return (T)row;
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            if (io.isLeaf())
                return true;

            if (foundInner == FALSE)
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
    private class Remove extends Get {
        /** We may need to lock part of the tree branch from the bottom to up for multiple levels. */
        Tail<L> tail;

        /** */
        byte needReplaceInner = FALSE;

        /** */
        byte needMerge = FALSE;

        /** Removed row. */
        T removed;

        /** Current page. */
        public Page page;

        /** */
        public short innerIdx = Short.MIN_VALUE;

        /**
         * @param row Row.
         */
        public Remove(L row) {
            super(row);
        }

        /** {@inheritDoc} */
        @Override boolean found(BPlusIO<L> io, ByteBuffer buf, int idx, int lvl) throws IgniteCheckedException {
            if (!io.isLeaf() && needReplaceInner == FALSE)
                needReplaceInner = TRUE;

            return io.isLeaf();
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
        void finish() {
            assert tail == null;

            row = null;
        }

        /**
         * Process tail and finish.
         *
         * @param skipMergeMore Ignore the attempt to merge more pages up.
         * @return {@code false} If failed to finish and we need to lock more pages up.
         * @throws IgniteCheckedException If failed.
         */
        boolean finishTail(boolean skipMergeMore) throws IgniteCheckedException {
            assert !isFinished();
            assert needMerge != FALSE || needReplaceInner != FALSE;
            assert tail != null;

            if (needReplaceInner == READY) {
                assert getTail(0, false) != null: "we must keep lock on the leaf page";

                // We increment remove ID in write lock on leaf page, thus it is guaranteed that
                // any successor will get greater value than he had read at the beginning of the operation.
                // Thus it will be guaranteed to do a retry from root.
                globalRmvId.incrementAndGet();

                // Need to replace inner key with new max key for the left subtree.
                doReplaceInner();

                needReplaceInner = DONE;
            }
            else if (needMerge == READY) {
                assert tail.down != null || tail.fwd.down != null;

                boolean needMergeMore = merge(tail.lvl - 1, true, true);

                if (needMergeMore && !skipMergeMore) {
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
         * @throws IgniteCheckedException If failed.
         */
        private void doReplaceInner() throws IgniteCheckedException {
            assert needReplaceInner == READY: needReplaceInner;
            assert tail.lvl > 0;
            assert innerIdx >= 0;

            Tail<L> leaf = getTail(0, false);
            Tail<L> inner = getTail(tail.lvl, false);

            assert innerIdx < inner.io.getCount(inner.buf);

            int cnt = leaf.io.getCount(leaf.buf);

            if (cnt == 0) { // Merge empty leaf page.
                if (!merge(0, true, false)) {
                    // For leaf pages this can happen only when parent is empty -> drop the whole branch.
                    dropEmptyBranch(meta, inner);

                    return;
                }

                // Need to handle possible tail restructuring after merge.
                leaf = getTail(0, false);

                cnt = leaf.io.getCount(leaf.buf);

                // If any leaf becomes empty we have to either merge it or drop the whole empty branch.
                assert cnt > 0: "leaf can't be empty after successful merge";
            }

            // If after leaf merge parent have lost inner key, we don't need to update it anymore.
            if (innerIdx < inner.io.getCount(inner.buf)) {
                inner.io.store(inner.buf, innerIdx, leaf.io, leaf.buf, cnt - 1);
                leaf.io.setRemoveId(leaf.buf, globalRmvId.get());
            }
            else {
                assert innerIdx == inner.io.getCount(inner.buf);
                assert inner(inner.io).getLeft(inner.buf, innerIdx) == leaf.page.id();
            }

            // Try to merge the whole branch up if possible.
            for (int i = 1, end = tail.lvl - 1; i < end; i++) { // TODO fix deadlock here
                if (!merge(i, false, true))
                    break;
            }
        }

        /**
         * @param lvl Level.
         * @param skipMayMerge Skip checking for {@link #mayMerge(int, int)}.
         * @return {@code true} If merged, {@code false} if not (for example because of insufficient space).
         * @throws IgniteCheckedException If failed.
         */
        private boolean merge(int lvl, boolean skipMayMerge, boolean releaseMerged) throws IgniteCheckedException {
            assert tail.lvl > lvl;

            Tail<L> prnt = getTail(lvl + 1, false);

            if (prnt.io.getCount(prnt.buf) == 0)
                return false; // Parent is an empty routing page, forward page will have another parent.

            Tail<L> cur = getTail(lvl, false);
            Tail<L> back = getTail(lvl, true);

            if (back == null) {
                // We don't have a back page -> last move was to the left.
                long fwdId = cur.io.getForward(cur.buf);

                if (fwdId == 0)  // We can get 0 only in the last rightmost page with empty parent -> drop both.
                    return false;

                int cnt = cur.io.getCount(cur.buf);

                if (!skipMayMerge) {
                    int maxCnt = cur.io.getMaxCount(cur.buf);

                    if (!mayMerge(cnt, maxCnt))
                        return false;
                }

                try (Page fwd = page(fwdId)) {
                    assert fwd != null; // We've locked cur page which is back for fwd.

                    // Check count in read lock first.
                    int fwdCnt = getLinksCount(fwd);

                    if (countAfterMerge(cur, fwdCnt) == -1)
                        return false;

                    if (writePage(fwd, mergePages, this, 0, FALSE) == TRUE) {
                        if (releaseMerged) {
                            prnt.down = null;

                            writeUnlockAndClose(cur.page);
                        }

                        return true;
                    }

                    return false;
                }
            }
            else {
                assert cur.io == back.io: "must always be the same"; // Otherwise may be not compatible.

                if (mergePages(meta, prnt, back, cur.page, cur.buf)) {
                    assert prnt.down == back;
                    assert back.fwd == cur;

                    // Back becomes current.
                    back.down = cur.down;
                    back.fwd = null;

                    if (releaseMerged) {
                        prnt.down = null;

                        writeUnlockAndClose(back.page);
                        writeUnlockAndClose(cur.page);
                    }

                    return true;
                }

                return false;
            }
        }

        /**
         * @return {@code true} If finished.
         */
        boolean isFinished() {
            return row == null;
        }

        /**
         * Release pages for all locked levels at the tail.
         */
        void releaseTail() {
            Tail t = tail;

            tail = null;

            while (t != null) {
                writeUnlockAndClose(t.page);

                if (t.down != null)
                    t = t.down;
                else
                    t = t.fwd;
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
        boolean isTail(long pageId, int lvl) {
            Tail t = tail;

            while (t != null) {
                if (t.lvl < lvl)
                    return false;

                if (t.lvl == lvl && t.page.id() == pageId)
                    return true;

                if (t.fwd != null)
                    t = t.fwd;
                else
                    t = t.down;
            }

            return false;
        }

        /**
         * @param page Page.
         * @param buf Buffer.
         * @param io IO.
         * @param lvl Level.
         * @param back If the given page is back page for the current tail, otherwise it must be an upper level page.
         * @param idx Insertion index.
         */
        void addTail(Page page, ByteBuffer buf, BPlusIO<L> io, int lvl, boolean back, int idx) {
            Tail<L> t = new Tail<>(page, buf, io, lvl, idx);

            if (back) {
                assert tail != null;
                assert tail.lvl == lvl : "must be on the same level as out forward";

                t.fwd = tail;
            }
            else {
                assert tail == null || tail.lvl == lvl - 1: "must be on upper higher than current tail";

                // Grow the tail.
                t.down = tail;
            }

            tail = t;
        }

        /**
         * @return Non-back tail page.
         */
        public Page nonBackTailPage() {
            assert tail != null;

            return tail.fwd == null ? tail.page : tail.fwd.page;
        }

        /**
         * @param lvl Level.
         * @param back Back page.
         * @return Tail.
         */
        public Tail<L> getTail(int lvl, boolean back) {
            assert lvl <= tail.lvl: "level is too high";

            Tail<L> t = tail;

            while (t.lvl != lvl) {
                if (t.fwd != null)
                    t = t.fwd;
                else
                    t = t.down;

                assert t != null : "level is too low";
            }

            if (back)
                return t.fwd != null ? t : null;

            return t.fwd != null ? t.fwd : t;
        }
    }

    /**
     * Tail for remove.
     */
    private static class Tail<L> {
        /** */
        final Page page;

        /** */
        final ByteBuffer buf;

        /** */
        final BPlusIO<L> io;

        /** */
        final int lvl;

        /** */
        final int idx;

        /** */
        Tail<L> fwd;

        /** */
        Tail<L> down;

        /**
         * @param page Write locked page.
         * @param buf Buffer.
         * @param io IO.
         * @param lvl Level.
         * @param idx Insertion index.
         */
        private Tail(Page page, ByteBuffer buf, BPlusIO<L> io, int lvl, int idx) {
            assert page != null;

            this.page = page;
            this.buf = buf;
            this.io = io;
            this.lvl = lvl;
            this.idx = idx;
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
    private static BPlusIOInner inner(BPlusIO io) {
        return (BPlusIOInner)io;
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
    protected abstract BPlusIOInner<L> latestInnerIO();

    /**
     * @return Latest version of leaf page IO.
     */
    protected abstract BPlusIOLeaf<L> latestLeafIO();

    /**
     * @param buf Buffer.
     * @param idx Index of row in the given buffer.
     * @param row Lookup row.
     * @return Comparison result as in {@link Comparator#compare(Object, Object)}.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract int compare(BPlusIO<L> io, ByteBuffer buf, int idx, L row) throws IgniteCheckedException;

    /**
     * @param pageId Page ID.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract Page page(long pageId) throws IgniteCheckedException;

    /**
     * @return Allocated page.
     */
    protected abstract Page allocatePage() throws IgniteCheckedException;

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
