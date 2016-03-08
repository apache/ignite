package org.apache.ignite.internal.processors.query.h2.database;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdUtils.dwordsOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.linkFromDwordOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 * B+Tree index over references to data stored in data pages ({@link DataPageIO}).
 */
@SuppressWarnings("PackageVisibleField")
public class BPlusTreeRefIndex extends PageMemoryIndex {
    /** */
    private final PageMemory pageMem;

    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final CacheObjectContext coctx;

    /** */
    private final long metaPageId;

    /** */
    private final AtomicLongArray leftWall = new AtomicLongArray(33); // Max tree height = size(int) + 1

    /** */
    private volatile int topLvl;

    /** */
    private volatile long lastDataPageId;

    /**
     * @param keyCol Key column.
     * @param valCol Value column.
     */
    public BPlusTreeRefIndex(
        GridCacheContext<?,?> cctx,
        PageMemory pageMem,
        long metaPageId,
        boolean initNew,
        int keyCol,
        int valCol,
        Table tbl,
        String name,
        boolean pk,
        IndexColumn[] cols
    ) {
        super(keyCol, valCol);

        try {
            if (!pk) {
                // For other indexes we add primary key at the end to avoid conflicts.
                cols = Arrays.copyOf(cols, cols.length + 1);

                cols[cols.length - 1] = ((GridH2Table)tbl).indexColumn(keyCol, SortOrder.ASCENDING);
            }

            this.cctx = cctx;
            this.pageMem = pageMem;

            coctx = cctx.cacheObjectContext();

            initBaseIndex(tbl, 0, name, cols,
                pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

            if (initNew) { // Init new index.
                Page meta = pageMem.page(metaPageId);

                try {
                    ByteBuffer buf = meta.getForInitialWrite();

                    MetaPageIO io = MetaPageIO.latest();

                    io.initNewPage(buf, metaPageId);

                    Page root = allocatePage(-1, FLAG_IDX);

                    try {
                        LeafPageIO.latest().initNewPage(root.getForInitialWrite(), root.id());

                        io.setRootPageId(buf, root.id());
                        io.setRootLevel(buf, 0);

                        leftWall.set(topLvl = 0, root.id());
                    }
                    finally {
                        pageMem.releasePage(root);
                    }
                }
                finally {
                    pageMem.releasePage(meta);
                }
            }
            else { // Init existing.
                long pageId;
                int lvl;

                Page meta = pageMem.page(metaPageId);

                try {
                    ByteBuffer buf = meta.getForRead();

                    try {
                        MetaPageIO io = MetaPageIO.forPage(buf);

                        pageId = io.getRootPageId(buf);
                        lvl = io.getRootLevel(buf);

                        topLvl = lvl;
                    }
                    finally {
                        meta.releaseRead();
                    }
                }
                finally {
                    pageMem.releasePage(meta);
                }

                for (;;) {
                    leftWall.set(lvl, pageId);

                    if (lvl == 0) // Leaf level.
                        break;

                    pageId = getLeftmostChild(pageId);

                    lvl--;
                }
            }

            this.metaPageId = metaPageId;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @return Meta page ID.
     */
    public long getMetaPageId() {
        return metaPageId;
    }

    /**
     * @return Root page ID.
     */
    private long getRootPageId() {
        return getRootPageId(topLvl);
    }

    /**
     * @return Root page ID.
     */
    private long getRootPageId(int lvl) {
        assert lvl <= topLvl : lvl;

        return leftWall.get(lvl);
    }

    /**
     * Must be called inside of write lock on the current root page.
     *
     * @param lvl Level.
     * @param rootPageId Root page ID.
     */
    private void setRootPageId(int lvl, long rootPageId) throws IgniteCheckedException {
        Page meta = pageMem.page(metaPageId);

        try {
            ByteBuffer buf = meta.getForWrite();

            try {
                MetaPageIO io = MetaPageIO.forPage(buf);

                io.setRootPageId(buf, rootPageId);
                io.setRootLevel(buf, lvl);

                leftWall.set(lvl, rootPageId);
                topLvl = lvl;
            }
            finally {
                meta.releaseWrite(true);
            }
        }
        finally {
            pageMem.releasePage(meta);
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        try {
            if (lower == null) {
                ForwardCursor cursor = new ForwardCursor(upper);

                Page first = pageMem.page(leftWall.get(0));

                try {
                    ByteBuffer buf = first.getForRead();

                    try {
                        cursor.bootstrap(buf, 0);
                    }
                    catch (IgniteCheckedException e) {
                        throw DbException.convert(e);
                    }
                    finally {
                        first.releaseRead();
                    }
                }
                finally {
                    pageMem.releasePage(first);
                }

                return cursor;
            }

            Get g = new Get(lower);

            g.cursor = new ForwardCursor(upper);

            doFind(g);

            return g.cursor;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row findOne(GridH2Row row) {
        Get g = new Get(row);

        g.findOne = true;

        doFind(g);

        return (GridH2Row)g.row;
    }

    /**
     * @param g Get.
     */
    private void doFind(Get g) {
        try {
            for (;;) { // Go down with retries.
                int lvl = topLvl;

                long rootId = getRootPageId(lvl);

                if (!findDown(g, rootId, 0L, lvl))
                    break;

                checkInterrupted();
            }
        }
        catch (IgniteCheckedException e) {
            // TODO handle the exception properly.
            throw new RuntimeException(e);
        }
    }

    /**
     * @param g Get.
     * @param pageId Page ID.
     * @param expFwdId Expected forward page ID.
     * @param lvl Level.
     * @return {@code true} If retry.
     * @throws IgniteCheckedException If failed.
     */
    private boolean findDown(final Get g, final long pageId, final long expFwdId, final int lvl)
        throws IgniteCheckedException {
        Page page = pageMem.page(pageId);

        try {
            for (;;) {
                // Init args.
                g.pageId = pageId;
                g.expFwdId = expFwdId;

                int res = readPage(page, search, g, lvl);

                switch (res) {
                    case Get.RETRY:
                        return true;

                    case Get.GO_DOWN:
                        assert g.pageId != pageId;
                        assert g.expFwdId != expFwdId || expFwdId == 0;

                        // Go down recursively.
                        if (findDown(g, g.pageId, g.expFwdId, lvl - 1)) {
                            checkInterrupted();

                            continue; // The child page got split, need to reread our page.
                        }

                        return false;

                    case Get.FOUND:
                        return false;

                    case Get.NOT_FOUND:
                        g.row = null; // Mark not found result.

                        return false;

                    default:
                        throw new IllegalStateException("Invalid result: " + res);
                }
            }
        }
        finally {
            pageMem.releasePage(page);
        }
    }

    /**
     * @param expectedLastId Expected last data page ID.
     * @return Next data page ID.
     */
    private synchronized long nextDataPage(long expectedLastId) throws IgniteCheckedException {
        if (expectedLastId != lastDataPageId)
            return lastDataPageId;

        // TODO we need a partition here
        Page page = allocatePage(-1, FLAG_DATA);

        try {
            ByteBuffer buf = page.getForInitialWrite();

            DataPageIO.latest().initNewPage(buf, page.id());
        }
        finally {
            pageMem.releasePage(page);
        }

        return lastDataPageId = page.id();
    }

    /**
     * @param row Row.
     */
    private void writeRowData(GridH2Row row) throws IgniteCheckedException {
        while (row.link == 0) {
            long pageId = lastDataPageId;

            if (pageId == 0)
                pageId = nextDataPage(0);

            Page page = pageMem.page(pageId);

            try {
                ByteBuffer buf = page.getForWrite();

                boolean ok = false;

                try {
                    DataPageIO io = DataPageIO.forPage(buf);

                    int idx = io.addRow(coctx, buf, row.key, row.val, row.ver);

                    if (idx != -1) {
                        ok = true;

                        row.link = linkFromDwordOffset(pageId, idx);

                        break;
                    }
                }
                finally {
                    page.releaseWrite(ok);
                }
            }
            finally {
                pageMem.releasePage(page);
            }

            nextDataPage(pageId);
        }
    }

    /**
     * Print tree structure to system out for debug.
     */
    private String printTree(boolean keys) {
        StringBuilder b = new StringBuilder();

        try {
            printTree(getRootPageId(), "", true, b, keys);
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }

        return b.toString();
    }

    /**
     * @param pageId Page ID.
     * @param prefix Prefix.
     * @param tail Tail.
     */
    private void printTree(long pageId, String prefix, boolean tail, StringBuilder b, boolean keys)
        throws IgniteCheckedException {
        Page page = pageMem.page(pageId);

        try {
            ByteBuffer buf = page.getForRead();

            try {
                IndexPageIO io = IndexPageIO.forPage(buf);

                int cnt = io.getCount(buf);

                b.append(prefix).append(tail ? "└── " : "├── ").append(printPage(io, buf, true, keys));

                b.append('\n');

                if (!io.isLeaf()) {
                    for (int i = 0; i < cnt - 1; i++) {
                        long leftPageId = inner(io).getLeft(buf, i);

                        if (leftPageId != 0)
                            printTree(leftPageId, prefix + (tail ? "    " : "│   "), false, b, keys);
                    }

                    if (cnt > 0) {
                        int i = cnt - 1;

                        long leftPageId = inner(io).getLeft(buf, i);
                        long rightPageId = inner(io).getRight(buf, i);

                        if (leftPageId != 0)
                            printTree(leftPageId, prefix + (tail ? "    " : "│   "), rightPageId == 0, b, keys);

                        if (rightPageId != 0)
                            printTree(rightPageId, prefix + (tail ? "    " : "│   "), true, b, keys);
                    }
                }
            }
            finally {
                page.releaseRead();
            }
        }
        finally {
            pageMem.releasePage(page);
        }
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param lr Leftmost and rightmost children.
     * @param keys Keys.
     * @return String.
     */
    private String printPage(IndexPageIO io, ByteBuffer buf, boolean lr, boolean keys) throws IgniteCheckedException {
        int cnt = io.getCount(buf);

        int lrCnt = -1;

        if (!io.isLeaf()) {
            lrCnt = 0;

            for (int i = 0; i < cnt; i++) {
                if (inner(io).getLeft(buf, i) != 0)
                    lrCnt++;
            }

            if (cnt != 0) {
                if (inner(io).getRight(buf, cnt - 1) != 0)
                    lrCnt++;
            }
        }

        StringBuilder b = new StringBuilder();

        b.append(formatPageId(PageIO.getPageId(buf))).append('(').append(cnt);

        if (lrCnt != -1)
            b.append(',').append(lrCnt);

        b.append(")");

        long fwdId = io.getForward(buf);

        if (fwdId != 0)
            b.append(' ').append(formatPageId(fwdId));

        if (lr && !io.isLeaf() && cnt > 0) {
            b.append(" <");
            b.append(inner(io).getLeft(buf, 0));
            b.append(',');
            b.append(inner(io).getRight(buf, cnt - 1));
            b.append('>');
        }

        b.append(' ');

        if (keys)
            b.append(printPageKeys(io, buf));

        return b.toString();
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @return Keys as String.
     */
    private String printPageKeys(IndexPageIO io, ByteBuffer buf) throws IgniteCheckedException {
        int cnt = io.getCount(buf);

        StringBuilder b = new StringBuilder();

        b.append('[');

        for (int i = 0; i < cnt; i++) {
            if (i != 0)
                b.append(',');

            b.append(getRow(io.getLink(buf, i)).key.value(coctx, true));
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
     */
    private static void checkInterrupted() {
        if (Thread.currentThread().isInterrupted())
            throw new IgniteInterruptedException("Interrupted.");
    }

    /** {@inheritDoc} */
    @SuppressWarnings("StatementWithEmptyBody")
    @Override public GridH2Row put(GridH2Row row) {
        Put p = new Put(row);

        try {
            int lvl;

            for (;;) { // Go down with retries.
                lvl = topLvl;

                long rootId = getRootPageId(lvl);

                if (!putDown(p, rootId, 0L, lvl))
                    break;

                checkInterrupted();
            }

            // Go up and finish if needed (root got concurrently splitted).
            if (!p.isFinished())
                putUp(p, lvl + 1);

            assert p.isFinished();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

//        if (p.split && "iVal_idx".equals(getName())) {
//            X.println(getName() + ": " + p.oldRow + " -> " + row);
//            X.println("============old==========");
//            X.println(oldTree);
//            X.println("============new==========");
//            X.println(printTree(true));
//            X.println("=========================");
//        }

        return p.oldRow;
    }

    /**
     * @param p Put.
     * @param lvl Level.
     * @throws IgniteCheckedException If failed.
     */
    private void putUp(Put p, int lvl) throws IgniteCheckedException {
        PageHandler<Put> handler = p.oldRow == null ? insert : replace;

        long pageId = getRootPageId(lvl);

        for (;;) {
            if (topLvl < lvl)
                throw new IllegalStateException("Level overflow.");

            Page page = pageMem.page(pageId);

            try {
                int res = writePage(page, handler, p, lvl);

                switch (res) {
                    case Put.FINISH:
                        if (p.isFinished())
                            return; // We are done.

                        pageId = getRootPageId(++lvl); // Go up.

                        break;

                    case Put.NOT_FOUND:
                        // Go forward.
                        assert p.pageId != pageId;

                        pageId = p.pageId;

                        break;

                    default:
                        throw new IllegalStateException("Illegal result: " + res);
                }
            }
            finally {
                pageMem.releasePage(page);
            }

            checkInterrupted();
        }
    }

    /**
     * @param io IO.
     * @param buf Splitting buffer.
     * @param fwdBuf Forward buffer.
     */
    private void splitPage(IndexPageIO io, ByteBuffer buf, ByteBuffer fwdBuf) {
        int cnt = io.getCount(buf);
        int mid = 1 + (cnt >>> 1);

        cnt -= mid;

        io.copyItems(buf, fwdBuf, mid, 0, cnt);

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
    private void insertSimple(IndexPageIO io, ByteBuffer buf, GridH2Row row, int idx, long rightId)
        throws IgniteCheckedException {
        if (getIndexType().isPrimaryKey())
            writeRowData(row);

        assert row.link != 0;

        int cnt = io.getCount(buf);

        if (idx != cnt) // Move right all the greater elements to make a free slot for new row link.
            io.copyItems(buf, buf, idx, idx + 1, cnt - idx);

        io.setLink(buf, idx, row.link);

        if (!io.isLeaf()) // Setup reference to the right page on split.
            inner(io).setRight(buf, idx, rightId);

        io.setCount(buf, cnt + 1);
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param row Row.
     * @param idx Index.
     * @param rightId Right page ID after split.
     * @param lvl Level.
     * @return Move up row.
     * @throws IgniteCheckedException If failed.
     */
    private GridH2Row insertWithSplit(IndexPageIO io, final ByteBuffer buf, GridH2Row row,
        int idx, long rightId, int lvl) throws IgniteCheckedException {
        Page fwdPage = allocatePage(-1, FLAG_IDX);

        try {
            boolean hadFwdBeforeSplit = io.getForward(buf) != 0;

            ByteBuffer fwdBuf = fwdPage.getForInitialWrite();
            io.initNewPage(fwdBuf, fwdPage.id());

            splitPage(io, buf, fwdBuf);

            // Do insert.
            int cnt = io.getCount(buf);

            if (idx <= cnt) {
                insertSimple(io, buf, row, idx, rightId);

                if (idx == cnt && !io.isLeaf()) // Fix leftmost child of fwdPage, because newly inserted row will go up.
                    inner(io).setLeft(fwdBuf, 0, rightId);
            }
            else
                insertSimple(io, fwdBuf, row, idx - cnt, rightId);

            // Do move up.
            cnt = io.getCount(buf);

            long moveUpLink = io.getLink(buf, cnt - 1); // Last item from backward goes up.

            if (!io.isLeaf()) // Leaf pages must contain all the links, inner pages remove moveUpLink.
                io.setCount(buf, cnt - 1);

            if (!hadFwdBeforeSplit && lvl == topLvl) { // We are splitting root.
                Page newRoot = allocatePage(-1, FLAG_IDX);

                try {
                    if (io.isLeaf())
                        io = InnerPageIO.latest();

                    ByteBuffer newRootBuf = newRoot.getForInitialWrite();

                    io.initNewPage(newRootBuf, newRoot.id());

                    io.setCount(newRootBuf, 1);
                    inner(io).setLeft(newRootBuf, 0, PageIO.getPageId(buf));
                    io.setLink(newRootBuf, 0, moveUpLink);
                    inner(io).setRight(newRootBuf, 0, fwdPage.id());
                }
                finally {
                    pageMem.releasePage(newRoot);
                }

                setRootPageId(lvl + 1, newRoot.id());

                return null; // We've just moved link up to root, nothing to return here.
            }
            else { // Regular split.
                row = getRow(moveUpLink);
                row.link = moveUpLink;

                return row;
            }
        }
        finally {
            pageMem.releasePage(fwdPage);
        }
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param row Row.
     * @param idx Index.
     * @param rightId Right ID.
     * @param lvl Level.
     * @return Move up row.
     * @throws IgniteCheckedException If failed.
     */
    private GridH2Row insert(IndexPageIO io, ByteBuffer buf, GridH2Row row, int idx, long rightId, int lvl)
        throws IgniteCheckedException {
        int maxCnt = io.getMaxCount(buf);
        int cnt = io.getCount(buf);

        if (cnt == maxCnt) // Need to split page.
            return insertWithSplit(io, buf, row, idx, rightId, lvl);

        insertSimple(io, buf, row, idx, rightId);

        return null;
    }

    /**
     * @param dataPage Data page.
     * @param idx Index.
     * @return {@code true} If replaced.
     */
    private boolean updateDataInPlace(Page dataPage, int idx, GridH2Row row) throws IgniteCheckedException {
        boolean ok = false;

        ByteBuffer buf = dataPage.getForWrite();

        try {
            DataPageIO io = DataPageIO.forPage(buf);

            int oldKeyValLen = io.getKeyValueSize(buf, idx);

            int newKeyValLen = row.key.valueBytesLength(coctx) + row.val.valueBytesLength(coctx);

            // Can replace in place.
            if (oldKeyValLen >= newKeyValLen) {
                int dataOff = io.getDataOffset(buf, idx);

                io.writeRowDataInPlace(coctx, buf, dataOff, oldKeyValLen, row.key, row.val, row.ver);

                ok = true;
            }
        }
        finally {
            dataPage.releaseWrite(ok);
        }

        return ok;
    }

    /** */
    private final PageHandler<Put> replace = new PageHandler<Put>() {
        @Override public int run(Page page, ByteBuffer buf, Put p, int lvl) throws IgniteCheckedException {
            IndexPageIO io = IndexPageIO.forPage(buf);

            assert io.isLeaf(); // TODO when we will update bigger values we will need to update inner pages

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, p.row);

            if (idx < 0) { // Not found, split happened.
                idx = -idx - 1;

                assert idx == cnt;

                long fwdId = io.getForward(buf);

                assert fwdId != 0;

                p.pageId = fwdId;

                return Put.NOT_FOUND;
            }

            // Do replace.
            assert p.row().link == 0;

            long link = io.getLink(buf, idx);

            p.oldRow = getRow(link); // TODO may be optimize

            Page dataPage = pageMem.page(PageIdUtils.pageId(link));

            try {
                if (updateDataInPlace(dataPage, PageIdUtils.dwordsOffset(link), p.row()))
                    p.row().link = link;
                else
                    throw new UnsupportedOperationException("Too big entry to replace."); // TODO
            }
            finally {
                pageMem.releasePage(dataPage);
            }

            p.finish();

            return Put.FINISH;
        }

        @Override protected boolean releaseAfterWrite(Page page, Put p) {
            return p.tailLock != page;
        }
    };

    /** */
    private final PageHandler<Put> insert = new PageHandler<Put>() {
        @Override public int run(Page page, ByteBuffer buf, Put p, int lvl) throws IgniteCheckedException {
            IndexPageIO io = IndexPageIO.forPage(buf);

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, p.row);

            if (idx >= 0)
                throw new IllegalStateException("Duplicate row in index.");

            idx = -idx - 1;

            // Possible split.
            if (idx == cnt) {
                if (io.isLeaf()) { // For leaf we recheck expected forward page.
                    if (io.getForward(buf) != p.expFwdId)
                        return Put.RETRY; // Go up and retry.
                }
                else {
                    assert p.tailLock != null : "tail lock must be kept";
                    assert cnt > 0 : cnt; // We have a locked tailLock which is our child, we can't become empty.

                    // Tail lock page (the page that we've splitted downstairs) must be the rightmost.
                    if (p.tailLock.id() != inner(io).getRight(buf, cnt - 1)) {
                        p.pageId = io.getForward(buf);

                        assert p.pageId != 0;

                        return Put.NOT_FOUND; // Go forward.
                    }
                }
            }

            GridH2Row moveUpRow = insert(io, buf, p.row(), idx, p.rightId, lvl);

            if (moveUpRow != null) { // Split happened.
                p.split = true;
                p.row = moveUpRow;
                p.rightId = io.getForward(buf);
                p.tailLock(page);

                assert p.rightId != 0;
            }
            else
                p.finish();

            return Put.FINISH;
        }

        @Override protected boolean releaseAfterWrite(Page page, Put p) {
            return p.tailLock != page;
        }
    };

    /** */
    private final PageHandler<Get> search = new PageHandler<Get>() {
        @Override public int run(Page page, ByteBuffer buf, Get g, int lvl) throws IgniteCheckedException {
            IndexPageIO io = IndexPageIO.forPage(buf);

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, g.row);

            if (idx >= 0) { // Found exact match.
                if (g.findOne || io.isLeaf()) {
                    if (g.findOne)
                        g.row = getRow(io.getLink(buf, idx));
                    else if (g.cursor != null)
                        g.cursor.bootstrap(buf, idx);

                    return Get.FOUND;
                }

                // Else we need to reach leaf, go left down.
            }
            else {
                idx = -idx - 1;

                // If we are on the right edge, then check for expected forward page and retry of it does match.
                // It means that concurrent split happened.
                if (idx == cnt && io.getForward(buf) != g.expFwdId)
                    return Get.RETRY;

                if (io.isLeaf()) { // No way down, insert here.
                    assert g.pageId == page.id();

                    if (g.cursor != null)
                        g.cursor.bootstrap(buf, idx);

                    return Get.NOT_FOUND;
                }
            }

            // Go left down.
            g.pageId = inner(io).getLeft(buf, idx);

            // If we see the tree in consistent state, then our right down page must be forward for our left down page.
            if (idx < cnt)
                g.expFwdId = inner(io).getRight(buf, idx);
            else {
                assert idx == cnt;
                // But here we are actually going to right and child forward is unknown to us, need to ask our forward.
                // This is ok from the locking standpoint because we take all locks in the forward direction.
                long fwdId = io.getForward(buf);

                g.expFwdId = fwdId == 0L ? 0L : getLeftmostChild(fwdId);
            }

            return Get.GO_DOWN;
        }
    };

    /**
     * @param pageId Inner page ID.
     * @return Leftmost child page ID.
     */
    private long getLeftmostChild(long pageId) throws IgniteCheckedException {
        Page page = pageMem.page(pageId);

        try {
            ByteBuffer buf = page.getForRead();

            try {
                IndexPageIO io = IndexPageIO.forPage(buf);

                assert io.getCount(buf) > 0;

                return inner(io).getLeft(buf, 0);
            }
            finally {
                page.releaseRead();
            }
        }
        finally {
            pageMem.releasePage(page);
        }
    }

    /**
     * @param p Put.
     * @param pageId Page ID.
     * @param expFwdId Expected forward page ID.
     * @param lvl Level.
     * @return {@code true} If need to retry.
     * @throws IgniteCheckedException If failed.
     */
    private boolean putDown(final Put p, final long pageId, final long expFwdId, final int lvl)
        throws IgniteCheckedException {
        assert lvl >= 0 : lvl;

        PageHandler<Put> handler;

        Page page = pageMem.page(pageId);

        try {
            int res;

            for (;;) {
                // Init args.
                p.pageId = pageId;
                p.expFwdId = expFwdId;

                res = readPage(page, search, p, lvl);

                switch (res) {
                    case Put.RETRY:
                        return true;

                    case Put.GO_DOWN:
                        assert p.pageId != pageId;
                        assert p.expFwdId != expFwdId || expFwdId == 0;

                        // Go down recursively.
                        if (putDown(p, p.pageId, p.expFwdId, lvl - 1)) {
                            checkInterrupted();

                            continue; // The child page got splitted, need to reread our page.
                        }

                        if (p.isFinished()) {
                            assert p.tailLock == null;

                            return false; // Successfully inserted or replaced down the stack.
                        }

                        res = Put.NOT_FOUND; // Insert after split.
                }

                break;
            }

            // Insert or replace row in our page.
            assert res == Put.FOUND || res == Put.NOT_FOUND : res;

            handler = res == Put.FOUND ? replace : insert;

            // Init args.
            p.pageId = pageId;
            p.expFwdId = expFwdId;

            res = writePage(page, handler, p, lvl);

            switch (res) {
                case Put.FINISH:
                    return false;

                case Put.RETRY: {
                    assert lvl == 0 : "we must be at leaf level";

                    return true;
                }
            }

            assert res == Put.NOT_FOUND: res; // Split happened, need to go forward.
        }
        finally {
            if (p.tailLock != page)
                pageMem.releasePage(page);
        }

        // We've failed to insert/replace in this page, need to go forward until we catch up with split.
        for (;;) {
            assert p.pageId != pageId;

            page = pageMem.page(p.pageId);

            try {
                int res = writePage(page, handler, p, lvl);

                if (res == Put.FINISH)
                    return false;

                assert res == Put.NOT_FOUND: res;

                if (p.pageId == expFwdId) {
                    assert handler == replace;

                    return false; // Go up and try to replace there.
                }
            }
            finally {
                if (p.tailLock != page)
                    pageMem.releasePage(page);
            }
        }
    }

    /**
     * Get operation.
     */
    private class Get {
        /** */
        static final int GO_DOWN = 1;

        /** */
        static final int RETRY = 2;

        /** */
        static final int NOT_FOUND = 7;

        /** */
        static final int FOUND = 8;

        /** */
        boolean findOne;

        /** */
        ForwardCursor cursor;

        /** */
        SearchRow row;

        /** In/Out parameter: Page ID. */
        long pageId;

        /** In/Out parameter: expected forward page ID. */
        long expFwdId;

        /**
         * @param row Row.
         */
        Get(SearchRow row) {
            this.row = row;
        }
    }

    /**
     * Put operation state machine.
     */
    private class Put extends Get {
        /** */
        static final int FINISH = Integer.MAX_VALUE;

        /** */
        long rightId;

        /** */
        GridH2Row oldRow;

        /** This page is kept locked after split until insert to the upper level will not be finished. */
        Page tailLock;

        /** */
        boolean split;

        /**
         * @param row Row.
         */
        Put(GridH2Row row) {
            super(row);
        }

        /**
         * @return Row.
         */
        GridH2Row row() {
            return (GridH2Row)row;
        }

        /**
         * @param tailLock Tail lock.
         */
        private void tailLock(Page tailLock) throws IgniteCheckedException {
            if (this.tailLock != null) {
                this.tailLock.releaseWrite(true);
                pageMem.releasePage(this.tailLock);
            }
            this.tailLock = tailLock;
        }

        /**
         * Finish put.
         */
        void finish() throws IgniteCheckedException {
            row = null;
            rightId = 0;
            tailLock(null);
        }

        /**
         * @return {@code true} If finished.
         */
        boolean isFinished() {
            return row == null;
        }
    }

    /**
     * @param part Partition.
     * @param flag Flag.
     * @return Allocated page ID.
     */
    private Page allocatePage(int part, byte flag) throws IgniteCheckedException {
        long pageId = pageMem.allocatePage(cctx.cacheId(), part, flag);

        return pageMem.page(pageId);
    }

    /**
     * @param buf Buffer.
     * @param cnt Row count.
     * @param row Row.
     * @return Insertion point as in {@link Arrays#binarySearch(Object[], Object, Comparator)}.
     */
    private int findInsertionPoint(IndexPageIO io, ByteBuffer buf, int cnt, SearchRow row)
        throws IgniteCheckedException {
        int low = 0;
        int high = cnt - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            long link = io.getLink(buf, mid);

            GridH2Row midRow = getRow(link);

            int cmp = compareRows(midRow, row);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // found
        }

        return -(low + 1);  // not found
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        return null; // TODO
    }

    /**
     * @param link Link.
     * @return Row.
     */
    private GridH2Row getRow(long link) throws IgniteCheckedException {
        CacheObject key;
        CacheObject val;
        GridCacheVersion ver;

        Page page = pageMem.page(pageId(link));

        try {
            ByteBuffer buf = page.getForRead();

            try {
                DataPageIO io = DataPageIO.forPage(buf);

                int dataOff = io.getDataOffset(buf, dwordsOffset(link));

                buf.position(dataOff);

                // Skip key-value size.
                buf.getShort();

                key = coctx.processor().toCacheObject(coctx, buf);
                val = coctx.processor().toCacheObject(coctx, buf);

                int topVer = buf.getInt();
                int nodeOrderDrId = buf.getInt();
                long globalTime = buf.getLong();
                long order = buf.getLong();

                ver = new GridCacheVersion(topVer, nodeOrderDrId, globalTime, order);
            }
            finally {
                page.releaseRead();
            }
        }
        finally {
            pageMem.releasePage(page);
        }

        GridH2Row res;

        try {
            res = ((GridH2Table)getTable()).rowDescriptor().createRow(key, val, ver, 0);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        assert res.ver != null;

        return res;
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter filter, SortOrder sortOrder) {
        return 10 * getCostRangeIndex(masks, getRowCountApproximation(), filter, sortOrder);
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session session) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return 10_000; // TODO
    }

    /**
     * @param io IO.
     * @return Inner page IO.
     */
    private static InnerPageIO inner(IndexPageIO io) {
        return (InnerPageIO)io;
    }

    /**
     * @param io IO.
     * @return Leaf page IO.
     */
    private static LeafPageIO leaf(IndexPageIO io) {
        return (LeafPageIO)io;
    }

    /**
     * @param page Page.
     * @param h Handler.
     * @param arg Argument.
     * @param lvl Level.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    private static <X> int readPage(Page page, PageHandler<X> h, X arg, int lvl) throws IgniteCheckedException {
        ByteBuffer buf = page.getForRead();

        try {
            return h.run(page, buf, arg, lvl);
        }
        finally {
            page.releaseRead();
        }
    }

    /**
     * @param page Page.
     * @param h Handler.
     * @param arg Argument.
     * @param lvl Level.
     * @return Handler result.
     * @throws IgniteCheckedException If failed.
     */
    private static <X> int writePage(Page page, PageHandler<X> h, X arg, int lvl) throws IgniteCheckedException {
        int res;

        boolean ok = false;

        ByteBuffer buf = page.getForWrite();

        try {
            // TODO we need a set of CRC markers: LOADED, UPDATING, DIRTY
            // Mark update start.
            PageIO.setCrc(buf, 0xFAFABABA);

            res = h.run(page, buf, arg, lvl);

            // Mark update end.
            PageIO.setCrc(buf, 0x0BABEB00);

            ok = true;
        }
        finally {
            if (h.releaseAfterWrite(page, arg))
                page.releaseWrite(ok);
        }

        return res;
    }

    /**
     * Forward cursor.
     */
    private class ForwardCursor implements Cursor {
        /** */
        private List<GridH2Row> rows;

        /** */
        private int row;

        /** */
        private Page page;

        /** */
        private ByteBuffer buf;

        /** */
        private final SearchRow upperBound;

        /**
         * @param upperBound Upper bound.
         */
        ForwardCursor(SearchRow upperBound) {
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
                IndexPageIO io = LeafPageIO.forPage(buf);

                int cnt = io.getCount(buf);
                long fwdId = io.getForward(buf);

                if (cnt > 0) {
                    if (upperBound != null) {
                        int cmp = compareRows(getRow(io.getLink(buf, cnt - 1)), upperBound);

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
                        rows.add(getRow(io.getLink(buf, i)));
                }

                Page prevPage = page;

                if (fwdId != 0) { // Lock next page.
                    page = pageMem.page(fwdId);
                    buf = page.getForRead();
                }
                else { // Clear.
                    page = null;
                    buf = null;
                }

                if (prevPage != null) { // Release previous page.
                    prevPage.releaseRead();
                    pageMem.releasePage(prevPage);
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
        @Override public boolean next() {
            if (rows == null)
                return false;

            if (++row < rows.size())
                return true;

            row = 0;
            rows.clear();

            try {
                return fillFromBuffer(0);
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return rows.get(row);
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }

    /**
     * Abstract index page IO routines.
     */
    private abstract static class IndexPageIO extends PageIO {
        /** */
        static final int CNT_OFF = COMMON_HEADER_END;

        /** */
        static final int FORWARD_OFF = CNT_OFF + 2;

        /** */
        static final int ITEMS_OFF = FORWARD_OFF + 8;

        /**
         * @param buf Buffer.
         * @return IO.
         */
        @SuppressWarnings("unchecked")
        public static IndexPageIO forPage(ByteBuffer buf) {
            int type = getType(buf);
            int ver = getVersion(buf);

            switch (type) {
                case T_BPLUS_REF3_INNER:
                    return InnerPageIO.forVersion(ver);

                case T_BPLUS_REF3_LEAF:
                    return LeafPageIO.forVersion(ver);

                default:
                    throw new IgniteException("Unsupported page type: " + type);
            }
        }

        /** {@inheritDoc} */
        @Override public void initNewPage(ByteBuffer buf, long pageId) {
            super.initNewPage(buf, pageId);

            setCount(buf, 0);
            setForward(buf, 0);
        }

        /**
         * @param buf Buffer.
         * @return Forward page ID.
         */
        public long getForward(ByteBuffer buf) {
            return buf.getLong(FORWARD_OFF);
        }

        /**
         * @param buf Buffer.
         * @param pageId Forward page ID.
         */
        public void setForward(ByteBuffer buf, long pageId) {
            buf.putLong(FORWARD_OFF, pageId);

            assert getForward(buf) == pageId;
        }

        /**
         * @return {@code true} if it is a leaf page.
         */
        public final boolean isLeaf() {
            return getType() == T_BPLUS_REF3_LEAF;
        }

        /**
         * @param buf Buffer.
         * @return Max items count.
         */
        public abstract int getMaxCount(ByteBuffer buf);

        /**
         * @param buf Buffer.
         * @return Items count in the page.
         */
        public abstract int getCount(ByteBuffer buf);

        /**
         * @param buf Buffer.
         * @param cnt Count.
         */
        public abstract void setCount(ByteBuffer buf, int cnt);

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Link for the given index.
         */
        public abstract long getLink(ByteBuffer buf, int idx);

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @param link Link.
         */
        public abstract void setLink(ByteBuffer buf, int idx, long link);

        /**
         * @param src Source buffer.
         * @param dst Destination buffer.
         * @param srcIdx Source begin index.
         * @param dstIdx Destination begin index.
         * @param cnt Items count.
         */
        public abstract void copyItems(ByteBuffer src, ByteBuffer dst, int srcIdx, int dstIdx, int cnt);
    }

    /**
     * Inner page IO routines.
     */
    private static final class InnerPageIO extends IndexPageIO {
        /** */
        private static final InnerPageIO V1 = new InnerPageIO();

        /** */
        private static final int ITEM_SIZE = 16;

        /** */
        private static final int SHIFT_LEFT = ITEMS_OFF;

        /** */
        private static final int SHIFT_LINK = ITEMS_OFF + 8;

        /** */
        private static final int SHIFT_RIGHT = ITEMS_OFF + 16;

        /**
         * @param ver version.
         * @return IO.
         */
        public static InnerPageIO forVersion(int ver) {
            switch (ver){
                case 1:
                    return V1;

                default:
                    throw new IgniteException("Unsupported version: " + ver);
            }
        }

        /**
         * @return Latest.
         */
        public static InnerPageIO latest() {
            return V1;
        }

        /** {@inheritDoc} */
        @Override public int getType() {
            return T_BPLUS_REF3_INNER;
        }

        /** {@inheritDoc} */
        @Override public int getVersion() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public int getMaxCount(ByteBuffer buf) {
            //  (capacity - ITEMS_OFF - RIGHTEST_PAGE_ID_SLOT_SIZE) / ITEM_SIZE
            return (buf.capacity() - ITEMS_OFF - 8) >>> 4;
        }

        /** {@inheritDoc} */
        @Override public int getCount(ByteBuffer buf) {
            return buf.getShort(CNT_OFF) & 0xFFFF;
        }

        /** {@inheritDoc} */
        @Override public void setCount(ByteBuffer buf, int cnt) {
            buf.putShort(CNT_OFF, (short)cnt);
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx, SHIFT_LINK));
        }

        /** {@inheritDoc} */
        @Override public void setLink(ByteBuffer buf, int idx, long link) {
            buf.putLong(offset(idx, SHIFT_LINK), link);

            assert getLink(buf, idx) == link;
        }

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Page ID.
         */
        long getLeft(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx, SHIFT_LEFT));
        }

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @param pageId Page ID.
         */
        void setLeft(ByteBuffer buf, int idx, long pageId) {
            buf.putLong(offset(idx, SHIFT_LEFT), pageId);

            assert pageId == getLeft(buf, idx);
        }

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @return Page ID.
         */
        long getRight(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx, SHIFT_RIGHT));
        }

        /**
         * @param buf Buffer.
         * @param idx Index.
         * @param pageId Page ID.
         */
        void setRight(ByteBuffer buf, int idx, long pageId) {
            buf.putLong(offset(idx, SHIFT_RIGHT), pageId);

            assert pageId == getRight(buf, idx);
        }

        /** {@inheritDoc} */
        @Override public void copyItems(ByteBuffer src, ByteBuffer dst, int srcIdx, int dstIdx, int cnt) {
            assert src != dst || srcIdx < dstIdx;
            // TODO Optimize?
            for (int i = cnt - 1; i >= 0; i--) {
                dst.putLong(offset(dstIdx + i, SHIFT_RIGHT), src.getLong(offset(srcIdx + i, SHIFT_RIGHT)));
                dst.putLong(offset(dstIdx + i, SHIFT_LINK), src.getLong(offset(srcIdx + i, SHIFT_LINK)));
            }

            if (dstIdx == 0)
                dst.putLong(offset(0, SHIFT_LEFT), src.getLong(offset(srcIdx, SHIFT_LEFT)));
        }

        /**
         * @param idx Index of element.
         * @param shift It can be either link itself or left or right page ID.
         * @return Offset from byte buffer begin in bytes.
         */
        private static int offset(int idx, int shift) {
            return shift + ITEM_SIZE * idx;
        }
    }

    /**
     * Leaf page IO routines.
     */
    private static final class LeafPageIO extends IndexPageIO {
        /** */
        private static final LeafPageIO V1 = new LeafPageIO();

        /** */
        private static final int ITEM_SIZE = 8;

        /**
         * @param ver Version.
         * @return IO instance.
         */
        public static LeafPageIO forVersion(int ver) {
            switch (ver) {
                case 1:
                    return V1;

                default:
                    throw new IgniteException("Unsupported version: " + ver);
            }
        }

        /**
         * @return Latest.
         */
        public static LeafPageIO latest() {
            return V1;
        }

        /** {@inheritDoc} */
        @Override public int getType() {
            return T_BPLUS_REF3_LEAF;
        }

        /** {@inheritDoc} */
        @Override public int getVersion() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public int getMaxCount(ByteBuffer buf) {
            return (buf.capacity() - ITEMS_OFF) >>> 3; // divide by ITEM_SIZE
        }

        /** {@inheritDoc} */
        @Override public int getCount(ByteBuffer buf) {
            return buf.getShort(CNT_OFF) & 0xFFFF;
        }

        /** {@inheritDoc} */
        @Override public void setCount(ByteBuffer buf, int cnt) {
            buf.putShort(CNT_OFF, (short)cnt);

            assert getCount(buf) == cnt;
        }

        /** {@inheritDoc} */
        @Override public long getLink(ByteBuffer buf, int idx) {
            return buf.getLong(offset(idx));
        }

        /** {@inheritDoc} */
        @Override public void setLink(ByteBuffer buf, int idx, long link) {
            buf.putLong(offset(idx), link);

            assert getLink(buf, idx) == link;
        }

        /** {@inheritDoc} */
        @Override public void copyItems(ByteBuffer src, ByteBuffer dst, int srcIdx, int dstIdx, int cnt) {
            assert src != dst || dstIdx > srcIdx;

            // TODO Optimize
            for (int i = cnt - 1; i >= 0; i--)
                dst.putLong(offset(dstIdx + i), src.getLong(offset(srcIdx + i)));
        }

        /**
         * @param idx Index of item.
         * @return Offset.
         */
        private static int offset(int idx) {
            return ITEMS_OFF + idx * ITEM_SIZE;
        }
    }

    /**
     * Meta page IO.
     */
    private static class MetaPageIO extends PageIO {
        /** */
        static final MetaPageIO V1 = new MetaPageIO();

        /** */
        static final int ROOT_ID_OFF = COMMON_HEADER_END;

        /** */
        static final int ROOT_LEVEL_OFF = ROOT_ID_OFF + 8;

        /**
         * @param buf Buffer.
         * @return IO.
         */
        static MetaPageIO forPage(ByteBuffer buf) {
            assert getType(buf) == T_BPLUS_REF3_META;

            return forVersion(getVersion(buf));
        }

        /**
         * @param ver Version.
         * @return IO.
         */
        static MetaPageIO forVersion(int ver) {
            switch (ver) {
                case 1:
                    return V1;

                default:
                    throw new IgniteException("Unsupported version: " + ver);
            }
        }

        /**
         * @return Instance.
         */
        static MetaPageIO latest() {
            return V1;
        }

        /** {@inheritDoc} */
        @Override public int getType() {
            return T_BPLUS_REF3_META;
        }

        /** {@inheritDoc} */
        @Override public int getVersion() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public void initNewPage(ByteBuffer buf, long pageId) {
            super.initNewPage(buf, pageId);

            setRootPageId(buf, 0);
            setRootLevel(buf, 0);
        }

        /**
         * @param buf Buffer.
         * @return Root page ID.
         */
        public long getRootPageId(ByteBuffer buf) {
            return buf.getLong(ROOT_ID_OFF);
        }

        /**
         * @param buf Buffer.
         * @param rootPageId Root page ID.
         */
        public void setRootPageId(ByteBuffer buf, long rootPageId) {
            buf.putLong(ROOT_ID_OFF, rootPageId);

            assert getRootPageId(buf) == rootPageId;
        }

        /**
         * @param buf Buffer.
         * @return Level.
         */
        public int getRootLevel(ByteBuffer buf) {
            return buf.get(ROOT_LEVEL_OFF);
        }

        /**
         * @param buf Buffer.
         * @param lvl Level.
         */
        public void setRootLevel(ByteBuffer buf, int lvl) {
            assert lvl >= 0 && lvl < 33 : lvl;

            buf.put(ROOT_LEVEL_OFF, (byte)lvl);

            assert getRootLevel(buf) == lvl;
        }
    }

    /**
     * Page handler.
     */
    private abstract static class PageHandler<X> {
        /**
         * @param page Page.
         * @param buf Page buffer.
         * @param arg Argument.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteCheckedException If failed.
         */
        abstract int run(Page page, ByteBuffer buf, X arg, int lvl) throws IgniteCheckedException;

        /**
         * @param page Page.
         * @param arg Argument.
         * @return {@code true} If release.
         */
        protected boolean releaseAfterWrite(Page page, X arg) {
            return true;
        }
    }
}
