package org.apache.ignite.internal.processors.query.h2.database;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
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
public class BPlusTreeRefIndex extends PageMemoryIndex {
    /** */
    private PageMemory pageMem;

    /** */
    private GridCacheContext<?,?> cctx;

    /** */
    private CacheObjectContext coctx;

    /** */
    private final long rootPageId;

    /** */
    private volatile long lastDataPageId;

    /**
     * @param keyCol Key column.
     * @param valCol Value column.
     */
    public BPlusTreeRefIndex(
        GridCacheContext<?,?> cctx,
        PageMemory pageMem,
        FullPageId rootPageId,
        boolean initNew,
        int keyCol,
        int valCol,
        Table tbl,
        String name,
        boolean pk,
        IndexColumn[] cols
    ) throws IgniteCheckedException {
        super(keyCol, valCol);

        assert cctx.cacheId() == rootPageId.cacheId();

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
            try (Page meta = pageMem.page(rootPageId)) {
                ByteBuffer buf = meta.getForInitialWrite();

                MetaPageIO io = MetaPageIO.latest();

                io.initNewPage(buf, rootPageId.pageId());

                try (Page root = allocatePage(-1, FLAG_IDX)) {
                    LeafPageIO.latest().initNewPage(root.getForInitialWrite(), root.id());

                    io.setLevelsCount(buf, 1);
                    io.setLeftmostPageId(buf, 0, root.id());
                }
            }
        }

        this.rootPageId = rootPageId.pageId();
    }

    /**
     * @param meta Meta page.
     * @return Root page ID.
     */
    private long getRootPageId(Page meta) {
        return getLeftmostPageId(meta, Integer.MIN_VALUE);
    }

    /**
     * @return Root level.
     */
    private int getRootLevel(Page meta) {
        ByteBuffer buf = meta.getForRead();

        try {
            return MetaPageIO.forPage(buf).getRootLevel(buf);
        }
        finally {
            meta.releaseRead();
        }
    }

    /**
     * @param meta Meta page.
     * @param lvl Level.
     * @return Root page ID.
     */
    private long getLeftmostPageId(Page meta, int lvl) {
        ByteBuffer buf = meta.getForRead();

        try {
            MetaPageIO io = MetaPageIO.forPage(buf);

            if (lvl == Integer.MIN_VALUE)
                lvl = io.getRootLevel(buf);
            else
                assert lvl <= io.getRootLevel(buf);

            return io.getLeftmostPageId(buf, lvl);
        }
        finally {
            meta.releaseRead();
        }
    }

    /**
     * Must be called inside of write lock on the current root page.
     *
     * @param meta Meta.
     * @param lvl Level.
     * @param rootPageId Root page ID.
     */
    private void setRootPageId(Page meta, int lvl, long rootPageId) {
        ByteBuffer buf = meta.getForWrite();

        boolean ok = false;

        try {
            MetaPageIO io = MetaPageIO.forPage(buf);

            io.setLevelsCount(buf, lvl + 1);
            io.setLeftmostPageId(buf, lvl, rootPageId);

            ok = true;
        }
        finally {
            meta.releaseWrite(ok);
        }
    }

    /**
     * @param upper Upper bound.
     * @return Cursor.
     */
    private Cursor findNoLower(SearchRow upper) throws IgniteCheckedException {
        ForwardCursor cursor = new ForwardCursor(upper);

        long firstPageId;

        try (Page meta = page(rootPageId)) {
            firstPageId = getLeftmostPageId(meta, 0); // Level 0 is always bottom.
        }

        try (Page first = page(firstPageId)) {
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

        return cursor;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        try {
            if (lower == null)
                return findNoLower(upper);

            GetCursor g = new GetCursor(lower, upper);

            doFind(g);

            return g.cursor;
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row findOne(GridH2Row row) {
        GetOne g = new GetOne(row);

        try {
            doFind(g);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }

        return (GridH2Row)g.row;
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
            g.meta = page(rootPageId);

        int rootLvl;
        long rootId;

        ByteBuffer buf = g.meta.getForRead();

        try {
            MetaPageIO io = MetaPageIO.forPage(buf);

            rootLvl = io.getRootLevel(buf);
            rootId = io.getLeftmostPageId(buf, rootLvl);
        }
        finally {
            g.meta.releaseRead();
        }

        g.restart(rootId, rootLvl);
    }

    /**
     * @param g Get.
     */
    private void doFind(Get g) throws IgniteCheckedException {
        try {
            for (;;) { // Go down with retries.
                initOperation(g);

                if (!findDown(g, g.rootId, 0L, g.rootLvl))
                    break;

                checkInterrupted();
            }
        }
        finally {
            g.releaseMeta();
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
        try (Page page = page(pageId)) {
            int res;

            for (;;) {
                // Init args.
                g.pageId = pageId;
                g.expFwdId = expFwdId;

                res = readPage(page, search, g, lvl);

                switch (res) {
                    case Get.RETRY:
                        return true;

                    case Get.GO_DOWN:
                        assert g.pageId != pageId;
                        assert g.expFwdId != expFwdId || expFwdId == 0;

                        // Go down recursively.
                        if (findDown(g, g.pageId, g.expFwdId, lvl - 1)) {
                            checkInterrupted();

                            continue; // The child page got splitted, need to reread our page.
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
    }

    /**
     * @param expLastDataPageId Expected last data page ID.
     * @return Next data page ID.
     */
    private synchronized long nextDataPage(long expLastDataPageId) throws IgniteCheckedException {
        if (expLastDataPageId != lastDataPageId)
            return lastDataPageId;

        long pageId;

        // TODO we need a partition here
        try (Page page = allocatePage(-1, FLAG_DATA)) {
            pageId = page.id();

            ByteBuffer buf = page.getForInitialWrite();

            DataPageIO.latest().initNewPage(buf, page.id());
        }

        return lastDataPageId = pageId;
    }

    /**
     * @param row Row.
     */
    private void writeRowData(GridH2Row row) throws IgniteCheckedException {
        while (row.link == 0) {
            long pageId = lastDataPageId;

            if (pageId == 0)
                pageId = nextDataPage(0);

            try (Page page = page(pageId)) {
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

            nextDataPage(pageId);
        }
    }

    /**
     * @param keys Print keys.
     * @return Tree structure as a string.
     */
    private String printTree(boolean keys) {
        StringBuilder b = new StringBuilder();

        try (Page meta = page(rootPageId)) {
            printTree(getRootPageId(meta), "", true, b, keys);
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
        try (Page page = page(pageId)) {
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
    }

    /**
     * @param io IO.
     * @param buf Buffer.
     * @param lr Leftmost and rightmost children.
     * @param keys Keys.
     * @return String.
     * @throws IgniteCheckedException If failed.
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
     * @throws IgniteCheckedException If failed.
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
    @Override public GridH2Row remove(SearchRow row) {
        if (true)
            return null;

        Remove r = new Remove(row);

        try {
            // Find leaf page.
            findPageForRemove(r);

            if (r.isFinished())
                return null; // Not found.

            // Leaf page must be already locked for write.
            assert r.leafPage != null;
            assert r.leafBuf != null;

            // If we went always to the right then no inner page will contain this row.
            // Also in case of a single root page in the tree we will not have inner page.
            if (r.foundInnerPageId != 0) {
                // Try to optimistically lock found inner page.
                if (!r.tryLockPage(page(r.foundInnerPageId), false))
                    findPageForRemove(r);

                assert r.innerPage != null;
                assert r.innerBuf != null;
            }

            r.remove();

            assert r.removed != null;
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
        finally {
            r.releasePages();
            r.releaseMeta();
        }

        return r.removed;
    }

    /**
     * @param r Remove.
     * @throws IgniteCheckedException If failed.
     */
    private void findPageForRemove(Remove r) throws IgniteCheckedException {
        for (;;) {
            initOperation(r);

            if (!findPageForRemove(r, r.rootId, 0L, r.rootLvl))
                break;

            checkInterrupted();
        }
    }

    /**
     * @param r Remove.
     * @param pageId Page ID.
     * @param expFwdId Expected forward page ID.
     * @param lvl Level.
     * @return {@code true} If need to retry.
     * @throws IgniteCheckedException If failed.
     */
    private boolean findPageForRemove(Remove r, final long pageId, final long expFwdId, final int lvl)
        throws IgniteCheckedException {
        assert pageId != 0;

        for (;;) {
            try (Page page = page(pageId)) {
                if (page == null)
                    return true; // Retry.

                // Init args.
                r.pageId = pageId;
                r.expFwdId = expFwdId;

                int res = readPage(page, search, r, lvl);

                switch (res) {
                    case Remove.RETRY:
                        return true;

                    case Remove.NOT_FOUND:
                        assert lvl == 0 : lvl; // We must be at leaf.

                        return false;

                    case Remove.GO_DOWN:
                        assert r.pageId != pageId;
                        assert r.expFwdId != expFwdId || expFwdId == 0;

                        if (findPageForRemove(r, r.pageId, r.expFwdId, lvl + 1)) {
                            checkInterrupted();

                            continue;
                        }
                }

                assert res == Remove.FOUND : res;


            }
        }
    }


    /** {@inheritDoc} */
    @SuppressWarnings("StatementWithEmptyBody")
    @Override public GridH2Row put(GridH2Row row) {
        Put p = new Put(row);

        try {
            if (getIndexType().isPrimaryKey()) // TODO move out of index
                writeRowData(row); // Write data if not already written.

            assert row.link != 0;

            for (;;) { // Go down with retries.
                initOperation(p);

                if (!putDown(p, p.rootId, 0L, p.rootLvl))
                    break;

                checkInterrupted();
            }

            // Go up and finish if needed (root got concurrently splitted).
            if (!p.isFinished())
                putUp(p, p.rootLvl + 1);

            assert p.isFinished();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            p.releaseMeta();
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

        long pageId = getLeftmostPageId(p.meta, lvl);

        for (;;) {
            try (Page page = page(pageId)) {
                int res = writePage(page, handler, p, lvl);

                switch (res) {
                    case Put.FINISH:
                        if (p.isFinished())
                            return; // We are done.

                        pageId = getLeftmostPageId(p.meta, ++lvl); // Go up to new root.

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

            checkInterrupted();
        }
    }

    /**
     * @param io IO.
     * @param buf Splitting buffer.
     * @param fwdBuf Forward buffer.
     * @throws IgniteCheckedException If failed.
     */
    private void splitPage(IndexPageIO io, ByteBuffer buf, ByteBuffer fwdBuf)
        throws IgniteCheckedException {
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
    private GridH2Row insertWithSplit(Page meta, IndexPageIO io, final ByteBuffer buf, GridH2Row row,
        int idx, long rightId, int lvl) throws IgniteCheckedException {
        try (Page fwdPage = allocatePage(-1, FLAG_IDX)) {
            // Need to check this before the actual split, because after the split we will have new forward page here.
            boolean hadFwd = io.getForward(buf) != 0;

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

            if (!hadFwd && lvl == getRootLevel(meta)) { // We are splitting root.
                long newRootId;

                try (Page newRoot = allocatePage(-1, FLAG_IDX)) {
                    newRootId = newRoot.id();

                    if (io.isLeaf())
                        io = InnerPageIO.latest();

                    ByteBuffer newRootBuf = newRoot.getForInitialWrite();

                    io.initNewPage(newRootBuf, newRoot.id());

                    io.setCount(newRootBuf, 1);
                    inner(io).setLeft(newRootBuf, 0, PageIO.getPageId(buf));
                    io.setLink(newRootBuf, 0, moveUpLink);
                    inner(io).setRight(newRootBuf, 0, fwdPage.id());
                }

                setRootPageId(meta, lvl + 1, newRootId);

                return null; // We've just moved link up to root, nothing to return here.
            }
            else { // Regular split.
                row = getRow(moveUpLink);
                row.link = moveUpLink;

                return row;
            }
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
    private GridH2Row insert(Page meta, IndexPageIO io, ByteBuffer buf, GridH2Row row, int idx, long rightId, int lvl)
        throws IgniteCheckedException {
        int maxCnt = io.getMaxCount(buf);
        int cnt = io.getCount(buf);

        if (cnt == maxCnt) // Need to split page.
            return insertWithSplit(meta, io, buf, row, idx, rightId, lvl);

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

            // Replace link at idx with new one.
            // Need to read link here because `p.finish()` will clear row.
            long newLink = p.row().link;

            assert newLink != 0;

            if (io.isLeaf()) { // Get old row in leaf page to reduce contention at upper level.
                assert p.oldRow == null;

                long oldLink = io.getLink(buf, idx);

                p.oldRow = getRow(oldLink);
                p.oldRow.link = oldLink;

                p.finish();
                // We can't erase data page here because it can be referred from other indexes.
            }

            io.setLink(buf, idx, newLink);

            return io.isLeaf() ? Put.FINISH : Put.FOUND;
        }

        @Override protected boolean releaseAfterWrite(Page page, Put p) {
            return p.tailLock != page;
        }

        @Override public String toString() {
            return "replace";
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
                    // This is an upper insert after split downstairs.
                    assert p.split : "split";
                    assert p.tailLock != null : "tail lock must be kept";
                    assert cnt > 0 : cnt; // We have a locked tailLock which is our child, we can't become empty.

                    // `tailLock` page (the page that we've splitted downstairs) must be the rightmost child to insert
                    // split row here.
                    // Proof:
                    // - `tailLock` page is locked and can't be removed from this page other way than split;
                    // - our split key is known to be greater than all the other keys in this page, then there are two
                    //   possible cases:
                    //     1. `tailLock` page is the rightmost child. It means that we went right last time, because
                    //        insertion key was already greater than all the keys in this page. Or it became like this
                    //        after split of this page, but we don't care because `triangle` invariant guaranties
                    //        that all the keys that were moved to `forward` at this level are greater than our split
                    //        key, thus it is safe to insert split key here (moreover we can't insert it into `forward`
                    //        because we have `right` reference but there we will need `left`).
                    //     2. `tailLock` page is not the rightmost child. It can't be in this page, because then this
                    //        page must contain key which is greater than all keys in our splitted `tailLock` page
                    //        including our split key (which was in the middle of two splitted pages), but this is
                    //        impossible because the split key is known to be greater than everyone here.
                    //        Thus we know that `tailLock` reference was moved to the `forward` page (may be already
                    //        multiple times) due to split and we need to go forward and catch up.
                    if (p.tailLock.id() != inner(io).getRight(buf, cnt - 1)) {
                        p.pageId = io.getForward(buf);

                        assert p.pageId != 0;

                        return Put.NOT_FOUND; // Go forward.
                    }
                }
            }

            // Do insert.
            GridH2Row moveUpRow = insert(p.meta, io, buf, p.row(), idx, p.rightId, lvl);

            // Check if split happened.
            if (moveUpRow != null) {
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

        @Override public String toString() {
            return "insert";
        }
    };

    /** */
    private final PageHandler<Get> search = new PageHandler<Get>() {
        @Override public int run(Page page, ByteBuffer buf, Get g, int lvl) throws IgniteCheckedException {
            IndexPageIO io = IndexPageIO.forPage(buf);

            int cnt = io.getCount(buf);
            int idx = findInsertionPoint(io, buf, cnt, g.row);

            boolean found = idx >= 0;

            if (found) { // Found exact match.
                if (g.found(io, buf, idx))
                    return Get.FOUND;

                assert !io.isLeaf();

                // Else we need to reach leaf, go left down.
            }
            else {
                idx = -idx - 1;

                // If we are on the right edge, then check for expected forward page and retry of it does match.
                // It means that concurrent split happened. This invariant is referred as `triangle`.
                if (idx == cnt && io.getForward(buf) != g.expFwdId)
                    return Get.RETRY;

                if (io.isLeaf()) { // No way down, stop here.
                    assert g.pageId == page.id();

                    g.notFound(io, buf, idx);

                    return Get.NOT_FOUND;
                }
            }

            // If idx == cnt then we go right down, else left down.
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

            if (found && g.isPut()) {
                // This is a replace on inner page.
                assert !io.isLeaf();
                assert g instanceof Put;

                return Put.REPLACE_AND_GO_DOWN;
            }

            return Get.GO_DOWN;
        }

        @Override public String toString() {
            return "search";
        }
    };

    /**
     * @param pageId Inner page ID.
     * @return Leftmost child page ID.
     */
    private long getLeftmostChild(long pageId) throws IgniteCheckedException {
        try (Page page = page(pageId)) {
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

        Page page = page(pageId);

        try {
            int res;

            for (;;) {
                // Init args.
                p.pageId = pageId;
                p.expFwdId = expFwdId;

                res = readPage(page, search, p, lvl);

                switch (res) {
                    case Put.RETRY:
                        return true; // Retry.

                    case Put.REPLACE_AND_GO_DOWN:
                        int res0 = writePage(page, replace, p, lvl);

                        switch (res0) {
                            case Put.NOT_FOUND:
                                return true; // Retry.

                            case Put.FOUND:
                                break;

                            default:
                                throw new IllegalStateException("Illegal operation result: " + res0);
                        }

                        // Intentional fallthrough.

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

                        assert p.split : "if we did not finish here, it must be a split.";

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
                page.close();
        }

        // We've failed to insert/replace in this page, need to go forward until we catch up with split.
        for (;;) {
            assert p.pageId != pageId;

            page = page(p.pageId);

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
                    page.close();
            }
        }
    }

    /**
     * Get operation.
     */
    private static abstract class Get {
        /** */
        static final int GO_DOWN = 1;

        /** */
        static final int RETRY = 5;

        /** */
        static final int NOT_FOUND = 7;

        /** */
        static final int FOUND = 8;

        /** Starting point root level. May be outdated. Must be modified only in {@link #initOperation(Get)}. */
        int rootLvl;

        /** Starting point root ID. May be outdated. Must be modified only in {@link #initOperation(Get)}. */
        long rootId;

        /** Meta page. Initialized by {@link #initOperation(Get)}, released by {@link Get#releaseMeta()}. */
        Page meta;

        /** */
        SearchRow row;

        /** In/Out parameter: Page ID. */
        long pageId;

        /** In/Out parameter: expected forward page ID. */
        long expFwdId;

        /**
         * @param row Row.
         */
        public Get(SearchRow row) {
            assert row != null;

            this.row = row;
        }

        /**
         * @param rootId Root page ID.
         * @param rootLvl Root level.
         */
        void restart(long rootId, int rootLvl) {
            this.rootId = rootId;
            this.rootLvl = rootLvl;
        }

        /**
         * @return {@code true} If this is a {@link Put} operation.
         */
        boolean isPut() {
            return false;
        }

        /**
         * @param io IO.
         * @param buf Buffer.
         * @param idx Index of found entry.
         * @return {@code true} If we need to stop.
         * @throws IgniteCheckedException If failed.
         */
        abstract boolean found(IndexPageIO io, ByteBuffer buf, int idx) throws IgniteCheckedException;

        /**
         * @param io IO.
         * @param buf Buffer.
         * @param idx Insertion point.
         */
        void notFound(IndexPageIO io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            // No-op.
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
    }

    /**
     * Get a single entry.
     */
    private class GetOne extends Get {
        /**
         * @param row Row.
         */
        public GetOne(SearchRow row) {
            super(row);
        }

        /** {@inheritDoc} */
        @Override boolean found(IndexPageIO io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            row = getRow(io.getLink(buf, idx));

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
         * @param lower Row.
         */
        public GetCursor(SearchRow lower, SearchRow upper) {
            super(lower);

            cursor = new ForwardCursor(upper);
        }

        /** {@inheritDoc} */
        @Override boolean found(IndexPageIO io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            if (!io.isLeaf())
                return false;

            cursor.bootstrap(buf, idx);

            return true;
        }

        /** {@inheritDoc} */
        @Override void notFound(IndexPageIO io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            assert io.isLeaf();

            cursor.bootstrap(buf, idx);
        }
    }

    /**
     * Put operation.
     */
    private static class Put extends Get {
        /** */
        static final int REPLACE_AND_GO_DOWN = 3;

        /** */
        static final int FINISH = Integer.MAX_VALUE;

        /** Right child page ID for split row. */
        long rightId;

        /** Replaced row if any. */
        GridH2Row oldRow;

        /**
         * This page is kept locked after split until insert to the upper level will not be finished.
         * It is needed to avoid excessive spinning which will happen while the following `triangle` invariant
         * of page split is not met: parent page must have `right` child reference the same as `forward`
         * reference of `left` child.
         * Some other split invariants rely on this locking behavior.
         */
        Page tailLock;

        /** Split happened. */
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

        /** {@inheritDoc} */
        @Override boolean isPut() {
            return true;
        }

        /** {@inheritDoc} */
        @Override boolean found(IndexPageIO io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            return io.isLeaf();
        }

        /**
         * @param tailLock Tail lock.
         */
        private void tailLock(Page tailLock) {
            if (this.tailLock != null) {
                this.tailLock.releaseWrite(true);
                this.tailLock.close();
            }
            this.tailLock = tailLock;
        }

        /**
         * Finish put.
         */
        private void finish() {
            row = null;
            rightId = 0;
            tailLock(null);
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
        /** */
        long foundInnerPageId;

        /** Removed row. */
        GridH2Row removed;

        /** */
        Page leafPage;

        /** */
        ByteBuffer leafBuf;

        /** */
        int leafIdx;

        /** */
        Page innerPage;

        /** */
        ByteBuffer innerBuf;

        /** */
        int innerIdx;

        /**
         * @param row Row.
         */
        public Remove(SearchRow row) {
            super(row);
        }

        /** {@inheritDoc} */
        @Override boolean found(IndexPageIO io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            if (!io.isLeaf())
                foundInnerPageId = PageIO.getPageId(buf);

            return io.isLeaf() || leafPage != null;
        }

        /** {@inheritDoc} */
        @Override void notFound(IndexPageIO io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            assert io.isLeaf();

            row = null; // Finish.
        }

        /**
         * @param page Page.
         * @param leaf If it is a leaf page.
         * @return {@code true} If locked successfully and found the needed row.
         * @throws IgniteCheckedException
         */
        boolean tryLockPage(Page page, boolean leaf) throws IgniteCheckedException {
            if (page == null)
                return false;

            ByteBuffer buf = page.getForWrite();

            IndexPageIO io = IndexPageIO.forPage(buf);

            int idx = findInsertionPoint(io, buf, io.getCount(buf), row);

            if (idx < 0) {
                page.close();

                return false;
            }

            if (leaf) {
                leafPage = page;
                leafBuf = buf;
                leafIdx = idx;
            }
            else {
                innerPage = page;
                innerBuf = buf;
                innerIdx = idx;
            }

            return true;
        }

        /**
         * @return {@code true} If finished.
         */
        public boolean isFinished() {
            return row == null; // TODO
        }

        /**
         * Release pages.
         */
        public void releasePages() {
            if (leafPage != null) {
                leafPage.close();

                if (innerPage != null)
                    innerPage.close();
            }
        }

        /**
         * Remove link from pages.
         * @throws IgniteCheckedException If failed.
         */
        public void remove() throws IgniteCheckedException {
            IndexPageIO io = IndexPageIO.forPage(leafBuf);

            removed =  getRow(io.getLink(leafBuf, leafIdx));

            int cnt = io.getCount(leafBuf);

            cnt--;

            if (leafIdx != cnt)
                io.copyItems(leafBuf, leafBuf, leafIdx + 1, leafIdx, cnt - leafIdx);

            io.setCount(leafBuf, cnt);

            if (innerPage != null) {
                long nextGreatest = 0;

                // If page is not free, we have to replace link in inner page with next greatest row in left sub-tree.
                if (cnt > 0) {
                    nextGreatest = io.getLink(leafBuf, cnt - 1);

                    assert nextGreatest != 0;
                }

                io = IndexPageIO.forPage(innerBuf);

                if (nextGreatest != 0)
                    io.setLink(innerBuf, innerIdx, nextGreatest);
                else {
                    // If it was the last link in the leaf page, then we can drop it, because it is
                    // TODO
                }
            }
        }
    }

    /**
     * @param part Partition.
     * @param flag Flag.
     * @return Allocated page ID.
     */
    private Page allocatePage(int part, byte flag) throws IgniteCheckedException {
        FullPageId pageId = pageMem.allocatePage(cctx.cacheId(), part, flag);

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

    /**
     * !!! This method must be invoked in read or write lock of referring index page. It is needed to
     * !!! make sure that row at this link will be invisible, when the link will be removed from
     * !!! from all the index pages, so that row can be safely erased from the data page.
     *
     * @param link Link.
     * @return Row.
     */
    private GridH2Row getRow(long link) throws IgniteCheckedException {
        CacheObject key;
        CacheObject val;
        GridCacheVersion ver;

        try (Page page = page(pageId(link))) {
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

    private Page page(long pageId) throws IgniteCheckedException {
        return pageMem.page(new FullPageId(pageId, cctx.cacheId()));
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
                    page = page(fwdId);
                    buf = page.getForRead();
                }
                else { // Clear.
                    page = null;
                    buf = null;
                }

                if (prevPage != null) { // Release previous page.
                    prevPage.releaseRead();
                    prevPage.close();
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
    private static abstract class IndexPageIO extends PageIO {
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
            //  (capacity - ITEMS_OFF - RIGHTMOST_PAGE_ID_SLOT_SIZE) / ITEM_SIZE
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
            assert srcIdx != dstIdx;

            if (dstIdx > srcIdx) {
                for (int i = cnt - 1; i >= 0; i--) {
                    dst.putLong(offset(dstIdx + i, SHIFT_RIGHT), src.getLong(offset(srcIdx + i, SHIFT_RIGHT)));
                    dst.putLong(offset(dstIdx + i, SHIFT_LINK), src.getLong(offset(srcIdx + i, SHIFT_LINK)));
                }

                if (dstIdx == 0)
                    dst.putLong(offset(0, SHIFT_LEFT), src.getLong(offset(srcIdx, SHIFT_LEFT)));
            }
            else {
                if (dstIdx == 0)
                    dst.putLong(offset(0, SHIFT_LEFT), src.getLong(offset(srcIdx, SHIFT_LEFT)));

                for (int i = 0; i < cnt; i++) {
                    dst.putLong(offset(dstIdx + i, SHIFT_RIGHT), src.getLong(offset(srcIdx + i, SHIFT_RIGHT)));
                    dst.putLong(offset(dstIdx + i, SHIFT_LINK), src.getLong(offset(srcIdx + i, SHIFT_LINK)));
                }
            }
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
            assert srcIdx != dstIdx;

            if (dstIdx > srcIdx) {
                for (int i = cnt - 1; i >= 0; i--)
                    dst.putLong(offset(dstIdx + i), src.getLong(offset(srcIdx + i)));
            }
            else {
                for (int i = 0; i < cnt; i++)
                    dst.putLong(offset(dstIdx + i), src.getLong(offset(srcIdx + i)));
            }
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
        static final int LVLS_OFF = COMMON_HEADER_END;

        /** */
        static final int REFS_OFF = LVLS_OFF + 1;

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

            setLevelsCount(buf, 0);
        }

        /**
         * @param buf Buffer.
         * @return Number of levels in this tree.
         */
        public int getLevelsCount(ByteBuffer buf) {
            return buf.get(LVLS_OFF);
        }

        /**
         * @param buf  Buffer.
         * @param lvls Number of levels in this tree.
         */
        public void setLevelsCount(ByteBuffer buf, int lvls) {
            assert lvls >= 0 && lvls < 30;

            buf.put(LVLS_OFF, (byte)lvls);

            assert getLevelsCount(buf) == lvls;
        }

        /**
         * @param lvl Level.
         * @return Offset for page reference.
         */
        private static int offset(int lvl) {
            return lvl * 8 + REFS_OFF;
        }

        /**
         * @param buf Buffer.
         * @param lvl Level.
         * @return Page reference at that level.
         */
        public long getLeftmostPageId(ByteBuffer buf, int lvl) {
            return buf.getLong(offset(lvl));
        }

        /**
         * @param buf    Buffer.
         * @param lvl    Level.
         * @param pageId Page ID.
         */
        public void setLeftmostPageId(ByteBuffer buf, int lvl, long pageId) {
            assert lvl >= 0 && lvl < getLevelsCount(buf);

            buf.putLong(offset(lvl), pageId);

            assert getLeftmostPageId(buf, lvl) == pageId;
        }

        /**
         * @param buf Buffer.
         * @return Root level.
         */
        public int getRootLevel(ByteBuffer buf) {
            int lvls = getLevelsCount(buf); // The highest level page is root.

            assert lvls > 0 : lvls;

            return lvls - 1;
        }
    }
    /**
     * Page handler.
     */
    private static abstract class PageHandler<X> {
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
