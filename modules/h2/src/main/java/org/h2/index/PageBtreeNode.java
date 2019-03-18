/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.api.DatabaseEventListener;
import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.util.Utils;

/**
 * A b-tree node page that contains index data. Format:
 * <ul>
 * <li>page type: byte</li>
 * <li>checksum: short</li>
 * <li>parent page id (0 for root): int</li>
 * <li>index id: varInt</li>
 * <li>count of all children (-1 if not known): int</li>
 * <li>entry count: short</li>
 * <li>rightmost child page id: int</li>
 * <li>entries (child page id: int, offset: short)</li>
 * </ul>
 * The row contains the largest key of the respective child,
 * meaning row[0] contains the largest key of child[0].
 */
public class PageBtreeNode extends PageBtree {

    private static final int CHILD_OFFSET_PAIR_LENGTH = 6;
    private static final int MAX_KEY_LENGTH = 10;

    private final boolean pageStoreInternalCount;

    /**
     * The page ids of the children.
     */
    private int[] childPageIds;

    private int rowCountStored = UNKNOWN_ROWCOUNT;

    private int rowCount = UNKNOWN_ROWCOUNT;

    private PageBtreeNode(PageBtreeIndex index, int pageId, Data data) {
        super(index, pageId, data);
        this.pageStoreInternalCount = index.getDatabase().
                getSettings().pageStoreInternalCount;
    }

    /**
     * Read a b-tree node page.
     *
     * @param index the index
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageBtreeIndex index, Data data, int pageId) {
        PageBtreeNode p = new PageBtreeNode(index, pageId, data);
        p.read();
        return p;
    }

    /**
     * Create a new b-tree node page.
     *
     * @param index the index
     * @param pageId the page id
     * @param parentPageId the parent page id
     * @return the page
     */
    static PageBtreeNode create(PageBtreeIndex index, int pageId,
            int parentPageId) {
        PageBtreeNode p = new PageBtreeNode(index, pageId, index.getPageStore()
                .createData());
        index.getPageStore().logUndo(p, null);
        p.parentPageId = parentPageId;
        p.writeHead();
        // 4 bytes for the rightmost child page id
        p.start = p.data.length() + 4;
        p.rows = SearchRow.EMPTY_ARRAY;
        if (p.pageStoreInternalCount) {
            p.rowCount = 0;
        }
        return p;
    }

    private void read() {
        data.reset();
        int type = data.readByte();
        data.readShortInt();
        this.parentPageId = data.readInt();
        onlyPosition = (type & Page.FLAG_LAST) == 0;
        int indexId = data.readVarInt();
        if (indexId != index.getId()) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected index:" + index.getId() +
                    "got:" + indexId);
        }
        rowCount = rowCountStored = data.readInt();
        entryCount = data.readShortInt();
        childPageIds = new int[entryCount + 1];
        childPageIds[entryCount] = data.readInt();
        rows = entryCount == 0 ? SearchRow.EMPTY_ARRAY : new SearchRow[entryCount];
        offsets = Utils.newIntArray(entryCount);
        for (int i = 0; i < entryCount; i++) {
            childPageIds[i] = data.readInt();
            offsets[i] = data.readShortInt();
        }
        check();
        start = data.length();
        written = true;
    }

    /**
     * Add a row. If it is possible this method returns -1, otherwise
     * the split point. It is always possible to add two rows.
     *
     * @param row the now to add
     * @return the split point of this page, or -1 if no split is required
     */
    private int addChildTry(SearchRow row) {
        if (entryCount < 4) {
            return -1;
        }
        int startData;
        if (onlyPosition) {
            // if we only store the position, we may at most store as many
            // entries as there is space for keys, because the current data area
            // might get larger when _removing_ a child (if the new key needs
            // more space) - and removing a child can't split this page
            startData = entryCount + 1 * MAX_KEY_LENGTH;
        } else {
            int rowLength = index.getRowSize(data, row, onlyPosition);
            int pageSize = index.getPageStore().getPageSize();
            int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
            startData = last - rowLength;
        }
        if (startData < start + CHILD_OFFSET_PAIR_LENGTH) {
            return entryCount / 2;
        }
        return -1;
    }

    /**
     * Add a child at the given position.
     *
     * @param x the position
     * @param childPageId the child
     * @param row the row smaller than the first row of the child and its
     *            children
     */
    private void addChild(int x, int childPageId, SearchRow row) {
        int rowLength = index.getRowSize(data, row, onlyPosition);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (last - rowLength < start + CHILD_OFFSET_PAIR_LENGTH) {
            readAllRows();
            onlyPosition = true;
            // change the offsets (now storing only positions)
            int o = pageSize;
            for (int i = 0; i < entryCount; i++) {
                o -= index.getRowSize(data, getRow(i), true);
                offsets[i] = o;
            }
            last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
            rowLength = index.getRowSize(data, row, true);
            if (SysProperties.CHECK && last - rowLength <
                    start + CHILD_OFFSET_PAIR_LENGTH) {
                throw DbException.throwInternalError();
            }
        }
        int offset = last - rowLength;
        if (entryCount > 0) {
            if (x < entryCount) {
                offset = (x == 0 ? pageSize : offsets[x - 1]) - rowLength;
            }
        }
        rows = insert(rows, entryCount, x, row);
        offsets = insert(offsets, entryCount, x, offset);
        add(offsets, x + 1, entryCount + 1, -rowLength);
        childPageIds = insert(childPageIds, entryCount + 1, x + 1, childPageId);
        start += CHILD_OFFSET_PAIR_LENGTH;
        if (pageStoreInternalCount) {
            if (rowCount != UNKNOWN_ROWCOUNT) {
                rowCount += offset;
            }
        }
        entryCount++;
        written = false;
        changeCount = index.getPageStore().getChangeCount();
    }

    @Override
    int addRowTry(SearchRow row) {
        while (true) {
            int x = find(row, false, true, true);
            PageBtree page = index.getPage(childPageIds[x]);
            int splitPoint = page.addRowTry(row);
            if (splitPoint == -1) {
                break;
            }
            SearchRow pivot = page.getRow(splitPoint - 1);
            index.getPageStore().logUndo(this, data);
            int splitPoint2 = addChildTry(pivot);
            if (splitPoint2 != -1) {
                return splitPoint2;
            }
            PageBtree page2 = page.split(splitPoint);
            readAllRows();
            addChild(x, page2.getPos(), pivot);
            index.getPageStore().update(page);
            index.getPageStore().update(page2);
            index.getPageStore().update(this);
        }
        updateRowCount(1);
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        return -1;
    }

    private void updateRowCount(int offset) {
        if (rowCount != UNKNOWN_ROWCOUNT) {
            rowCount += offset;
        }
        if (rowCountStored != UNKNOWN_ROWCOUNT) {
            rowCountStored = UNKNOWN_ROWCOUNT;
            index.getPageStore().logUndo(this, data);
            if (written) {
                writeHead();
            }
            index.getPageStore().update(this);
        }
    }

    @Override
    PageBtree split(int splitPoint) {
        int newPageId = index.getPageStore().allocatePage();
        PageBtreeNode p2 = PageBtreeNode.create(index, newPageId, parentPageId);
        index.getPageStore().logUndo(this, data);
        if (onlyPosition) {
            // TODO optimize: maybe not required
            p2.onlyPosition = true;
        }
        int firstChild = childPageIds[splitPoint];
        readAllRows();
        while (splitPoint < entryCount) {
            p2.addChild(p2.entryCount, childPageIds[splitPoint + 1], getRow(splitPoint));
            removeChild(splitPoint);
        }
        int lastChild = childPageIds[splitPoint - 1];
        removeChild(splitPoint - 1);
        childPageIds[splitPoint - 1] = lastChild;
        if (p2.childPageIds == null) {
            p2.childPageIds = new int[1];
        }
        p2.childPageIds[0] = firstChild;
        p2.remapChildren();
        return p2;
    }

    @Override
    protected void remapChildren() {
        for (int i = 0; i < entryCount + 1; i++) {
            int child = childPageIds[i];
            PageBtree p = index.getPage(child);
            p.setParentPageId(getPos());
            index.getPageStore().update(p);
        }
    }

    /**
     * Initialize the page.
     *
     * @param page1 the first child page
     * @param pivot the pivot key
     * @param page2 the last child page
     */
    void init(PageBtree page1, SearchRow pivot, PageBtree page2) {
        entryCount = 0;
        childPageIds = new int[] { page1.getPos() };
        rows = SearchRow.EMPTY_ARRAY;
        offsets = Utils.EMPTY_INT_ARRAY;
        addChild(0, page2.getPos(), pivot);
        if (pageStoreInternalCount) {
            rowCount = page1.getRowCount() + page2.getRowCount();
        }
        check();
    }

    @Override
    void find(PageBtreeCursor cursor, SearchRow first, boolean bigger) {
        int i = find(first, bigger, false, false);
        if (i > entryCount) {
            if (parentPageId == PageBtree.ROOT) {
                return;
            }
            PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
            next.find(cursor, first, bigger);
            return;
        }
        PageBtree page = index.getPage(childPageIds[i]);
        page.find(cursor, first, bigger);
    }

    @Override
    void last(PageBtreeCursor cursor) {
        int child = childPageIds[entryCount];
        index.getPage(child).last(cursor);
    }

    @Override
    PageBtreeLeaf getFirstLeaf() {
        int child = childPageIds[0];
        return index.getPage(child).getFirstLeaf();
    }

    @Override
    PageBtreeLeaf getLastLeaf() {
        int child = childPageIds[entryCount];
        return index.getPage(child).getLastLeaf();
    }

    @Override
    SearchRow remove(SearchRow row) {
        int at = find(row, false, false, true);
        // merge is not implemented to allow concurrent usage
        // TODO maybe implement merge
        PageBtree page = index.getPage(childPageIds[at]);
        SearchRow last = page.remove(row);
        index.getPageStore().logUndo(this, data);
        updateRowCount(-1);
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        if (last == null) {
            // the last row didn't change - nothing to do
            return null;
        } else if (last == row) {
            // this child is now empty
            index.getPageStore().free(page.getPos());
            if (entryCount < 1) {
                // no more children - this page is empty as well
                return row;
            }
            if (at == entryCount) {
                // removing the last child
                last = getRow(at - 1);
            } else {
                last = null;
            }
            removeChild(at);
            index.getPageStore().update(this);
            return last;
        }
        // the last row is in the last child
        if (at == entryCount) {
            return last;
        }
        int child = childPageIds[at];
        removeChild(at);
        // TODO this can mean only the position is now stored
        // should split at the next possible moment
        addChild(at, child, last);
        // remove and add swapped two children, fix that
        int temp = childPageIds[at];
        childPageIds[at] = childPageIds[at + 1];
        childPageIds[at + 1] = temp;
        index.getPageStore().update(this);
        return null;
    }

    @Override
    int getRowCount() {
        if (rowCount == UNKNOWN_ROWCOUNT) {
            int count = 0;
            for (int i = 0; i < entryCount + 1; i++) {
                int child = childPageIds[i];
                PageBtree page = index.getPage(child);
                count += page.getRowCount();
                index.getDatabase().setProgress(
                        DatabaseEventListener.STATE_SCAN_FILE,
                        index.getName(), count, Integer.MAX_VALUE);
            }
            rowCount = count;
        }
        return rowCount;
    }

    @Override
    void setRowCountStored(int rowCount) {
        if (rowCount < 0 && pageStoreInternalCount) {
            return;
        }
        this.rowCount = rowCount;
        if (rowCountStored != rowCount) {
            rowCountStored = rowCount;
            index.getPageStore().logUndo(this, data);
            if (written) {
                changeCount = index.getPageStore().getChangeCount();
                writeHead();
            }
            index.getPageStore().update(this);
        }
    }

    private void check() {
        if (SysProperties.CHECK) {
            for (int i = 0; i < entryCount + 1; i++) {
                int child = childPageIds[i];
                if (child == 0) {
                    DbException.throwInternalError();
                }
            }
        }
    }

    @Override
    public void write() {
        check();
        writeData();
        index.getPageStore().writePage(getPos(), data);
    }

    private void writeHead() {
        data.reset();
        data.writeByte((byte) (Page.TYPE_BTREE_NODE |
                (onlyPosition ? 0 : Page.FLAG_LAST)));
        data.writeShortInt(0);
        data.writeInt(parentPageId);
        data.writeVarInt(index.getId());
        data.writeInt(rowCountStored);
        data.writeShortInt(entryCount);
    }

    private void writeData() {
        if (written) {
            return;
        }
        readAllRows();
        writeHead();
        data.writeInt(childPageIds[entryCount]);
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(childPageIds[i]);
            data.writeShortInt(offsets[i]);
        }
        for (int i = 0; i < entryCount; i++) {
            index.writeRow(data, offsets[i], rows[i], onlyPosition);
        }
        written = true;
    }

    @Override
    void freeRecursive() {
        index.getPageStore().logUndo(this, data);
        index.getPageStore().free(getPos());
        for (int i = 0; i < entryCount + 1; i++) {
            int child = childPageIds[i];
            index.getPage(child).freeRecursive();
        }
    }

    private void removeChild(int i) {
        readAllRows();
        entryCount--;
        if (pageStoreInternalCount) {
            updateRowCount(-index.getPage(childPageIds[i]).getRowCount());
        }
        written = false;
        changeCount = index.getPageStore().getChangeCount();
        if (entryCount < 0) {
            DbException.throwInternalError("" + entryCount);
        }
        if (entryCount > i) {
            int startNext = i > 0 ? offsets[i - 1] : index.getPageStore().getPageSize();
            int rowLength = startNext - offsets[i];
            add(offsets, i, entryCount + 1, rowLength);
        }
        rows = remove(rows, entryCount + 1, i);
        offsets = remove(offsets, entryCount + 1, i);
        childPageIds = remove(childPageIds, entryCount + 2, i);
        start -= CHILD_OFFSET_PAIR_LENGTH;
    }

    /**
     * Set the cursor to the first row of the next page.
     *
     * @param cursor the cursor
     * @param pageId id of the next page
     */
    void nextPage(PageBtreeCursor cursor, int pageId) {
        int i;
        // TODO maybe keep the index in the child page (transiently)
        for (i = 0; i < entryCount + 1; i++) {
            if (childPageIds[i] == pageId) {
                i++;
                break;
            }
        }
        if (i > entryCount) {
            if (parentPageId == PageBtree.ROOT) {
                cursor.setCurrent(null, 0);
                return;
            }
            PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
            next.nextPage(cursor, getPos());
            return;
        }
        PageBtree page = index.getPage(childPageIds[i]);
        PageBtreeLeaf leaf = page.getFirstLeaf();
        cursor.setCurrent(leaf, 0);
    }

    /**
     * Set the cursor to the last row of the previous page.
     *
     * @param cursor the cursor
     * @param pageId id of the previous page
     */
    void previousPage(PageBtreeCursor cursor, int pageId) {
        int i;
        // TODO maybe keep the index in the child page (transiently)
        for (i = entryCount; i >= 0; i--) {
            if (childPageIds[i] == pageId) {
                i--;
                break;
            }
        }
        if (i < 0) {
            if (parentPageId == PageBtree.ROOT) {
                cursor.setCurrent(null, 0);
                return;
            }
            PageBtreeNode previous = (PageBtreeNode) index.getPage(parentPageId);
            previous.previousPage(cursor, getPos());
            return;
        }
        PageBtree page = index.getPage(childPageIds[i]);
        PageBtreeLeaf leaf = page.getLastLeaf();
        cursor.setCurrent(leaf, leaf.entryCount - 1);
    }


    @Override
    public String toString() {
        return "page[" + getPos() + "] b-tree node table:" +
                index.getId() + " entries:" + entryCount;
    }

    @Override
    public void moveTo(Session session, int newPos) {
        PageStore store = index.getPageStore();
        store.logUndo(this, data);
        PageBtreeNode p2 = PageBtreeNode.create(index, newPos, parentPageId);
        readAllRows();
        p2.rowCountStored = rowCountStored;
        p2.rowCount = rowCount;
        p2.childPageIds = childPageIds;
        p2.rows = rows;
        p2.entryCount = entryCount;
        p2.offsets = offsets;
        p2.onlyPosition = onlyPosition;
        p2.parentPageId = parentPageId;
        p2.start = start;
        store.update(p2);
        if (parentPageId == ROOT) {
            index.setRootPageId(session, newPos);
        } else {
            Page p = store.getPage(parentPageId);
            if (!(p instanceof PageBtreeNode)) {
                throw DbException.throwInternalError();
            }
            PageBtreeNode n = (PageBtreeNode) p;
            n.moveChild(getPos(), newPos);
        }
        for (int i = 0; i < entryCount + 1; i++) {
            int child = childPageIds[i];
            PageBtree p = index.getPage(child);
            p.setParentPageId(newPos);
            store.update(p);
        }
        store.free(getPos());
    }

    /**
     * One of the children has moved to a new page.
     *
     * @param oldPos the old position
     * @param newPos the new position
     */
    void moveChild(int oldPos, int newPos) {
        for (int i = 0; i < entryCount + 1; i++) {
            if (childPageIds[i] == oldPos) {
                index.getPageStore().logUndo(this, data);
                written = false;
                changeCount = index.getPageStore().getChangeCount();
                childPageIds[i] = newPos;
                index.getPageStore().update(this);
                return;
            }
        }
        throw DbException.throwInternalError(oldPos + " " + newPos);
    }

}