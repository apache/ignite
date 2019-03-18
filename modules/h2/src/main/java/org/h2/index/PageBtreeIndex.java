/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.HashSet;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.TableFilter;
import org.h2.util.MathUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This is the most common type of index, a b tree index.
 * Only the data of the indexed columns are stored in the index.
 */
public class PageBtreeIndex extends PageIndex {

    private static int memoryChangeRequired;

    private final PageStore store;
    private final RegularTable tableData;
    private final boolean needRebuild;
    private long rowCount;
    private int memoryPerPage;
    private int memoryCount;

    public PageBtreeIndex(RegularTable table, int id, String indexName,
            IndexColumn[] columns,
            IndexType indexType, boolean create, Session session) {
        initBaseIndex(table, id, indexName, columns, indexType);
        if (!database.isStarting() && create) {
            checkIndexColumnTypes(columns);
        }
        // int test;
        // trace.setLevel(TraceSystem.DEBUG);
        tableData = table;
        if (!database.isPersistent() || id < 0) {
            throw DbException.throwInternalError("" + indexName);
        }
        this.store = database.getPageStore();
        store.addIndex(this);
        if (create) {
            // new index
            rootPageId = store.allocatePage();
            // TODO currently the head position is stored in the log
            // it should not for new tables, otherwise redo of other operations
            // must ensure this page is not used for other things
            store.addMeta(this, session);
            PageBtreeLeaf root = PageBtreeLeaf.create(this, rootPageId, PageBtree.ROOT);
            store.logUndo(root, null);
            store.update(root);
        } else {
            rootPageId = store.getRootPageId(id);
            PageBtree root = getPage(rootPageId);
            rowCount = root.getRowCount();
        }
        this.needRebuild = create || (rowCount == 0 && store.isRecoveryRunning());
        if (trace.isDebugEnabled()) {
            trace.debug("opened {0} rows: {1}", getName() , rowCount);
        }
        memoryPerPage = (Constants.MEMORY_PAGE_BTREE + store.getPageSize()) >> 2;
    }

    @Override
    public void add(Session session, Row row) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} add {1}", getName(), row);
        }
        // safe memory
        SearchRow newRow = getSearchRow(row);
        try {
            addRow(newRow);
        } finally {
            store.incrementChangeCount();
        }
    }

    private void addRow(SearchRow newRow) {
        while (true) {
            PageBtree root = getPage(rootPageId);
            int splitPoint = root.addRowTry(newRow);
            if (splitPoint == -1) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("split {0}", splitPoint);
            }
            SearchRow pivot = root.getRow(splitPoint - 1);
            store.logUndo(root, root.data);
            PageBtree page1 = root;
            PageBtree page2 = root.split(splitPoint);
            store.logUndo(page2, null);
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(rootPageId);
            page2.setParentPageId(rootPageId);
            PageBtreeNode newRoot = PageBtreeNode.create(
                    this, rootPageId, PageBtree.ROOT);
            store.logUndo(newRoot, null);
            newRoot.init(page1, pivot, page2);
            store.update(page1);
            store.update(page2);
            store.update(newRoot);
            root = newRoot;
        }
        invalidateRowCount();
        rowCount++;
    }

    /**
     * Create a search row for this row.
     *
     * @param row the row
     * @return the search row
     */
    private SearchRow getSearchRow(Row row) {
        SearchRow r = table.getTemplateSimpleRow(columns.length == 1);
        r.setKeyAndVersion(row);
        for (Column c : columns) {
            int idx = c.getColumnId();
            r.setValue(idx, row.getValue(idx));
        }
        return r;
    }

    /**
     * Read the given page.
     *
     * @param id the page id
     * @return the page
     */
    PageBtree getPage(int id) {
        Page p = store.getPage(id);
        if (p == null) {
            PageBtreeLeaf empty = PageBtreeLeaf.create(this, id, PageBtree.ROOT);
            // could have been created before, but never committed
            store.logUndo(empty, null);
            store.update(empty);
            return empty;
        } else if (!(p instanceof PageBtree)) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "" + p);
        }
        return (PageBtree) p;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findNext(Session session, SearchRow first, SearchRow last) {
        return find(session, first, true, last);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session, first, false, last);
    }

    private Cursor find(Session session, SearchRow first, boolean bigger,
            SearchRow last) {
        if (SysProperties.CHECK && store == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        PageBtree root = getPage(rootPageId);
        PageBtreeCursor cursor = new PageBtreeCursor(session, this, last);
        root.find(cursor, first, bigger);
        return cursor;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        if (first) {
            // TODO optimization: this loops through NULL elements
            Cursor cursor = find(session, null, false, null);
            while (cursor.next()) {
                SearchRow row = cursor.getSearchRow();
                Value v = row.getValue(columnIds[0]);
                if (v != ValueNull.INSTANCE) {
                    return cursor;
                }
            }
            return cursor;
        }
        PageBtree root = getPage(rootPageId);
        PageBtreeCursor cursor = new PageBtreeCursor(session, this, null);
        root.last(cursor);
        cursor.previous();
        // TODO optimization: this loops through NULL elements
        do {
            SearchRow row = cursor.getSearchRow();
            if (row == null) {
                break;
            }
            Value v = row.getValue(columnIds[0]);
            if (v != ValueNull.INSTANCE) {
                return cursor;
            }
        } while (cursor.previous());
        return cursor;
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        return 10 * getCostRangeIndex(masks, tableData.getRowCount(session),
                filters, filter, sortOrder, false, allColumnsSet);
    }

    @Override
    public boolean needRebuild() {
        return needRebuild;
    }

    @Override
    public void remove(Session session, Row row) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} remove {1}", getName(), row);
        }
        // TODO invalidate row count
        // setChanged(session);
        if (rowCount == 1) {
            removeAllRows();
        } else {
            try {
                PageBtree root = getPage(rootPageId);
                root.remove(row);
                invalidateRowCount();
                rowCount--;
            } finally {
                store.incrementChangeCount();
            }
        }
    }

    @Override
    public void remove(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("remove");
        }
        removeAllRows();
        store.free(rootPageId);
        store.removeMeta(this, session);
    }

    @Override
    public void truncate(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("truncate");
        }
        removeAllRows();
        if (tableData.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        tableData.setRowCount(0);
    }

    private void removeAllRows() {
        try {
            PageBtree root = getPage(rootPageId);
            root.freeRecursive();
            root = PageBtreeLeaf.create(this, rootPageId, PageBtree.ROOT);
            store.removeFromCache(rootPageId);
            store.update(root);
            rowCount = 0;
        } finally {
            store.incrementChangeCount();
        }
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get a row from the main index.
     *
     * @param session the session
     * @param key the row key
     * @return the row
     */
    @Override
    public Row getRow(Session session, long key) {
        return tableData.getRow(session, key);
    }

    PageStore getPageStore() {
        return store;
    }

    @Override
    public long getRowCountApproximation() {
        return tableData.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return tableData.getDiskSpaceUsed();
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public void close(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("close");
        }
        // can not close the index because it might get used afterwards,
        // for example after running recovery
        try {
            writeRowCount();
        } finally {
            store.incrementChangeCount();
        }
    }

    /**
     * Read a row from the data page at the given offset.
     *
     * @param data the data
     * @param offset the offset
     * @param onlyPosition whether only the position of the row is stored
     * @param needData whether the row data is required
     * @return the row
     */
    SearchRow readRow(Data data, int offset, boolean onlyPosition,
            boolean needData) {
        synchronized (data) {
            data.setPos(offset);
            long key = data.readVarLong();
            if (onlyPosition) {
                if (needData) {
                    return tableData.getRow(null, key);
                }
                SearchRow row = table.getTemplateSimpleRow(true);
                row.setKey(key);
                return row;
            }
            SearchRow row = table.getTemplateSimpleRow(columns.length == 1);
            row.setKey(key);
            for (Column col : columns) {
                int idx = col.getColumnId();
                row.setValue(idx, data.readValue());
            }
            return row;
        }
    }

    /**
     * Get the complete row from the data index.
     *
     * @param key the key
     * @return the row
     */
    SearchRow readRow(long key) {
        return tableData.getRow(null, key);
    }

    /**
     * Write a row to the data page at the given offset.
     *
     * @param data the data
     * @param offset the offset
     * @param onlyPosition whether only the position of the row is stored
     * @param row the row to write
     */
    void writeRow(Data data, int offset, SearchRow row, boolean onlyPosition) {
        data.setPos(offset);
        data.writeVarLong(row.getKey());
        if (!onlyPosition) {
            for (Column col : columns) {
                int idx = col.getColumnId();
                data.writeValue(row.getValue(idx));
            }
        }
    }

    /**
     * Get the size of a row (only the part that is stored in the index).
     *
     * @param dummy a dummy data page to calculate the size
     * @param row the row
     * @param onlyPosition whether only the position of the row is stored
     * @return the number of bytes
     */
    int getRowSize(Data dummy, SearchRow row, boolean onlyPosition) {
        int rowsize = Data.getVarLongLen(row.getKey());
        if (!onlyPosition) {
            for (Column col : columns) {
                Value v = row.getValue(col.getColumnId());
                rowsize += dummy.getValueLen(v);
            }
        }
        return rowsize;
    }

    @Override
    public boolean canFindNext() {
        return true;
    }

    /**
     * The root page has changed.
     *
     * @param session the session
     * @param newPos the new position
     */
    void setRootPageId(Session session, int newPos) {
        store.removeMeta(this, session);
        this.rootPageId = newPos;
        store.addMeta(this, session);
        store.addIndex(this);
    }

    private void invalidateRowCount() {
        PageBtree root = getPage(rootPageId);
        root.setRowCountStored(PageData.UNKNOWN_ROWCOUNT);
    }

    @Override
    public void writeRowCount() {
        if (SysProperties.MODIFY_ON_WRITE && rootPageId == 0) {
            // currently creating the index
            return;
        }
        PageBtree root = getPage(rootPageId);
        root.setRowCountStored(MathUtils.convertLongToInt(rowCount));
    }

    /**
     * Check whether the given row contains data.
     *
     * @param row the row
     * @return true if it contains data
     */
    boolean hasData(SearchRow row) {
        return row.getValue(columns[0].getColumnId()) != null;
    }

    int getMemoryPerPage() {
        return memoryPerPage;
    }

    /**
     * The memory usage of a page was changed. The new value is used to adopt
     * the average estimated memory size of a page.
     *
     * @param x the new memory size
     */
    void memoryChange(int x) {
        if (memoryCount < Constants.MEMORY_FACTOR) {
            memoryPerPage += (x - memoryPerPage) / ++memoryCount;
        } else {
            memoryPerPage += (x > memoryPerPage ? 1 : -1) +
                    ((x - memoryPerPage) / Constants.MEMORY_FACTOR);
        }
    }

    /**
     * Check if calculating the memory is required.
     *
     * @return true if it is
     */
    static boolean isMemoryChangeRequired() {
        if (memoryChangeRequired-- <= 0) {
            memoryChangeRequired = 10;
            return true;
        }
        return false;
    }

}
