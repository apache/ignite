/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.api.ErrorCode;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.PageStoreTable;
import org.h2.table.TableFilter;
import org.h2.util.MathUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * The scan index allows to access a row by key. It can be used to iterate over
 * all rows of a table. Each regular table has one such object, even if no
 * primary key or indexes are defined.
 */
public class PageDataIndex extends PageIndex {

    private final PageStore store;
    private final PageStoreTable tableData;
    private long lastKey;
    private long rowCount;
    private int mainIndexColumn = -1;
    private DbException fastDuplicateKeyException;

    /**
     * The estimated heap memory per page, in number of double words (4 bytes
     * each).
     */
    private int memoryPerPage;
    private int memoryCount;

    public PageDataIndex(PageStoreTable table, int id, IndexColumn[] columns,
            IndexType indexType, boolean create, Session session) {
        super(table, id, table.getName() + "_DATA", columns, indexType);

        // trace = database.getTrace(Trace.PAGE_STORE + "_di");
        // trace.setLevel(TraceSystem.DEBUG);
        tableData = table;
        this.store = database.getPageStore();
        store.addIndex(this);
        if (!database.isPersistent()) {
            throw DbException.throwInternalError(table.getName());
        }
        if (create) {
            rootPageId = store.allocatePage();
            store.addMeta(this, session);
            PageDataLeaf root = PageDataLeaf.create(this, rootPageId, PageData.ROOT);
            store.update(root);
        } else {
            rootPageId = store.getRootPageId(id);
            PageData root = getPage(rootPageId, 0);
            lastKey = root.getLastKey();
            rowCount = root.getRowCount();
        }
        if (trace.isDebugEnabled()) {
            trace.debug("{0} opened rows: {1}", this, rowCount);
        }
        table.setRowCount(rowCount);
        memoryPerPage = (Constants.MEMORY_PAGE_DATA + store.getPageSize()) >> 2;
    }

    @Override
    public DbException getDuplicateKeyException(String key) {
        if (fastDuplicateKeyException == null) {
            fastDuplicateKeyException = super.getDuplicateKeyException(null);
        }
        return fastDuplicateKeyException;
    }

    @Override
    public void add(Session session, Row row) {
        boolean retry = false;
        if (mainIndexColumn != -1) {
            row.setKey(row.getValue(mainIndexColumn).getLong());
        } else {
            if (row.getKey() == 0) {
                row.setKey((int) ++lastKey);
                retry = true;
            }
        }
        if (tableData.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                Value v2 = v.copy(database, getId());
                if (v2.isLinkedToTable()) {
                    session.removeAtCommitStop(v2);
                }
                if (v != v2) {
                    row.setValue(i, v2);
                }
            }
        }
        // when using auto-generated values, it's possible that multiple
        // tries are required (specially if there was originally a primary key)
        if (trace.isDebugEnabled()) {
            trace.debug("{0} add {1}", getName(), row);
        }
        long add = 0;
        while (true) {
            try {
                addTry(session, row);
                break;
            } catch (DbException e) {
                if (e != fastDuplicateKeyException) {
                    throw e;
                }
                if (!retry) {
                    throw getNewDuplicateKeyException();
                }
                if (add == 0) {
                    // in the first re-try add a small random number,
                    // to avoid collisions after a re-start
                    row.setKey((long) (row.getKey() + Math.random() * 10_000));
                } else {
                    row.setKey(row.getKey() + add);
                }
                add++;
            } finally {
                store.incrementChangeCount();
            }
        }
        lastKey = Math.max(lastKey, row.getKey());
    }

    public DbException getNewDuplicateKeyException() {
        StringBuilder builder = new StringBuilder("PRIMARY KEY ON ");
        table.getSQL(builder, false);
        if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
            builder.append('(');
            indexColumns[mainIndexColumn].getSQL(builder, false).append(')');
        }
        DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, builder.toString());
        e.setSource(this);
        return e;
    }

    private void addTry(Session session, Row row) {
        while (true) {
            PageData root = getPage(rootPageId, 0);
            int splitPoint = root.addRowTry(row);
            if (splitPoint == -1) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("{0} split", this);
            }
            long pivot = splitPoint == 0 ? row.getKey() : root.getKey(splitPoint - 1);
            PageData page1 = root;
            PageData page2 = root.split(splitPoint);
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(rootPageId);
            page2.setParentPageId(rootPageId);
            PageDataNode newRoot = PageDataNode.create(this, rootPageId, PageData.ROOT);
            newRoot.init(page1, pivot, page2);
            store.update(page1);
            store.update(page2);
            store.update(newRoot);
            root = newRoot;
        }
        row.setDeleted(false);
        invalidateRowCount();
        rowCount++;
        store.logAddOrRemoveRow(session, tableData.getId(), row, true);
    }

    /**
     * Read an overflow page.
     *
     * @param id the page id
     * @return the page
     */
    PageDataOverflow getPageOverflow(int id) {
        Page p = store.getPage(id);
        if (p instanceof PageDataOverflow) {
            return (PageDataOverflow) p;
        }
        throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                p == null ? "null" : p.toString());
    }

    /**
     * Read the given page.
     *
     * @param id the page id
     * @param parent the parent, or -1 if unknown
     * @return the page
     */
    PageData getPage(int id, int parent) {
        Page pd = store.getPage(id);
        if (pd == null) {
            PageDataLeaf empty = PageDataLeaf.create(this, id, parent);
            // could have been created before, but never committed
            store.logUndo(empty, null);
            store.update(empty);
            return empty;
        } else if (!(pd instanceof PageData)) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, String.valueOf(pd));
        }
        PageData p = (PageData) pd;
        if (parent != -1) {
            if (p.getParentPageId() != parent) {
                throw DbException.throwInternalError(p +
                        " parent " + p.getParentPageId() + " expected " + parent);
            }
        }
        return p;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    /**
     * Get the key from the row.
     *
     * @param row the row
     * @param ifEmpty the value to use if the row is empty
     * @param ifNull the value to use if the column is NULL
     * @return the key
     */
    long getKey(SearchRow row, long ifEmpty, long ifNull) {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(mainIndexColumn);
        if (v == null) {
            return row.getKey();
        } else if (v == ValueNull.INSTANCE) {
            return ifNull;
        }
        return v.getLong();
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        long from = first == null ? Long.MIN_VALUE : first.getKey();
        long to = last == null ? Long.MAX_VALUE : last.getKey();
        PageData root = getPage(rootPageId, 0);
        return root.find(session, from, to);

    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @return the cursor
     */
    Cursor find(Session session, long first, long last) {
        PageData root = getPage(rootPageId, 0);
        return root.find(session, first, last);
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.throwInternalError(toString());
    }

    long getLastKey() {
        PageData root = getPage(rootPageId, 0);
        return root.getLastKey();
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        // The +200 is so that indexes that can return the same data, but have less
        // columns, will take precedence. This all works out easier in the MVStore case,
        // because MVStore uses the same cost calculation code for the ScanIndex (i.e.
        // the MVPrimaryIndex) and all other indices.
        return 10 * (tableData.getRowCountApproximation() +
                Constants.COST_ROW_OFFSET) + 200;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void remove(Session session, Row row) {
        if (tableData.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v.isLinkedToTable()) {
                    session.removeAtCommit(v);
                }
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("{0} remove {1}", getName(), row);
        }
        if (rowCount == 1) {
            removeAllRows();
        } else {
            try {
                long key = row.getKey();
                PageData root = getPage(rootPageId, 0);
                root.remove(key);
                invalidateRowCount();
                rowCount--;
            } finally {
                store.incrementChangeCount();
            }
        }
        store.logAddOrRemoveRow(session, tableData.getId(), row, false);
    }

    @Override
    public void remove(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} remove", this);
        }
        removeAllRows();
        store.free(rootPageId);
        store.removeMeta(this, session);
    }

    @Override
    public void truncate(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} truncate", this);
        }
        store.logTruncate(session, tableData.getId());
        removeAllRows();
        if (tableData.getContainsLargeObject() && tableData.isPersistData()) {
            // unfortunately, the data is gone on rollback
            session.commit(false);
            database.getLobStorage().removeAllForTable(table.getId());
        }
        tableData.setRowCount(0);
    }

    private void removeAllRows() {
        try {
            PageData root = getPage(rootPageId, 0);
            root.freeRecursive();
            root = PageDataLeaf.create(this, rootPageId, PageData.ROOT);
            store.removeFromCache(rootPageId);
            store.update(root);
            rowCount = 0;
            lastKey = 0;
        } finally {
            store.incrementChangeCount();
        }
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("PAGE");
    }

    @Override
    public Row getRow(Session session, long key) {
        return getRowWithKey(key);
    }

    /**
     * Get the row with the given key.
     *
     * @param key the key
     * @return the row
     */
    public Row getRowWithKey(long key) {
        PageData root = getPage(rootPageId, 0);
        return root.getRowWithKey(key);
    }

    PageStore getPageStore() {
        return store;
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        PageData root = getPage(rootPageId, 0);
        return root.getDiskSpaceUsed();
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public int getColumnIndex(Column col) {
        // can not use this index - use the PageDelegateIndex instead
        return -1;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return false;
    }

    @Override
    public void close(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} close", this);
        }
        // can not close the index because it might get used afterwards,
        // for example after running recovery
        writeRowCount();
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

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    @Override
    public String toString() {
        return getName();
    }

    private void invalidateRowCount() {
        PageData root = getPage(rootPageId, 0);
        root.setRowCountStored(PageData.UNKNOWN_ROWCOUNT);
    }

    @Override
    public void writeRowCount() {
        if (SysProperties.MODIFY_ON_WRITE && rootPageId == 0) {
            // currently creating the index
            return;
        }
        try {
            PageData root = getPage(rootPageId, 0);
            root.setRowCountStored(MathUtils.convertLongToInt(rowCount));
        } finally {
            store.incrementChangeCount();
        }
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL(new StringBuilder(), false).append(".tableScan").toString();
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

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

}
