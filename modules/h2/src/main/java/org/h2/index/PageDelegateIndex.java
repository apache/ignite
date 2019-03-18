/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.HashSet;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.store.PageStore;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.TableFilter;

/**
 * An index that delegates indexing to the page data index.
 */
public class PageDelegateIndex extends PageIndex {

    private final PageDataIndex mainIndex;

    public PageDelegateIndex(RegularTable table, int id, String name,
            IndexType indexType, PageDataIndex mainIndex, boolean create,
            Session session) {
        IndexColumn[] cols = IndexColumn.wrap(
                new Column[] { table.getColumn(mainIndex.getMainIndexColumn())});
        this.initBaseIndex(table, id, name, cols, indexType);
        this.mainIndex = mainIndex;
        if (!database.isPersistent() || id < 0) {
            throw DbException.throwInternalError("" + name);
        }
        PageStore store = database.getPageStore();
        store.addIndex(this);
        if (create) {
            store.addMeta(this, session);
        }
    }

    @Override
    public void add(Session session, Row row) {
        // nothing to do
    }

    @Override
    public boolean canFindNext() {
        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        long min = mainIndex.getKey(first, Long.MIN_VALUE, Long.MIN_VALUE);
        // ifNull is MIN_VALUE as well, because the column is never NULL
        // so avoid returning all rows (returning one row is OK)
        long max = mainIndex.getKey(last, Long.MAX_VALUE, Long.MIN_VALUE);
        return mainIndex.find(session, min, max, false);
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        Cursor cursor;
        if (first) {
            cursor = mainIndex.find(session, Long.MIN_VALUE, Long.MAX_VALUE, false);
        } else  {
            long x = mainIndex.getLastKey();
            cursor = mainIndex.find(session, x, x, false);
        }
        cursor.next();
        return cursor;
    }

    @Override
    public Cursor findNext(Session session, SearchRow higherThan, SearchRow last) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public int getColumnIndex(Column col) {
        if (col.getColumnId() == mainIndex.getMainIndexColumn()) {
            return 0;
        }
        return -1;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return getColumnIndex(column) == 0;
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        return 10 * getCostRangeIndex(masks, mainIndex.getRowCount(session),
                filters, filter, sortOrder, false, allColumnsSet);
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void remove(Session session, Row row) {
        // nothing to do
    }

    @Override
    public void remove(Session session) {
        mainIndex.setMainIndexColumn(-1);
        session.getDatabase().getPageStore().removeMeta(this, session);
    }

    @Override
    public void truncate(Session session) {
        // nothing to do
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public long getRowCount(Session session) {
        return mainIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return mainIndex.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return mainIndex.getDiskSpaceUsed();
    }

    @Override
    public void writeRowCount() {
        // ignore
    }

}
