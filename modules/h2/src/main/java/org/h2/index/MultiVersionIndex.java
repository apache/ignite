/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;
import java.util.HashSet;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A multi-version index is a combination of a regular index,
 * and a in-memory tree index that contains uncommitted changes.
 * Uncommitted changes can include new rows, and deleted rows.
 */
public class MultiVersionIndex implements Index {

    private final Index base;
    private final TreeIndex delta;
    private final RegularTable table;
    private final Object sync;
    private final Column firstColumn;

    public MultiVersionIndex(Index base, RegularTable table) {
        this.base = base;
        this.table = table;
        IndexType deltaIndexType = IndexType.createNonUnique(false);
        if (base instanceof SpatialIndex) {
            throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,
                    "MVCC & spatial index");
        }
        this.delta = new TreeIndex(table, -1, "DELTA", base.getIndexColumns(),
                deltaIndexType);
        delta.setMultiVersion(true);
        this.sync = base.getDatabase();
        this.firstColumn = base.getColumns()[0];
    }

    @Override
    public void add(Session session, Row row) {
        synchronized (sync) {
            base.add(session, row);
            if (removeIfExists(session, row)) {
                // for example rolling back a delete operation
            } else if (row.getSessionId() != 0) {
                // don't insert rows that are added when creating an index
                delta.add(session, row);
            }
        }
    }

    @Override
    public void close(Session session) {
        synchronized (sync) {
            base.close(session);
        }
    }

    @Override
    public boolean isFindUsingFullTableScan() {
        return base.isFindUsingFullTableScan();
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        synchronized (sync) {
            Cursor baseCursor = base.find(filter, first, last);
            Cursor deltaCursor = delta.find(filter, first, last);
            return new MultiVersionCursor(filter.getSession(), this,
                    baseCursor, deltaCursor, sync);
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        synchronized (sync) {
            Cursor baseCursor = base.find(session, first, last);
            Cursor deltaCursor = delta.find(session, first, last);
            return new MultiVersionCursor(session, this, baseCursor, deltaCursor, sync);
        }
    }

    @Override
    public Cursor findNext(Session session, SearchRow first, SearchRow last) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public boolean canFindNext() {
        // TODO possible, but more complicated
        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return base.canGetFirstOrLast() && delta.canGetFirstOrLast();
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        if (first) {
            // TODO optimization: this loops through NULL elements
            Cursor cursor = find(session, null, null);
            while (cursor.next()) {
                SearchRow row = cursor.getSearchRow();
                Value v = row.getValue(firstColumn.getColumnId());
                if (v != ValueNull.INSTANCE) {
                    return cursor;
                }
            }
            return cursor;
        }
        Cursor baseCursor = base.findFirstOrLast(session, false);
        Cursor deltaCursor = delta.findFirstOrLast(session, false);
        MultiVersionCursor cursor = new MultiVersionCursor(session, this,
                baseCursor, deltaCursor, sync);
        cursor.loadCurrent();
        // TODO optimization: this loops through NULL elements
        while (cursor.previous()) {
            SearchRow row = cursor.getSearchRow();
            if (row == null) {
                break;
            }
            Value v = row.getValue(firstColumn.getColumnId());
            if (v != ValueNull.INSTANCE) {
                return cursor;
            }
        }
        return cursor;
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        return base.getCost(session, masks, filters, filter, sortOrder, allColumnsSet);
    }

    @Override
    public boolean needRebuild() {
        return base.needRebuild();
    }

    /**
     * Check if there is an uncommitted row with the given key
     * within a different session.
     *
     * @param session the original session
     * @param row the row (only the key is checked)
     * @return true if there is an uncommitted row
     */
    public boolean isUncommittedFromOtherSession(Session session, Row row) {
        Cursor c = delta.find(session, row, row);
        while (c.next()) {
            Row r = c.get();
            return r.getSessionId() != session.getId();
        }
        return false;
    }

    private boolean removeIfExists(Session session, Row row) {
        // maybe it was inserted by the same session just before
        Cursor c = delta.find(session, row, row);
        while (c.next()) {
            Row r = c.get();
            if (r.getKey() == row.getKey() && r.getVersion() == row.getVersion()) {
                if (r != row && table.getScanIndex(session).compareRows(r, row) != 0) {
                    row.setVersion(r.getVersion() + 1);
                } else {
                    delta.remove(session, r);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void remove(Session session, Row row) {
        synchronized (sync) {
            base.remove(session, row);
            if (removeIfExists(session, row)) {
                // added and deleted in the same transaction: no change
            } else {
                delta.add(session, row);
            }
        }
    }

    @Override
    public void remove(Session session) {
        synchronized (sync) {
            base.remove(session);
        }
    }

    @Override
    public void truncate(Session session) {
        synchronized (sync) {
            delta.truncate(session);
            base.truncate(session);
        }
    }

    @Override
    public void commit(int operation, Row row) {
        synchronized (sync) {
            removeIfExists(null, row);
        }
    }

    @Override
    public int compareRows(SearchRow rowData, SearchRow compare) {
        return base.compareRows(rowData, compare);
    }

    @Override
    public int getColumnIndex(Column col) {
        return base.getColumnIndex(col);
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return base.isFirstColumn(column);
    }

    @Override
    public Column[] getColumns() {
        return base.getColumns();
    }

    @Override
    public IndexColumn[] getIndexColumns() {
        return base.getIndexColumns();
    }

    @Override
    public String getCreateSQL() {
        return base.getCreateSQL();
    }

    @Override
    public String getCreateSQLForCopy(Table forTable, String quotedName) {
        return base.getCreateSQLForCopy(forTable, quotedName);
    }

    @Override
    public String getDropSQL() {
        return base.getDropSQL();
    }

    @Override
    public IndexType getIndexType() {
        return base.getIndexType();
    }

    @Override
    public String getPlanSQL() {
        return base.getPlanSQL();
    }

    @Override
    public long getRowCount(Session session) {
        return base.getRowCount(session);
    }

    @Override
    public Table getTable() {
        return base.getTable();
    }

    @Override
    public int getType() {
        return base.getType();
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        synchronized (sync) {
            table.removeIndex(this);
            remove(session);
        }
    }

    @Override
    public String getSQL() {
        return base.getSQL();
    }

    @Override
    public Schema getSchema() {
        return base.getSchema();
    }

    @Override
    public void checkRename() {
        base.checkRename();
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        return base.getChildren();
    }

    @Override
    public String getComment() {
        return base.getComment();
    }

    @Override
    public Database getDatabase() {
        return base.getDatabase();
    }

    @Override
    public int getId() {
        return base.getId();
    }

    @Override
    public String getName() {
        return base.getName();
    }

    @Override
    public boolean isTemporary() {
        return base.isTemporary();
    }

    @Override
    public void rename(String newName) {
        base.rename(newName);
    }

    @Override
    public void setComment(String comment) {
        base.setComment(comment);
    }

    @Override
    public void setTemporary(boolean temporary) {
        base.setTemporary(temporary);
    }

    @Override
    public long getRowCountApproximation() {
        return base.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return base.getDiskSpaceUsed();
    }

    public Index getBaseIndex() {
        return base;
    }

    @Override
    public Row getRow(Session session, long key) {
        return base.getRow(session, key);
    }

    @Override
    public boolean isHidden() {
        return base.isHidden();
    }

    @Override
    public boolean isRowIdIndex() {
        return base.isRowIdIndex() && delta.isRowIdIndex();
    }

    @Override
    public boolean canScan() {
        return base.canScan();
    }

    @Override
    public void setSortedInsertMode(boolean sortedInsertMode) {
        base.setSortedInsertMode(sortedInsertMode);
        delta.setSortedInsertMode(sortedInsertMode);
    }

    @Override
    public IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        // Lookup batching is not supported.
        return null;
    }
}
