/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.TableFilter;
import org.h2.util.New;

/**
 * The scan index is not really an 'index' in the strict sense, because it can
 * not be used for direct lookup. It can only be used to iterate over all rows
 * of a table. Each regular table has one such object, even if no primary key or
 * indexes are defined.
 */
public class ScanIndex extends BaseIndex {
    private long firstFree = -1;
    private ArrayList<Row> rows = New.arrayList();
    private final RegularTable tableData;
    private int rowCountDiff;
    private final HashMap<Integer, Integer> sessionRowCount;
    private HashSet<Row> delta;
    private long rowCount;

    public ScanIndex(RegularTable table, int id, IndexColumn[] columns,
            IndexType indexType) {
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        if (database.isMultiVersion()) {
            sessionRowCount = new HashMap<>();
        } else {
            sessionRowCount = null;
        }
        tableData = table;
    }

    @Override
    public void remove(Session session) {
        truncate(session);
    }

    @Override
    public void truncate(Session session) {
        rows = New.arrayList();
        firstFree = -1;
        if (tableData.getContainsLargeObject() && tableData.isPersistData()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        tableData.setRowCount(0);
        rowCount = 0;
        rowCountDiff = 0;
        if (database.isMultiVersion()) {
            sessionRowCount.clear();
        }
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public Row getRow(Session session, long key) {
        return rows.get((int) key);
    }

    @Override
    public void add(Session session, Row row) {
        // in-memory
        if (firstFree == -1) {
            int key = rows.size();
            row.setKey(key);
            rows.add(row);
        } else {
            long key = firstFree;
            Row free = rows.get((int) key);
            firstFree = free.getKey();
            row.setKey(key);
            rows.set((int) key, row);
        }
        row.setDeleted(false);
        if (database.isMultiVersion()) {
            if (delta == null) {
                delta = new HashSet<>();
            }
            boolean wasDeleted = delta.remove(row);
            if (!wasDeleted) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), 1);
        }
        rowCount++;
    }

    @Override
    public void commit(int operation, Row row) {
        if (database.isMultiVersion()) {
            if (delta != null) {
                delta.remove(row);
            }
            incrementRowCount(row.getSessionId(),
                    operation == UndoLogRecord.DELETE ? 1 : -1);
        }
    }

    private void incrementRowCount(int sessionId, int count) {
        if (database.isMultiVersion()) {
            Integer id = sessionId;
            Integer c = sessionRowCount.get(id);
            int current = c == null ? 0 : c.intValue();
            sessionRowCount.put(id, current + count);
            rowCountDiff += count;
        }
    }

    @Override
    public void remove(Session session, Row row) {
        // in-memory
        if (!database.isMultiVersion() && rowCount == 1) {
            rows = New.arrayList();
            firstFree = -1;
        } else {
            Row free = session.createRow(null, 1);
            free.setKey(firstFree);
            long key = row.getKey();
            if (rows.size() <= key) {
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                        rows.size() + ": " + key);
            }
            rows.set((int) key, free);
            firstFree = key;
        }
        if (database.isMultiVersion()) {
            // if storage is null, the delete flag is not yet set
            row.setDeleted(true);
            if (delta == null) {
                delta = new HashSet<>();
            }
            boolean wasAdded = delta.remove(row);
            if (!wasAdded) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), -1);
        }
        rowCount--;
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new ScanCursor(session, this, database.isMultiVersion());
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        return tableData.getRowCountApproximation() + Constants.COST_ROW_OFFSET;
    }

    @Override
    public long getRowCount(Session session) {
        if (database.isMultiVersion()) {
            Integer i = sessionRowCount.get(session.getId());
            long count = i == null ? 0 : i.intValue();
            count += rowCount;
            count -= rowCountDiff;
            return count;
        }
        return rowCount;
    }

    /**
     * Get the next row that is stored after this row.
     *
     * @param row the current row or null to start the scan
     * @return the next row or null if there are no more rows
     */
    Row getNextRow(Row row) {
        long key;
        if (row == null) {
            key = -1;
        } else {
            key = row.getKey();
        }
        while (true) {
            key++;
            if (key >= rows.size()) {
                return null;
            }
            row = rows.get((int) key);
            if (!row.isEmpty()) {
                return row;
            }
        }
    }

    @Override
    public int getColumnIndex(Column col) {
        // the scan index cannot use any columns
        return -1;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return false;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("SCAN");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("SCAN");
    }

    Iterator<Row> getDelta() {
        if (delta == null) {
            List<Row> e = Collections.emptyList();
            return e.iterator();
        }
        return delta.iterator();
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

}
