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
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.TableFilter;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;

/**
 * An unique index based on an in-memory hash map.
 */
public class HashIndex extends BaseIndex {

    /**
     * The index of the indexed column.
     */
    private final int indexColumn;

    private final RegularTable tableData;
    private ValueHashMap<Long> rows;

    public HashIndex(RegularTable table, int id, String indexName,
            IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
        this.indexColumn = columns[0].column.getColumnId();
        this.tableData = table;
        reset();
    }

    private void reset() {
        rows = ValueHashMap.newInstance();
    }

    @Override
    public void truncate(Session session) {
        reset();
    }

    @Override
    public void add(Session session, Row row) {
        Value key = row.getValue(indexColumn);
        Object old = rows.get(key);
        if (old != null) {
            // TODO index duplicate key for hash indexes: is this allowed?
            throw getDuplicateKeyException(key.toString());
        }
        rows.put(key, row.getKey());
    }

    @Override
    public void remove(Session session, Row row) {
        rows.remove(row.getValue(indexColumn));
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (first == null || last == null) {
            // TODO hash index: should additionally check if values are the same
            throw DbException.throwInternalError(first + " " + last);
        }
        Value v = first.getValue(indexColumn);
        /*
         * Sometimes the incoming search is a similar, but not the same type
         * e.g. the search value is INT, but the index column is LONG. In which
         * case we need to convert, otherwise the ValueHashMap will not find the
         * result.
         */
        v = v.convertTo(tableData.getColumn(indexColumn).getType());
        Row result;
        Long pos = rows.get(v);
        if (pos == null) {
            result = null;
        } else {
            result = tableData.getRow(session, pos.intValue());
        }
        return new SingleRowCursor(result);
    }

    @Override
    public long getRowCount(Session session) {
        return getRowCountApproximation();
    }

    @Override
    public long getRowCountApproximation() {
        return rows.size();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void remove(Session session) {
        // nothing to do
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) != IndexCondition.EQUALITY) {
                return Long.MAX_VALUE;
            }
        }
        return 2;
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("HASH");
    }

    @Override
    public boolean canScan() {
        return false;
    }

}
