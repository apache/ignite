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
import org.h2.table.RangeTable;
import org.h2.table.TableFilter;

/**
 * An index for the SYSTEM_RANGE table.
 * This index can only scan through all rows, search is not supported.
 */
public class RangeIndex extends BaseIndex {

    private final RangeTable rangeTable;

    public RangeIndex(RangeTable table, IndexColumn[] columns) {
        initBaseIndex(table, 0, "RANGE_INDEX", columns,
                IndexType.createNonUnique(true));
        this.rangeTable = table;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    @Override
    public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        long min = rangeTable.getMin(session), start = min;
        long max = rangeTable.getMax(session), end = max;
        long step = rangeTable.getStep(session);
        try {
            start = Math.max(min, first == null ? min : first.getValue(0).getLong());
        } catch (Exception e) {
            // error when converting the value - ignore
        }
        try {
            end = Math.min(max, last == null ? max : last.getValue(0).getLong());
        } catch (Exception e) {
            // error when converting the value - ignore
        }
        return new RangeCursor(session, start, end, step);
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        return 1;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void remove(Session session) {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    @Override
    public void truncate(Session session) {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        long pos = first ? rangeTable.getMin(session) : rangeTable.getMax(session);
        return new RangeCursor(session, pos, pos);
    }

    @Override
    public long getRowCount(Session session) {
        return rangeTable.getRowCountApproximation();
    }

    @Override
    public long getRowCountApproximation() {
        return rangeTable.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }
}
