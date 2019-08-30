/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sys;

import java.util.Iterator;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/**
 * Meta view H2 index.
 */
public class SqlSystemIndex extends BaseIndex {
    /** Distributed view cost multiplier. */
    private static final int DISTRIBUTED_MUL = 100;

    /**
     * @param tbl Table.
     * @param col Column.
     */
    SqlSystemIndex(SystemViewH2Adapter tbl, Column... col) {
        super(tbl, 0, null,
            col != null && col.length > 0 ? IndexColumn.wrap(col) : H2Utils.EMPTY_COLUMNS,
            IndexType.createNonUnique(false));
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("system view is read-only");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("system view is read-only");
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow first, SearchRow last) {
        assert table instanceof SystemViewH2Adapter;

        Iterator<Row> rows = ((SystemViewH2Adapter)table).getRows(ses, first, last);

        return new GridH2Cursor(rows);
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        AllColumnsForPlan allColsSet) {
        long rowCnt = getRowCountApproximation(ses);

        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false, allColsSet);

        if (((SystemViewH2Adapter)table).view.isDistributed())
            baseCost = baseCost * DISTRIBUTED_MUL;

        return baseCost;
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("system view cannot be truncated");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        throw DbException.getUnsupportedException("system view cannot be removed");
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("system view cannot be renamed");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getCreateSQL() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        throw DbException.getUnsupportedException("system views cannot be used to get first or last value");
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        return table.getRowCount(ses);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation(Session ses) {
        return table.getRowCountApproximation(ses);
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }
}
