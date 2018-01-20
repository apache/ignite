/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.views;

import java.util.HashSet;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexCondition;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.MetaTable;
import org.h2.table.TableFilter;

/**
 * Sys view index
 */
public class GridH2SysViewIndex extends BaseIndex {
    /** Indexed column. */
    private final Column indexedCol;

    /**
     * @param tbl Table.
     * @param col Column.
     */
    GridH2SysViewIndex(GridH2SysViewTable tbl, Column col) {
        this.indexedCol = col;
        IndexColumn[] idxCols;

        if (col != null)
            idxCols = IndexColumn.wrap(new Column[] { col });
        else
            idxCols = new IndexColumn[0];

        initBaseIndex(tbl, 0, null, idxCols, IndexType.createNonUnique(false));
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow first, SearchRow last) {
        if (table instanceof GridH2SysViewTable) {
            Iterable<Row> rows = ((GridH2SysViewTable)table).getRows(ses, first, last);

            return new GridH2Cursor(rows.iterator());
        }
        else
            throw DbException.throwInternalError("Unexpected table class: " + table.getClass().getName());
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        HashSet<Column> allColsSet) {
        if (indexedCol != null) {
            if ((masks[indexedCol.getColumnId()] & IndexCondition.EQUALITY) != 0)
                return 1d;
            else if ((masks[indexedCol.getColumnId()] & IndexCondition.RANGE) != 0)
                return 10d;
        }

        return 100d + getRowCountApproximation();
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("META");
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
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        return MetaTable.ROW_COUNT_APPROXIMATION;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return MetaTable.ROW_COUNT_APPROXIMATION;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String getPlanSQL() {
        return "meta";
    }
}

