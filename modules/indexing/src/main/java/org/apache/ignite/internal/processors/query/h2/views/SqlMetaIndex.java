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

import java.util.Arrays;
import java.util.HashSet;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.h2.engine.Constants;
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
import org.h2.table.TableFilter;

/**
 * Meta view H2 index.
 */
public class SqlMetaIndex extends BaseIndex {
    /**
     * @param tbl Table.
     * @param col Column.
     */
    SqlMetaIndex(SqlMetaTable tbl, Column... col) {
        IndexColumn[] idxCols;

        if (col != null && col.length > 0)
            idxCols = IndexColumn.wrap(col);
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
        if (table instanceof SqlMetaTable) {
            Iterable<Row> rows = ((SqlMetaTable)table).getRows(ses, first, last);

            return new GridH2Cursor(rows.iterator());
        }
        else
            throw DbException.throwInternalError("Unexpected table class: " + table.getClass().getName());
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        HashSet<Column> allColsSet) {
        double colsCost = 2d;

        boolean isIdxUsed = false;

        if (masks != null) {
            double colWeight = 2d;

            for (Column col : columns) {
                colWeight /= 2d;

                if ((masks[col.getColumnId()] & IndexCondition.EQUALITY) != 0) {
                    isIdxUsed = true;

                    colsCost -= colWeight;
                } else
                    colsCost += colWeight;
            }
        }

        return isIdxUsed ? colsCost : Constants.COST_ROW_OFFSET + getRowCountApproximation();
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
        return table.getRowCount(ses);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return table.getRowCountApproximation();
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String getPlanSQL() {
        return getTable().getName() + ((columns == null || columns.length == 0) ? " full scan"
            : " using index " + Arrays.toString(columns));
    }
}
