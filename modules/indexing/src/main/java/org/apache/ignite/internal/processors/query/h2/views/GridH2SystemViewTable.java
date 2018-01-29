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

import java.util.ArrayList;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.table.TableType;

/**
 * System view H2 table.
 */
public class GridH2SystemViewTable extends TableBase {
    /** Scan index. */
    protected final GridH2SystemViewIndex scanIdx;

    /** System view. */
    protected final GridH2SystemView sysView;

    /**
     * Indexes.
     *
     * Note: We need ArrayList here by H2 {@link #getIndexes()} method contract.
     */
    protected final ArrayList<Index> indexes;

    /**
     * @param data Data.
     * @param sysView System view.
     */
    public GridH2SystemViewTable(CreateTableData data, GridH2SystemView sysView) {
        super(data);

        assert sysView != null;

        this.sysView = sysView;

        this.setColumns(sysView.getColumns());

        scanIdx = new GridH2SystemViewIndex(this);

        indexes = new ArrayList<>();
        indexes.add(scanIdx);

        for (String index : sysView.getIndexes()) {
            String[] indexedCols = index.split(",");

            Column[] cols = new Column[indexedCols.length];

            for (int i = 0; i < indexedCols.length; i++)
                cols[i] = getColumn(indexedCols[i]);

            GridH2SystemViewIndex idx = new GridH2SystemViewIndex(this, cols);

            indexes.add(idx);
        }
    }

    /** {@inheritDoc} */
    @Override public String getDropSQL() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String getCreateSQL() {
        return sysView.getCreateSQL();
    }

    /** {@inheritDoc} */
    @Override public Index addIndex(Session ses, String idxName, int idxId, IndexColumn[] cols,
        IndexType idxType, boolean create, String idxComment) {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public boolean lock(Session ses, boolean exclusive, boolean forceLockEvenInMvcc) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void unlock(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedExclusively() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void removeRow(Session ses, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public void addRow(Session ses, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public void removeChildrenAndResources(Session ses) {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public void checkSupportAlter() {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("META");
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        return sysView.getRowCount();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return sysView.canGetRowCount();
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return sysView.getRowCount();
    }

    /** {@inheritDoc} */
    @Override public boolean canDrop() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public TableType getTableType() {
        return TableType.SYSTEM_TABLE;
    }

    /** {@inheritDoc} */
    @Override public Index getScanIndex(Session ses) {
        return scanIdx;
    }

    /** {@inheritDoc} */
    @Override public Index getUniqueIndex() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public ArrayList<Index> getIndexes() {
        return indexes;
    }

    /** {@inheritDoc} */
    @Override public long getMaxDataModificationId() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isDeterministic() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean canReference() {
        return false;
    }

    /**
     * @param ses Session.
     * @param first First.
     * @param last Last.
     */
    public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        return sysView.getRows(ses, first, last);
    }
}
