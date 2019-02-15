/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.ignite.internal.processors.query.h2.opt.GridH2ScanIndex;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableBase;
import org.h2.table.TableFilter;
import org.h2.table.TableType;

/**
 * Merge table for distributed queries.
 */
public class GridMergeTable extends TableBase {
    /** */
    private ArrayList<Index> idxs;

    /**
     * @param data Data.
     */
    public GridMergeTable(CreateTableData data) {
        super(data);
    }

    /**
     * @param idxs Indexes.
     */
    public void indexes(ArrayList<Index> idxs) {
        assert !F.isEmpty(idxs);

        this.idxs = idxs;
    }

    /**
     * @return Merge index.
     */
    public GridMergeIndex getMergeIndex() {
        return (GridMergeIndex)idxs.get(idxs.size() - 1); // Sorted index must be the last.
    }

    /**
     * @param idx Index.
     * @return Scan index.
     */
    public static GridH2ScanIndex<GridMergeIndex> createScanIndex(GridMergeIndex idx) {
        return new ScanIndex(idx);
    }

    /** {@inheritDoc} */
    @Override public boolean lock(Session session, boolean exclusive, boolean force) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void unlock(Session s) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols,
        IndexType indexType, boolean create, String indexComment) {
        throw DbException.getUnsupportedException("addIndex");
    }

    /** {@inheritDoc} */
    @Override public void removeRow(Session session, Row row) {
        throw DbException.getUnsupportedException("removeRow");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session session) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public void addRow(Session session, Row row) {
        throw DbException.getUnsupportedException("addRow");
    }

    /** {@inheritDoc} */
    @Override public void checkSupportAlter() {
        throw DbException.getUnsupportedException("alter");
    }

    /** {@inheritDoc} */
    @Override public TableType getTableType() {
        return TableType.EXTERNAL_TABLE_ENGINE;
    }

    /** {@inheritDoc} */
    @Override public Index getScanIndex(Session session) {
        return idxs.get(0); // Must be always at 0.
    }

    /** {@inheritDoc} */
    @Override public Index getUniqueIndex() {
        return null; // We don't have a PK.
    }

    /** {@inheritDoc} */
    @Override public ArrayList<Index> getIndexes() {
        return idxs;
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedExclusively() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getMaxDataModificationId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isDeterministic() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean canDrop() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        return getScanIndex(ses).getRowCount(ses);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return getScanIndex(null).getRowCountApproximation();
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /**
     * Scan index wrapper.
     */
    private static class ScanIndex extends GridH2ScanIndex<GridMergeIndex> {
        /**
         * @param delegate Delegate.
         */
        public ScanIndex(GridMergeIndex delegate) {
            super(delegate);
        }

        /** {@inheritDoc} */
        @Override public double getCost(Session session, int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder, HashSet<Column> allColumnsSet) {
            long rows = getRowCountApproximation();

            return getCostRangeIndex(masks, rows, filters, filter, sortOrder, true, allColumnsSet);
        }
    }
}