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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.HashSet;
import java.util.List;
import org.apache.ignite.internal.processors.query.h2.opt.join.ProxyDistributedLookupBatch;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexLookupBatch;
import org.h2.index.IndexType;
import org.h2.index.SpatialIndex;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/**
 * Allows to have 'free' index for alias columns
 * Delegates the calls to underlying normal index
 */
public class GridH2ProxyIndex extends H2IndexCostedBase {
    /** Underlying normal index */
    protected Index idx;

    /**
     *
     * @param tbl Table.
     * @param name Name of the proxy index.
     * @param colsList Column list for the proxy index.
     * @param idx Target index.
     */
    public GridH2ProxyIndex(GridH2Table tbl,
                            String name,
                            List<IndexColumn> colsList,
                            Index idx) {
        super(tbl, name, GridH2IndexBase.columnsArray(tbl, colsList),
            IndexType.createNonUnique(false, false, idx instanceof SpatialIndex));

        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols, IndexType.createNonUnique(false, false, idx instanceof SpatialIndex));

        this.idx = idx;
    }

    /**
     * @return Underlying index.
     */
    public Index underlyingIndex() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void close(Session session) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session session, SearchRow first, SearchRow last) {
        GridH2RowDescriptor desc = ((GridH2Table)idx.getTable()).rowDescriptor();
        return idx.find(session, desc.prepareProxyIndexRow(first), desc.prepareProxyIndexRow(last));
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndexEx(masks, rowCnt, filters, filter, sortOrder, false, allColumnsSet);

        int mul = ((GridH2IndexBase)idx).getDistributedMultiplier(session, filters, filter);

        return mul * baseCost;
    }

    /** {@inheritDoc} */
    @Override public void remove(Session session) {
        throw DbException.getUnsupportedException("remove index");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session session) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return idx.canGetFirstOrLast();
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean first) {
        return idx.findFirstOrLast(session, first);
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session session) {
        return idx.getRowCount(session);
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return idx.getRowCountApproximation();
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        IndexLookupBatch batch = idx.createLookupBatch(filters, filter);

        if (batch == null)
            return null;

        GridH2RowDescriptor rowDesc = ((GridH2Table)idx.getTable()).rowDescriptor();

        return new ProxyDistributedLookupBatch(batch, rowDesc);
    }

    /** {@inheritDoc} */
    @Override public void removeChildrenAndResources(Session session) {
        // No-op. Will be removed when underlying index is removed
    }

}
