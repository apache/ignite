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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.jetbrains.annotations.Nullable;

/**
 * H2 Index over {@link BPlusTree}.
 */
@SuppressWarnings({"TypeMayBeWeakened", "unchecked"})
public class H2TreeNoDataIndex extends GridH2IndexBase {

    /** */
    private final H2Tree[] segments;

    /** */
    private final List<InlineIndexHelper> inlineIdxs;

    /**
     * @param tbl Table.
     * @param name Index name.
     * @param pk Primary key.
     * @param colsList Index columns.
     * @throws IgniteCheckedException If failed.
     */
    public H2TreeNoDataIndex(
        GridH2Table tbl,
        String name,
        boolean pk,
        List<IndexColumn> colsList
    ) throws IgniteCheckedException {

        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        // We need indexes on the client node, but index will not contain any data.
        segments = null;
        inlineIdxs = null;

        initDistributedJoinMessaging(tbl);
    }

    /** {@inheritDoc} */
    @Override protected int segmentsCount() {
        return segments.length;
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
        HashSet<Column> allColumnsSet) {
        long rowCnt = getRowCountApproximation();

        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false, allColumnsSet);

        int mul = getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;
    }


    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return 10_000;
    }

    /** {@inheritDoc} */
    @Override public void refreshColumnIds() {
        super.refreshColumnIds();
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow lower, SearchRow upper) {
        return GridH2Cursor.EMPTY;
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        throw new IgniteSQLException("Should be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public boolean putx(GridH2Row row) {
        throw new IgniteSQLException("Should be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        throw new IgniteSQLException("Should be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        throw new IgniteSQLException("Should be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        throw new IgniteSQLException("Should be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session session, boolean b) {
        throw new IgniteSQLException("Should be invoked, due to it's not affinity node");
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean rmvIndex) {
    }

    /** {@inheritDoc} */
    @Override protected H2Tree treeForRead(int segment) {
        throw new IgniteSQLException("Should be invoked, due to it's not affinity node");
    }
}
