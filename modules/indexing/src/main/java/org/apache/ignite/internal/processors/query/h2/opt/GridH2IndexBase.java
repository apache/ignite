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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.join.CollocationModel;
import org.apache.ignite.internal.processors.query.h2.opt.join.CollocationModelMultiplier;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.BaseIndex;
import org.h2.index.IndexCondition;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * Index base.
 */
public abstract class GridH2IndexBase extends BaseIndex {
    /** Underlying table. */
    private final GridH2Table tbl;

    public static volatile boolean start;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    protected GridH2IndexBase(GridH2Table tbl) {
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public final void close(Session ses) {
        // No-op. Actual index destruction must happen in method destroy.
    }

    /**
     * Attempts to destroys index and release all the resources.
     * We use this method instead of {@link #close(Session)} because that method
     * is used by H2 internally.
     *
     * @param rmv Flag remove.
     */
    public void destroy(boolean rmv) {
        // No-op.
    }

    /**
     * @return Index segment ID for current query context.
     */
    protected int threadLocalSegment() {
        if(segmentsCount() == 1)
            return 0;

        QueryContext qctx = queryContextRegistry().getThreadLocal();

        if(qctx == null)
            throw new IllegalStateException("GridH2QueryContext is not initialized.");

        return qctx.segment();
    }

    /**
     * Puts row.
     *
     * @param row Row.
     * @return Existing row or {@code null}.
     */
    public abstract H2CacheRow put(H2CacheRow row);

    /**
     * Puts row.
     *
     * @param row Row.
     * @return {@code True} if existing row row has been replaced.
     */
    public abstract boolean putx(H2CacheRow row);

    /**
     * Removes row from index.
     *
     * @param row Row.
     * @return {@code True} if row has been removed.
     */
    public abstract boolean removex(SearchRow row);

    /**
     * @param ses Session.
     * @param filters All joined table filters.
     * @param filter Current filter.
     * @return Multiplier.
     */
    public final int getDistributedMultiplier(Session ses, TableFilter[] filters, int filter) {
        //assert !localQuery();

        CollocationModelMultiplier mul = CollocationModel.distributedMultiplier(ses, filters, filter);

        return mul.multiplier();
    }

    /**
     * Returns the selectivity of the greater/less than comparison.
     * @param val Value.
     * @param min Min value (from stats).
     * @param max Max value (from stats).
     * @param greater {@code True} if grater than, {@code false} if less than.
     * @return The selectivity of comparison - the number between 0 and 1: the percent of rows
     * to be selected by the operation.
     */
    private static double selectivity(int val, int min, int max, boolean greater) {
        assert min <= max;

        int range = max - min;

        if (range == 0)
            return val == max ? 1 : 0;

        double interval = greater ? (max - val) : (val - min);

        //noinspection IntegerDivisionInFloatingPointContext
        double sel = interval / range;

        if (sel < 0)
            return 0;
        else if (sel > 1)
            return 1;
        else
            return sel;
    }


    /**
     * Calculate the cost for the given mask as if this index was a typical
     * b-tree range index. This is the estimated cost required to search one
     * row, and then iterate over the given number of rows.
     *
     * @param masks the IndexCondition search masks, one for each column in the
     *            table
     * @param rowCount the number of rows in the index
     * @param filters all joined table filters
     * @param filter the current table filter index
     * @param sortOrder the sort order
     * @param isScanIndex whether this is a "table scan" index
     * @param allColumnsSet the set of all columns
     * @return the estimated cost
     */
    protected long getCostUsingStatistics(int[] masks, long rowCount,
        TableFilter[] filters, int filter, SortOrder sortOrder,
        boolean isScanIndex, HashSet<Column> allColumnsSet) {
        assert localQuery();

        TableFilter curFilter = filters == null ? null : filters[filter];

        // Fallback to the previous behaviour.
        if (curFilter == null  || masks == null)
            return getCostRangeIndex(masks, rowCount, filters, filter, sortOrder, isScanIndex, allColumnsSet);

        ArrayList<IndexCondition> conditions = curFilter.getIndexConditions();

        // Check if we have constant conditions like x = 10 (not x = ?0)
        Value[] condVals = new Value[tbl.getColumns().length];

        for (IndexCondition cond : conditions) {
            Expression exp = cond.getExpression();
            int colId = cond.getColumn().getColumnId();

            if (exp.isConstant())
                condVals[colId] = exp.getValue(null);
        }

        // Fallback if the first index column is not filtered by the table filter.
        if (condVals[columns[0].getColumnId()] == null)
            return getCostRangeIndex(masks, rowCount, filters, filter, sortOrder, isScanIndex, allColumnsSet);

        //TODO Index with multiple columns?
        assert columns.length <= 2;
        assert columns.length < 2 || "_KEY".equals(columns[1].getName());

        long rowCount0 = rowCount;

        rowCount += Constants.COST_ROW_OFFSET;

        // TODO selectivity estimation for multiple columns.
        int totalSelectivity = 0;

        long rowsCost = rowCount;

        Map<Integer, GridH2Table.ColumnStatistics> stats = tbl.columnStatistics();

        for (int i = 0, len = columns.length; i < len; i++) {
            Column col = columns[i];

            // TODO fallback if null
            GridH2Table.ColumnStatistics colStats = stats.get(col.getColumnId());

            final int min = colStats.min();
            final int max = colStats.max();
            final int ndv = colStats.ndv();

            int idx = col.getColumnId();
            int mask = masks[idx];
            Value v = condVals[idx];

            if (v == null)
                System.out.println("!!!");

            // TODO implement check for the other types: double, dates etc.
            int val = v.getInt();

            if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                // Most likely there are no rows will be returned if the value is greater than the maximum
                // or below the minimum.
                if (val < min || val > max) {
                    rowsCost = 1;

                    System.err.println("Estimated rows count=" + 0);

                    break;
                }

                // Unique index returns for last index column with equality returns at most one row.
                if (i == columns.length - 1 && getIndexType().isUnique()) {
                    rowsCost = 3;

                    System.err.println("Estimated rows count=" + 1);

                    break;
                }

                System.err.println("Estimated rows count=" + Math.max(rowCount0 / ndv, 1));

                rowsCost = 2 + Math.max(rowCount / ndv, 1);

                break;
            }
            else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                // TODO multiple condition on the single column
                assert false;

//                double sel = selectivity(val, min, max, false) * selectivity(val, min, max, true);
//
//                rowsCost = 2 + (long)(rowCount * sel);

                break;
            }
            else if ((mask & IndexCondition.START) == IndexCondition.START) {
                double sel = selectivity(val, min, max, true);

                System.err.println("Estimated rows count=" + (long)(rowCount0 * sel));

                rowsCost = 2 + (long)(rowCount * sel);

                break;
            }
            else if ((mask & IndexCondition.END) == IndexCondition.END) {
                double sel = selectivity(val, min, max, false);

                System.err.println("Estimated rows count=" + (long)(rowCount0 * sel));

                rowsCost = 2 + (long)(rowCount * sel);

                break;
            }
            else
                break;
        }

        // TODO understand the code listed below (left as is from org.h2.index.BaseIndex.getCostRangeIndex).

        // If the ORDER BY clause matches the ordering of this index,
        // it will be cheaper than another index, so adjust the cost
        // accordingly.
        long sortingCost = 0;
        if (sortOrder != null)
            sortingCost = 100 + rowCount / 10;
        if (sortOrder != null && !isScanIndex) {
            boolean sortOrderMatches = true;
            int coveringCount = 0;
            int[] sortTypes = sortOrder.getSortTypes();
            TableFilter tableFilter = filters == null ? null : filters[filter];
            for (int i = 0, len = sortTypes.length; i < len; i++) {
                if (i >= indexColumns.length) {
                    // We can still use this index if we are sorting by more
                    // than it's columns, it's just that the coveringCount
                    // is lower than with an index that contains
                    // more of the order by columns.
                    break;
                }
                Column col = sortOrder.getColumn(i, tableFilter);
                if (col == null) {
                    sortOrderMatches = false;
                    break;
                }
                IndexColumn indexCol = indexColumns[i];
                if (!col.equals(indexCol.column)) {
                    sortOrderMatches = false;
                    break;
                }
                int sortType = sortTypes[i];
                if (sortType != indexCol.sortType) {
                    sortOrderMatches = false;
                    break;
                }
                coveringCount++;
            }
            if (sortOrderMatches) {
                // "coveringCount" makes sure that when we have two
                // or more covering indexes, we choose the one
                // that covers more.
                sortingCost = 100 - coveringCount;
            }
        }
        // If we have two indexes with the same cost, and one of the indexes can
        // satisfy the query without needing to read from the primary table
        // (scan index), make that one slightly lower cost.
        boolean needsToReadFromScanIndex = true;
        if (!isScanIndex && allColumnsSet != null && !allColumnsSet.isEmpty()) {
            boolean foundAllColumnsWeNeed = true;
            for (Column c : allColumnsSet) {
                if (c.getTable() == getTable()) {
                    boolean found = false;
                    for (Column c2 : columns) {
                        if (c == c2) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        foundAllColumnsWeNeed = false;
                        break;
                    }
                }
            }
            if (foundAllColumnsWeNeed) {
                needsToReadFromScanIndex = false;
            }
        }
        long rc;
        if (isScanIndex) {
            rc = rowsCost + sortingCost + 20;
        } else if (needsToReadFromScanIndex) {
            rc = rowsCost + rowsCost + sortingCost + 20;
        } else {
            // The (20-x) calculation makes sure that when we pick a covering
            // index, we pick the covering index that has the smallest number of
            // columns (the more columns we have in index - the higher cost).
            // This is faster because a smaller index will fit into fewer data
            // blocks.
            rc = rowsCost + sortingCost + columns.length;
        }
        return rc;
    }


    /** {@inheritDoc} */
    @Override public GridH2Table getTable() {
        return (GridH2Table)super.getTable();
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        // No-op: destroyed from owning table.
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void removeChildrenAndResources(Session session) {
        // The sole purpose of this override is to pass session to table.removeIndex
        assert table instanceof GridH2Table;

        ((GridH2Table)table).removeIndex(session, this);

        remove(session);

        database.removeMeta(session, getId());
    }

    /**
     * @return Index segments count.
     */
    public abstract int segmentsCount();

    /**
     * @param partition Partition idx.
     * @return Segment ID for given key
     */
    public int segmentForPartition(int partition){
        return segmentsCount() == 1 ? 0 : (partition % segmentsCount());
    }

    /**
     * @param row Table row.
     * @return Segment ID for given row.
     */
    @SuppressWarnings("IfMayBeConditional")
    protected int segmentForRow(GridCacheContext ctx, SearchRow row) {
        assert row != null;

        if (segmentsCount() == 1 || ctx == null)
            return 0;

        CacheObject key;

        final Value keyColValue = row.getValue(QueryUtils.KEY_COL);

        assert keyColValue != null;

        final Object o = keyColValue.getObject();

        if (o instanceof CacheObject)
            key = (CacheObject)o;
        else
            key = ctx.toCacheKeyObject(o);

        return segmentForPartition(ctx.affinity().partition(key));
    }

    /**
     * Re-assign column ids after removal of column(s).
     */
    public void refreshColumnIds() {
        assert columnIds.length == columns.length;

        for (int pos = 0; pos < columnIds.length; ++pos)
            columnIds[pos] = columns[pos].getColumnId();
    }

    /**
     * @return Row descriptor.
     */
    protected GridH2RowDescriptor rowDescriptor() {
        return tbl.rowDescriptor();
    }

    /**
     * @return Query context registry.
     */
    protected QueryContextRegistry queryContextRegistry() {
        return tbl.rowDescriptor().indexing().queryContextRegistry();
    }

    protected boolean localQuery() {
        QueryContext qctx = queryContextRegistry().getThreadLocal();

        assert qctx != null;

        return qctx.local();
    }

    @Override public long getRowCountApproximation() {
        QueryContext qctx = queryContextRegistry().getThreadLocal();
        assert qctx != null;

        if (!qctx.local())
            return 10_000; // Fallback to the previous behavior for distributed queries.

        GridCacheContext cctx = tbl.cacheInfo().cacheContext();

        long cnt = 0;
        int partCnt = 0;

        if (cctx.isReplicated())
            cnt = tbl.getRowCountApproximation(); // Count all entries for replicated caches.
        else {
            // Consider only primary partitions for partitioned caches.
            IndexingQueryFilter f = qctx.filter();
            IndexingQueryCacheFilter filter = f != null ? f.forCache(getTable().cacheName()) : null;

            for (IgniteCacheOffheapManager.CacheDataStore store : cctx.offheap().cacheDataStores()) {
                int part = store.partId();

                if (filter == null || filter.applyPartition(part))
                    cnt += store.cacheSize(cctx.cacheId());
            }
        }

        return cnt;
    }
}
