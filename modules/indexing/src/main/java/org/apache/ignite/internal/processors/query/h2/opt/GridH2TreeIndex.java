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

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.query.h2.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeMap;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for snapshotable tree indexes.
 */
@SuppressWarnings("ComparatorNotSerializable")
public class GridH2TreeIndex extends GridH2IndexBase implements Comparator<GridSearchRowPointer> {
    /** */
    private final IgniteNavigableMapTree tree;

    /** */
    private final boolean snapshotEnabled;

    /**
     * Constructor with index initialization.
     *
     * @param name Index name.
     * @param tbl Table.
     * @param pk If this index is primary key.
     * @param colsList Index columns list.
     */
    @SuppressWarnings("unchecked")
    public GridH2TreeIndex(String name, GridH2Table tbl, boolean pk, List<IndexColumn> colsList) {
        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createUnique(false, false) : IndexType.createNonUnique(false, false, false));

        final GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (desc == null || desc.memory() == null) {
            snapshotEnabled = desc == null || desc.snapshotableIndex();

            if (snapshotEnabled) {
                tree = new IgniteNavigableMapTree(new SnapTreeMap<GridSearchRowPointer, GridH2Row>(this) {
                    @Override protected void afterNodeUpdate_nl(Node<GridSearchRowPointer, GridH2Row> node, Object val) {
                        if (val != null)
                            node.key = (GridSearchRowPointer)val;
                    }

                    @Override protected Comparable<? super GridSearchRowPointer> comparable(Object key) {
                        if (key instanceof ComparableRow)
                            return (Comparable<? super SearchRow>)key;

                        return super.comparable(key);
                    }
                });
            }
            else {
                tree = new IgniteNavigableMapTree(
                    new ConcurrentSkipListMap<GridSearchRowPointer, GridH2Row>(
                        new Comparator<GridSearchRowPointer>() {
                            @Override public int compare(GridSearchRowPointer o1, GridSearchRowPointer o2) {
                                if (o1 instanceof ComparableRow)
                                    return ((ComparableRow)o1).compareTo(o2);

                                if (o2 instanceof ComparableRow)
                                    return -((ComparableRow)o2).compareTo(o1);

                                return compareRows(o1, o2);
                            }
                        }
                ));
            }
        }
        else {
            assert desc.snapshotableIndex() : desc;

            snapshotEnabled = true;

            tree = new IgniteNavigableMapTree(new GridOffHeapSnapTreeMap<GridSearchRowPointer, GridH2Row>(desc, desc, desc.memory(), desc.guard(), this) {
                @Override protected void afterNodeUpdate_nl(long node, GridH2Row val) {
                    final long oldKey = keyPtr(node);

                    if (val != null) {
                        key(node, val);

                        guard.finalizeLater(new Runnable() {
                            @Override public void run() {
                                desc.createPointer(oldKey).decrementRefCount();
                            }
                        });
                    }
                }

                @Override protected Comparable<? super GridSearchRowPointer> comparable(Object key) {
                    if (key instanceof ComparableRow)
                        return (Comparable<? super SearchRow>)key;

                    return super.comparable(key);
                }
            });
        }

        initDistributedJoinMessaging(tbl);
    }

    /** {@inheritDoc} */
    @Override protected IgniteTree doTakeSnapshot() {
        assert snapshotEnabled;

        return tree.clone();
    }

    /** {@inheritDoc} */
    protected final IgniteTree treeForRead() {
        if (!snapshotEnabled)
            return tree;

        IgniteTree res = threadLocalSnapshot();

        if (res == null)
            res = tree;

        return res;
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        assert threadLocalSnapshot() == null;

        super.destroy();
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(@Nullable Session ses) {
        IndexingQueryFilter f = threadLocalFilter();

        // Fast path if we don't need to perform any filtering.
        if (f == null || f.forSpace((getTable()).spaceName()) == null)
            try {
                return treeForRead().size();
            } catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }

        GridCursor<GridH2Row> cursor = doFind(null, false, null);

        long size = 0;

        try {
            while (cursor.next())
                size++;
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }

        return size;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return table.getRowCountApproximation();
    }

    /** {@inheritDoc} */
    @Override public int compare(GridSearchRowPointer r1, GridSearchRowPointer r2) {
        // Second row here must be data row if first is a search row.
        return -compareRows(r2, r1);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB((indexType.isUnique() ? "Unique index '" : "Index '") + getName() + "' [");

        boolean first = true;

        for (IndexColumn col : getIndexColumns()) {
            if (first)
                first = false;
            else
                sb.a(", ");

            sb.a(col.getSQL());
        }

        sb.a(" ]");

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder) {
        long rowCnt = getRowCountApproximation();
        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false);
        int mul = getDistributedMultiplier(ses, filters, filter);

        return mul * baseCost;
    }

    /** {@inheritDoc} */
    @Override public boolean canFindNext() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, @Nullable SearchRow first, @Nullable SearchRow last) {
        return new H2Cursor(doFind(first, true, last), null);
    }

    /** {@inheritDoc} */
    @Override public Cursor findNext(Session ses, SearchRow higherThan, SearchRow last) {
        return new H2Cursor(doFind(higherThan, false, last), null);
    }

    /**
     * Finds row with key equal one in given search row.
     * WARNING!! Method call must be protected by {@link GridUnsafeGuard#begin()}
     * {@link GridUnsafeGuard#end()} block.
     *
     * @param row Search row.
     * @return Row.
     */
    public GridH2Row findOne(GridH2Row row) {
        try {
            return tree.findOne(row);
        } catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Returns sub-tree bounded by given values.
     *
     * @param first Lower bound.
     * @param includeFirst Whether lower bound should be inclusive.
     * @param last Upper bound always inclusive.
     * @return Iterator over rows in given range.
     */
    @SuppressWarnings("unchecked")
    private GridCursor<GridH2Row> doFind(@Nullable SearchRow first, boolean includeFirst, @Nullable SearchRow last) {
        IgniteTree t = treeForRead();

        return doFind0(t, first, includeFirst, last, threadLocalFilter());
    }

    /** {@inheritDoc} */
    @Override protected final GridCursor<GridH2Row> doFind0(
        IgniteTree t,
        @Nullable SearchRow first,
        boolean includeFirst,
        @Nullable SearchRow last,
        IndexingQueryFilter filter
    ) {
        includeFirst &= first != null;

        GridCursor<GridH2Row> range = subTree(t, comparable(first, includeFirst ? -1 : 1),
            comparable(last, 1));

        if (range == null)
            return EMPTY_CURSOR;

        return filter(range, filter);
    }

    /**
     * @param row Row.
     * @param bias Bias.
     * @return Comparable row.
     */
    private GridSearchRowPointer comparable(SearchRow row, int bias) {
        if (row == null)
            return null;

        if (bias == 0 && row instanceof GridH2Row)
            return (GridSearchRowPointer)row;

        return new ComparableRow(row, bias);
    }

    /**
     * Takes sup-map from given one.
     *
     * @param tree Tree.
     * @param first Lower bound.
     * @param last Upper bound.
     * @return Sub-map.
     */
    @SuppressWarnings({"IfMayBeConditional", "TypeMayBeWeakened"})
    private GridCursor<GridH2Row> subTree(IgniteTree tree,
        @Nullable GridSearchRowPointer first, @Nullable GridSearchRowPointer last) {

        if (first != null && last != null && compare(first, last) > 0)
            return null;

        try {
            // We take exclusive bounds because it is possible that one search row will be equal to multiple key rows
            // in tree and we must return them all.
            return tree.find(first, last);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Gets iterator over all rows in this index.
     *
     * @return Rows iterator.
     */
    GridCursor<GridH2Row> rows() {
        return doFind(null, false, null);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        throw DbException.throwInternalError();
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        try {
            return tree.put(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        try {
            return tree.remove(comparable(row, 0));
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Comparable row with bias. Will be used for queries to have correct bounds (in case of multicolumn index
     * and query on few first columns we will multiple equal entries in tree).
     */
    private final class ComparableRow implements GridSearchRowPointer, Comparable<SearchRow> {
        /** */
        private final SearchRow row;

        /** */
        private final int bias;

        /**
         * @param row Row.
         * @param bias Bias.
         */
        private ComparableRow(SearchRow row, int bias) {
            this.row = row;
            this.bias = bias;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(SearchRow o) {
            int res = compareRows(o, row);

            if (res == 0)
                return bias;

            return -res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            throw new IllegalStateException("Should never be called.");
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return row.getColumnCount();
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            return row.getValue(idx);
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            row.setValue(idx, v);
        }

        /** {@inheritDoc} */
        @Override public void setKeyAndVersion(SearchRow old) {
            row.setKeyAndVersion(old);
        }

        /** {@inheritDoc} */
        @Override public int getVersion() {
            return row.getVersion();
        }

        /** {@inheritDoc} */
        @Override public void setKey(long key) {
            row.setKey(key);
        }

        /** {@inheritDoc} */
        @Override public long getKey() {
            return row.getKey();
        }

        /** {@inheritDoc} */
        @Override public int getMemory() {
            return row.getMemory();
        }

        /** {@inheritDoc} */
        @Override public long pointer() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public void incrementRefCount() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public void decrementRefCount() {
            throw new IllegalStateException();
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2TreeIndex rebuild() throws InterruptedException {
        IndexColumn[] cols = getIndexColumns();

        GridH2TreeIndex idx = new GridH2TreeIndex(getName(), getTable(),
            getIndexType().isUnique(), F.asList(cols));

        Thread thread = Thread.currentThread();

        long i = 0;

        try {
            GridCursor<GridH2Row> cursor = tree.find(null, null);

            while(cursor.next()) {
                GridH2Row row = cursor.get();

                // Check for interruptions every 1000 iterations.
                if (++i % 1000 == 0 && thread.isInterrupted())
                    throw new InterruptedException();

                idx.tree.put(row);
            }
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }

        return idx;
    }

    /**
     * Adapter from {@link NavigableMap} to {@link IgniteTree}.
     */
    private final class IgniteNavigableMapTree implements IgniteTree<GridSearchRowPointer, GridH2Row> {
        private NavigableMap<GridSearchRowPointer, GridH2Row> tree;

        public IgniteNavigableMapTree(NavigableMap<GridSearchRowPointer, GridH2Row> tree) {
            this.tree = tree;
        }

        @Override public GridH2Row put(GridH2Row value) throws IgniteCheckedException {
            return tree.put(value, value);
        }

        @Override public GridH2Row findOne(GridSearchRowPointer key) throws IgniteCheckedException {
            return tree.get(key);
        }

        @Override public GridCursor<GridH2Row> find(GridSearchRowPointer lower, GridSearchRowPointer upper)
            throws IgniteCheckedException {
            if (lower == null || upper == null)
                throw new NullPointerException();

            NavigableMap<GridSearchRowPointer, GridH2Row> subMap =
                tree.subMap(lower, false, upper, false);

            return new GridCursorIteratorWrapper<GridH2Row>(subMap.values().iterator());
        }

        @Override public GridH2Row remove(GridSearchRowPointer key) throws IgniteCheckedException {
            return tree.remove(key);
        }

        @Override public long size() throws IgniteCheckedException {
            return tree.size();
        }

        @Override public IgniteNavigableMapTree clone() {
            AbstractMap copy;

            try {
                copy = (AbstractMap) super.clone();
            } catch (final CloneNotSupportedException e) {
                throw DbException.convert(e);
            }

            return new IgniteNavigableMapTree((NavigableMap)copy);
        }
    }
}