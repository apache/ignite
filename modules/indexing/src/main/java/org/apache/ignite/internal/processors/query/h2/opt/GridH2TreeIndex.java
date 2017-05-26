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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.util.GridCursorIteratorWrapper;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeMap;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for snapshotable segmented tree indexes.
 */
@SuppressWarnings("ComparatorNotSerializable")
public class GridH2TreeIndex extends GridH2IndexBase implements Comparator<GridSearchRowPointer> {
    /** */
    private final IgniteNavigableMapTree[] segments;

    /** */
    private final boolean snapshotEnabled;

    /**
     * Constructor with index initialization. Creates index with single segment.
     *
     * @param name Index name.
     * @param tbl Table.
     * @param pk If this index is primary key.
     * @param colsList Index columns list.
     */
    @SuppressWarnings("unchecked")
    public GridH2TreeIndex(String name, GridH2Table tbl, boolean pk, List<IndexColumn> colsList) {
        this(name, tbl, pk, colsList, 1);
    }

    /**
     * Constructor with index initialization.
     *
     * @param name Index name.
     * @param tbl Table.
     * @param pk If this index is primary key.
     * @param colsList Index columns list.
     * @param segmentsCnt Number of segments.
     */
    @SuppressWarnings("unchecked")
    public GridH2TreeIndex(String name, GridH2Table tbl, boolean pk, List<IndexColumn> colsList, int segmentsCnt) {
        assert segmentsCnt > 0 : segmentsCnt;

        IndexColumn[] cols = colsList.toArray(new IndexColumn[colsList.size()]);

        IndexColumn.mapColumns(cols, tbl);

        initBaseIndex(tbl, 0, name, cols,
            pk ? IndexType.createPrimaryKey(false, false) : IndexType.createNonUnique(false, false, false));

        segments = new IgniteNavigableMapTree[segmentsCnt];

        final GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (desc == null || desc.memory() == null) {
            snapshotEnabled = desc == null || desc.snapshotableIndex();

            if (snapshotEnabled) {
                for (int i = 0; i < segmentsCnt; i++) {
                    segments[i] = new IgniteNavigableMapTree(new SnapTreeMap<GridSearchRowPointer, GridH2Row>(this) {
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
            }
            else {
                for (int i = 0; i < segmentsCnt; i++) {
                    segments[i] = new IgniteNavigableMapTree(
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
        }
        else {
            assert desc.snapshotableIndex() : desc;

            snapshotEnabled = true;

            for (int i = 0; i < segmentsCnt; i++) {
                segments[i] = new IgniteNavigableMapTree(new GridOffHeapSnapTreeMap<GridSearchRowPointer, GridH2Row>(desc, desc, desc.memory(), desc.guard(), this) {
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
        }

        initDistributedJoinMessaging(tbl);
    }

    /** {@inheritDoc} */
    @Override protected IgniteTree doTakeSnapshot() {
        assert snapshotEnabled;

        int seg = threadLocalSegment();

        IgniteNavigableMapTree tree = segments[seg];

        return tree.clone();
    }

    /** {@inheritDoc} */
    @Override protected final IgniteTree treeForRead(int seg) {
        if (!snapshotEnabled)
            return segments[seg];

        IgniteTree res = threadLocalSnapshot();

        if (res == null)
            return segments[seg];

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

        int seg = threadLocalSegment();

        // Fast path if we don't need to perform any filtering.
        if (f == null || f.forCache((getTable()).cacheName()) == null)
            try {
                return treeForRead(seg).size();
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
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter,
        SortOrder sortOrder, HashSet<Column> cols) {
        long rowCnt = getRowCountApproximation();
        double baseCost = getCostRangeIndex(masks, rowCnt, filters, filter, sortOrder, false, cols);
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
    @Override public GridH2Row findOne(GridH2Row row) {
        int seg = segmentForRow(row);

        return segments[seg].findOne(row);
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
        int seg = threadLocalSegment();

        IgniteTree t = treeForRead(seg);

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
        return true;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        try {
            int seg = threadLocalSegment();

            IgniteTree t = treeForRead(seg);

            GridH2Row row = (GridH2Row)(first ? t.findFirst() : t.findLast());

            return new SingleRowCursor(row);
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridH2Row put(GridH2Row row) {
        int seg = segmentForRow(row);

        return segments[seg].put(row);
    }

    /** {@inheritDoc} */
    @Override public GridH2Row remove(SearchRow row) {
        GridSearchRowPointer comparable = comparable(row, 0);

        int seg = segmentForRow(row);

        return segments[seg].remove(comparable);
    }

    /** {@inheritDoc} */
    @Override protected int segmentsCount() {
        return segments.length;
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

    /**
     * Adapter from {@link NavigableMap} to {@link IgniteTree}.
     */
    private static final class IgniteNavigableMapTree implements IgniteTree<GridSearchRowPointer, GridH2Row>, Cloneable {
        /** Tree. */
        private final NavigableMap<GridSearchRowPointer, GridH2Row> tree;

        /**
         * @param tree Tree.
         */
        private IgniteNavigableMapTree(NavigableMap<GridSearchRowPointer, GridH2Row> tree) {
            this.tree = tree;
        }

        /** {@inheritDoc} */
        @Override public void invoke(GridSearchRowPointer key, Object x, InvokeClosure<GridH2Row> c) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public GridH2Row put(GridH2Row val) {
            return tree.put(val, val);
        }

        /** {@inheritDoc} */
        @Override public GridH2Row findOne(GridSearchRowPointer key) {
            return tree.get(key);
        }

        /** {@inheritDoc} */
        @Override public GridCursor<GridH2Row> find(GridSearchRowPointer lower, GridSearchRowPointer upper)
            throws IgniteCheckedException {

            Collection<GridH2Row> rows;

            if (lower == null && upper == null)
                rows = tree.values();
            else if (lower != null && upper == null)
                rows = tree.tailMap(lower).values();
            else if (lower == null)
                rows = tree.headMap(upper).values();
            else
                rows = tree.subMap(lower, false, upper, false).values();

            return new GridCursorIteratorWrapper<>(rows.iterator());
        }

        /** {@inheritDoc} */
        @Override public GridH2Row findFirst() throws IgniteCheckedException {
            Map.Entry<GridSearchRowPointer, GridH2Row> first = tree.firstEntry();
            return (first == null) ? null : first.getValue();
        }

        /** {@inheritDoc} */
        @Override public GridH2Row findLast() throws IgniteCheckedException {
            Map.Entry<GridSearchRowPointer, GridH2Row> last = tree.lastEntry();
            return (last == null) ? null : last.getValue();
        }

        /** {@inheritDoc} */
        @Override public GridH2Row remove(GridSearchRowPointer key) {
            return tree.remove(key);
        }

        /** {@inheritDoc} */
        @Override public long size() {
            return tree.size();
        }

        /** {@inheritDoc} */
        @Override public IgniteNavigableMapTree clone() {
            IgniteNavigableMapTree cp;

            try {
                cp = (IgniteNavigableMapTree)super.clone();
            }
            catch (final CloneNotSupportedException e) {
                throw DbException.convert(e);
            }

            return new IgniteNavigableMapTree(cp.tree);
        }
    }
}