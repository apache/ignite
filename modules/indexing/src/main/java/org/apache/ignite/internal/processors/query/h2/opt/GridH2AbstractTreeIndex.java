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

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
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
public abstract class GridH2AbstractTreeIndex extends GridH2IndexBase implements Comparator<GridSearchRowPointer> {

    /** */
    protected abstract boolean isSnapshotEnabled();

    /** {@inheritDoc} */
    @Override public long getRowCount(@Nullable Session ses) {
        IndexingQueryFilter f = threadLocalFilter();

        // Fast path if we don't need to perform any filtering.
        if (f == null || f.forSpace((getTable()).spaceName()) == null)
            return treeForRead().size();

        Iterator<GridH2Row> iter = doFind(null, false, null);

        long size = 0;

        while (iter.hasNext()) {
            iter.next();

            size++;
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
        return new GridH2Cursor(doFind(first, true, last));
    }

    /** {@inheritDoc} */
    @Override public Cursor findNext(Session ses, SearchRow higherThan, SearchRow last) {
        return new GridH2Cursor(doFind(higherThan, false, last));
    }

    /**
     * Finds row with key equal one in given search row.
     * WARNING!! Method call must be protected by {@link GridUnsafeGuard#begin()}
     * {@link GridUnsafeGuard#end()} block.
     *
     * @param row Search row.
     * @return Row.
     */
    public abstract GridH2Row findOne(GridSearchRowPointer row);

    /**
     * Returns sub-tree bounded by given values.
     *
     * @param first Lower bound.
     * @param includeFirst Whether lower bound should be inclusive.
     * @param last Upper bound always inclusive.
     * @return Iterator over rows in given range.
     */
    @SuppressWarnings("unchecked")
    private Iterator<GridH2Row> doFind(@Nullable SearchRow first, boolean includeFirst, @Nullable SearchRow last) {
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> t = treeForRead();

        return doFind0(t, first, includeFirst, last, threadLocalFilter());
    }

    /** {@inheritDoc} */
    @Override protected final Iterator<GridH2Row> doFind0(
        ConcurrentNavigableMap<GridSearchRowPointer, GridH2Row> t,
        @Nullable SearchRow first,
        boolean includeFirst,
        @Nullable SearchRow last,
        IndexingQueryFilter filter
    ) {
        includeFirst &= first != null;

        NavigableMap<GridSearchRowPointer, GridH2Row> range = subTree(t, comparable(first, includeFirst ? -1 : 1),
            comparable(last, 1));

        if (range == null)
            return new GridEmptyIterator<>();

        return filter(range.values().iterator(), filter);
    }

    /**
     * @param row Row.
     * @param bias Bias.
     * @return Comparable row.
     */
    protected GridSearchRowPointer comparable(SearchRow row, int bias) {
        if (row == null)
            return null;

        if (bias == 0 && row instanceof GridH2Row)
            return (GridSearchRowPointer)row;

        return new ComparableRow(row, bias);
    }

    /**
     * Takes sup-map from given one.
     *
     * @param map Map.
     * @param first Lower bound.
     * @param last Upper bound.
     * @return Sub-map.
     */
    @SuppressWarnings({"IfMayBeConditional", "TypeMayBeWeakened"})
    private NavigableMap<GridSearchRowPointer, GridH2Row> subTree(NavigableMap<GridSearchRowPointer, GridH2Row> map,
        @Nullable GridSearchRowPointer first, @Nullable GridSearchRowPointer last) {
        // We take exclusive bounds because it is possible that one search row will be equal to multiple key rows
        // in tree and we must return them all.
        if (first == null) {
            if (last == null)
                return map;
            else
                return map.headMap(last, false);
        }
        else {
            if (last == null)
                return map.tailMap(first, false);
            else {
                if (compare(first, last) > 0)
                    return null;

                return map.subMap(first, false, last, false);
            }
        }
    }

    /**
     * Gets iterator over all rows in this index.
     *
     * @return Rows iterator.
     */
    Iterator<GridH2Row> rows() {
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

    /**
     * Comparable row with bias. Will be used for queries to have correct bounds (in case of multicolumn index
     * and query on few first columns we will multiple equal entries in tree).
     */
    final class ComparableRow implements GridSearchRowPointer, Comparable<SearchRow> {
        /** */
        private final SearchRow row;

        /** */
        private final int bias;

        /**
         * @param row Row.
         * @param bias Bias.
         */
         ComparableRow(SearchRow row, int bias) {
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
}
