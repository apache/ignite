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

import java.util.Iterator;
import org.apache.ignite.internal.util.lang.GridFilteredIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Index base.
 */
public abstract class GridH2IndexBase extends BaseIndex {
    /** */
    protected static final ThreadLocal<IndexingQueryFilter> filters = new ThreadLocal<>();

    /** */
    protected final int keyCol;

    /** */
    protected final int valCol;

    /**
     * @param keyCol Key column.
     * @param valCol Value column.
     */
    protected GridH2IndexBase(int keyCol, int valCol) {
        this.keyCol = keyCol;
        this.valCol = valCol;
    }

    /**
     * Sets key filters for current thread.
     *
     * @param fs Filters.
     */
    public static void setFiltersForThread(IndexingQueryFilter fs) {
        if (fs == null)
            filters.remove();
        else
            filters.set(fs);
    }

    /**
     * If the index supports rebuilding it has to creates its own copy.
     *
     * @return Rebuilt copy.
     * @throws InterruptedException If interrupted.
     */
    public GridH2IndexBase rebuild() throws InterruptedException {
        return this;
    }

    /**
     * Put row if absent.
     *
     * @param row Row.
     * @return Existing row or {@code null}.
     */
    public abstract GridH2Row put(GridH2Row row);

    /**
     * Remove row from index.
     *
     * @param row Row.
     * @return Removed row.
     */
    public abstract GridH2Row remove(SearchRow row);

    /**
     * Takes or sets existing snapshot to be used in current thread.
     *
     * @param s Optional existing snapshot to use.
     * @return Snapshot.
     */
    public Object takeSnapshot(@Nullable Object s) {
        return s;
    }

    /**
     * Releases snapshot for current thread.
     */
    public void releaseSnapshot() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int compareRows(SearchRow rowData, SearchRow compare) {
        if (rowData == compare)
            return 0;

        for (int i = 0, len = indexColumns.length; i < len; i++) {
            int index = columnIds[i];

            Value v1 = rowData.getValue(index);
            Value v2 = compare.getValue(index);

            if (v1 == null || v2 == null)
                return 0;

            int c = compareValues(v1, v2, indexColumns[i].sortType);

            if (c != 0)
                return c;
        }
        return 0;
    }

    /**
     * @param a First value.
     * @param b Second value.
     * @param sortType Sort type.
     * @return Comparison result.
     */
    private int compareValues(Value a, Value b, int sortType) {
        if (a == b)
            return 0;

        int comp = table.compareTypeSave(a, b);

        if ((sortType & SortOrder.DESCENDING) != 0)
            comp = -comp;

        return comp;
    }

    /**
     * Filters rows from expired ones and using predicate.
     *
     * @param iter Iterator over rows.
     * @return Filtered iterator.
     */
    protected Iterator<GridH2Row> filter(Iterator<GridH2Row> iter) {
        IgniteBiPredicate<Object, Object> p = null;

        IndexingQueryFilter f = filters.get();

        if (f != null) {
            String spaceName = ((GridH2Table)getTable()).spaceName();

            p = f.forSpace(spaceName);
        }

        return new FilteringIterator(iter, U.currentTimeMillis(), p);
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
        throw DbException.getUnsupportedException("remove index");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /**
     * Iterator which filters by expiration time and predicate.
     */
    protected class FilteringIterator extends GridFilteredIterator<GridH2Row> {
        /** */
        private final IgniteBiPredicate<Object, Object> fltr;

        /** */
        private final long time;

        /**
         * @param iter Iterator.
         * @param time Time for expired rows filtering.
         */
        protected FilteringIterator(Iterator<GridH2Row> iter, long time,
            IgniteBiPredicate<Object, Object> fltr) {
            super(iter);

            this.time = time;
            this.fltr = fltr;
        }

        /**
         * @param row Row.
         * @return If this row was accepted.
         */
        @SuppressWarnings("unchecked")
        @Override protected boolean accept(GridH2Row row) {
            if (row instanceof GridH2AbstractKeyValueRow) {
                if (((GridH2AbstractKeyValueRow) row).expirationTime() <= time)
                    return false;
            }

            if (fltr == null)
                return true;

            Object key = row.getValue(keyCol).getObject();
            Object val = row.getValue(valCol).getObject();

            assert key != null;
            assert val != null;

            return fltr.apply(key, val);
        }
    }
}