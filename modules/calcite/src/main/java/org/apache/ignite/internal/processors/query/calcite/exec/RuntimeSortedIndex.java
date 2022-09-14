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
package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.BoundsValues;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Runtime sorted index.
 */
public class RuntimeSortedIndex<Row> implements RuntimeIndex<Row>, TreeIndex<Row> {
    /** */
    protected final ExecutionContext<Row> ectx;

    /** */
    protected final Comparator<Row> comp;

    /** Collation. */
    private final RelCollation collation;

    /** Rows. */
    private final ArrayList<Row> rows = new ArrayList<>();

    /**
     *
     */
    public RuntimeSortedIndex(
        ExecutionContext<Row> ectx,
        RelCollation collation,
        Comparator<Row> comp
    ) {
        this.ectx = ectx;
        this.comp = comp;

        assert Objects.nonNull(collation);

        this.collation = collation;
    }

    /** {@inheritDoc} */
    @Override public void push(Row r) {
        assert rows.isEmpty() || comp.compare(r, rows.get(rows.size() - 1)) >= 0 : "Not sorted input";

        rows.add(r);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        rows.clear();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<Row> find(
        Row lower,
        Row upper,
        boolean lowerInclude,
        boolean upperInclude,
        IndexQueryContext qctx
    ) {
        assert qctx == null;

        int firstCol = F.first(collation.getKeys());

        Object lowerBound = (lower == null) ? null : ectx.rowHandler().get(firstCol, lower);
        Object upperBound = (upper == null) ? null : ectx.rowHandler().get(firstCol, upper);

        Row lowerRow = (lowerBound == null) ? null : lower;
        Row upperRow = (upperBound == null) ? null : upper;

        return new Cursor(rows, lowerRow, upperRow, lowerInclude, upperInclude);
    }

    /**
     * Creates iterable on the index.
     */
    public Iterable<Row> scan(
        ExecutionContext<Row> ectx,
        RelDataType rowType,
        Predicate<Row> filter,
        Iterable<BoundsValues<Row>> boundsValues
    ) {
        return new IndexScan(rowType, this, filter, boundsValues);
    }

    /**
     * Cursor to navigate through a sorted list with duplicates.
     */
    private class Cursor implements GridCursor<Row> {
        /** List of rows. */
        private final List<Row> rows;

        /** Upper bound. */
        private final Row upper;

        /** Include upper bound. */
        private final boolean includeUpper;

        /** Current row. */
        private Row row;

        /** Current index of list element. */
        private int idx;

        /**
         * @param rows List of rows.
         * @param lower Lower bound.
         * @param upper Upper bound.
         * @param lowerInclude {@code True} for inclusive lower bound.
         * @param upperInclude {@code True} for inclusive upper bound.
         */
        Cursor(List<Row> rows, @Nullable Row lower, @Nullable Row upper, boolean lowerInclude, boolean upperInclude) {
            this.rows = rows;
            this.upper = upper;
            this.includeUpper = upperInclude;

            idx = lower == null ? 0 : lowerBound(rows, lower, lowerInclude);
        }

        /**
         * Searches the lower bound (skipping duplicates) using a binary search.
         *
         * @param rows List of rows.
         * @param bound Lower bound.
         * @return Lower bound position in the list.
         */
        private int lowerBound(List<Row> rows, Row bound, boolean includeBound) {
            int low = 0, high = rows.size() - 1, idx = -1;

            while (low <= high) {
                int mid = (high - low) / 2 + low;
                int compRes = comp.compare(rows.get(mid), bound);

                if (compRes > 0)
                    high = mid - 1;
                else if (compRes == 0 && includeBound) {
                    idx = mid;
                    high = mid - 1;
                }
                else
                    low = mid + 1;
            }

            return idx == -1 ? low : idx;
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (idx == rows.size() || (upper != null && comp.compare(upper, rows.get(idx)) < (includeUpper ? 0 : 1)))
                return false;

            row = rows.get(idx++);

            return true;
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return row;
        }
    }

    /**
     *
     */
    private class IndexScan extends AbstractIndexScan<Row, Row> {
        /**
         * @param rowType Row type.
         * @param idx Physical index.
         * @param filter Additional filters.
         * @param boundsValues Index scan bounds.
         */
        IndexScan(
            RelDataType rowType,
            TreeIndex<Row> idx,
            Predicate<Row> filter,
            Iterable<BoundsValues<Row>> boundsValues
        ) {
            super(RuntimeSortedIndex.this.ectx, rowType, idx, filter, boundsValues, null);
        }

        /** {@inheritDoc} */
        @Override protected Row row2indexRow(Row bound) {
            return bound;
        }

        /** {@inheritDoc} */
        @Override protected Row indexRow2Row(Row row) {
            return row;
        }

        /** */
        @Override protected IndexQueryContext indexQueryContext() {
            return null;
        }
    }
}
