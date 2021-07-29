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
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
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
    @Override public GridCursor<Row> find(Row lower, Row upper, IndexQueryContext qctx) {
        assert qctx == null;

        int firstCol = F.first(collation.getKeys());

        if (ectx.rowHandler().get(firstCol, lower) != null && ectx.rowHandler().get(firstCol, upper) != null)
            return new Cursor(rows, lower, upper);
        else if (ectx.rowHandler().get(firstCol, lower) == null && ectx.rowHandler().get(firstCol, upper) != null)
            return new Cursor(rows, null, upper);
        else if (ectx.rowHandler().get(firstCol, lower) != null && ectx.rowHandler().get(firstCol, upper) == null)
            return new Cursor(rows, lower, null);
        else
            return new Cursor(rows, null, null);
    }

    /**
     * Creates iterable on the index.
     */
    public Iterable<Row> scan(
        ExecutionContext<Row> ectx,
        RelDataType rowType,
        Predicate<Row> filter,
        Supplier<Row> lowerBound,
        Supplier<Row> upperBound
    ) {
        return new IndexScan(rowType, this, filter, lowerBound, upperBound);
    }

    /**
     * Cursor to navigate through a sorted list with duplicates.
     */
    private class Cursor implements GridCursor<Row> {
        /** List of rows. */
        private final List<Row> rows;

        /** Upper bound. */
        private final Row upper;

        /** Current row. */
        private Row row;

        /** Current index of list element. */
        private int idx;

        /**
         * @param rows List of rows.
         * @param lower Lower bound (inclusive).
         * @param upper Upper bound (inclusive).
         */
        Cursor(List<Row> rows, @Nullable Row lower, @Nullable Row upper) {
            this.rows = rows;
            this.upper = upper;

            idx = lower == null ? 0 : lowerBound(rows, lower);
        }

        /**
         * Searches the lower bound (skipping duplicates) using a binary search.
         *
         * @param rows List of rows.
         * @param target Lower bound.
         * @return Lower bound position in the list.
         */
        private int lowerBound(List<Row> rows, Row target) {
            int low = 0, high = rows.size() - 1, idx = -1;

            while (low <= high) {
                int mid = (high - low) / 2 + low;

                if (comp.compare(rows.get(mid), target) > 0)
                    high = mid - 1;
                else if (comp.compare(rows.get(mid), target) == 0) {
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
            if (idx == rows.size() || (upper != null && comp.compare(upper, rows.get(idx)) < 0))
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
         * @param lowerBound Lower index scan bound.
         * @param upperBound Upper index scan bound.
         */
        IndexScan(
            RelDataType rowType,
            TreeIndex<Row> idx,
            Predicate<Row> filter,
            Supplier<Row> lowerBound,
            Supplier<Row> upperBound) {
            super(RuntimeSortedIndex.this.ectx, rowType, idx, filter, lowerBound, upperBound, null);
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
