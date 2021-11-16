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

import static org.apache.ignite.internal.util.CollectionUtils.first;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Runtime sorted index.
 */
public class RuntimeSortedIndex<RowT> implements RuntimeIndex<RowT>, TreeIndex<RowT> {
    protected final ExecutionContext<RowT> ectx;

    protected final Comparator<RowT> comp;

    private final RelCollation collation;

    private final ArrayList<RowT> rows = new ArrayList<>();
    
    /**
     * Constructor.
     *
     * @param ectx Execution context.
     * @param collation Index collation.
     * @param comp Index row comparator.
     */
    public RuntimeSortedIndex(
            ExecutionContext<RowT> ectx,
            RelCollation collation,
            Comparator<RowT> comp
    ) {
        this.ectx = ectx;
        this.comp = comp;

        assert Objects.nonNull(collation);

        this.collation = collation;
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT r) {
        assert rows.isEmpty() || comp.compare(r, rows.get(rows.size() - 1)) >= 0 : "Not sorted input";

        rows.add(r);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        rows.clear();
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<RowT> find(RowT lower, RowT upper) {
        int firstCol = first(collation.getKeys());

        if (ectx.rowHandler().get(firstCol, lower) != null && ectx.rowHandler().get(firstCol, upper) != null) {
            return new CursorImpl(rows, lower, upper);
        } else if (ectx.rowHandler().get(firstCol, lower) == null && ectx.rowHandler().get(firstCol, upper) != null) {
            return new CursorImpl(rows, null, upper);
        } else if (ectx.rowHandler().get(firstCol, lower) != null && ectx.rowHandler().get(firstCol, upper) == null) {
            return new CursorImpl(rows, lower, null);
        } else {
            return new CursorImpl(rows, null, null);
        }
    }

    /**
     * Creates iterable on the index.
     */
    public Iterable<RowT> scan(
            ExecutionContext<RowT> ectx,
            RelDataType rowType,
            Predicate<RowT> filter,
            Supplier<RowT> lowerBound,
            Supplier<RowT> upperBound
    ) {
        return new IndexScan(rowType, this, filter, lowerBound, upperBound);
    }

    /**
     * Cursor to navigate through a sorted list with duplicates.
     */
    private class CursorImpl implements Cursor<RowT> {
        /** List of rows. */
        private final List<RowT> rows;

        /** Upper bound. */
        private final RowT upper;

        /** Current index of list element. */
        private int idx;

        CursorImpl(List<RowT> rows, @Nullable RowT lower, @Nullable RowT upper) {
            this.rows = rows;
            this.upper = upper;

            idx = lower == null ? 0 : lowerBound(rows, lower);
        }

        /**
         * Searches the lower bound (skipping duplicates) using a binary search.
         *
         * @param rows List of rows.
         * @param bound Lower bound.
         * @return Lower bound position in the list.
         */
        private int lowerBound(List<RowT> rows, RowT bound) {
            int low = 0;
            int high = rows.size() - 1;
            int idx = -1;

            while (low <= high) {
                int mid = (high - low) / 2 + low;
                int compRes = comp.compare(rows.get(mid), bound);

                if (compRes > 0) {
                    high = mid - 1;
                } else if (compRes == 0) {
                    idx = mid;
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }

            return idx == -1 ? low : idx;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            if (idx == rows.size() || (upper != null && comp.compare(upper, rows.get(idx)) < 0)) {
                return false;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override
        public RowT next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return rows.get(idx++);
        }

        /** {@inheritDoc} */
        @Override
        public void close() throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override
        @NotNull public Iterator<RowT> iterator() {
            return this;
        }
    }

    /**
     * Index scan for RuntimeSortedIndex.
     */
    private class IndexScan extends AbstractIndexScan<RowT, RowT> {
        IndexScan(
                RelDataType rowType,
                TreeIndex<RowT> idx,
                Predicate<RowT> filter,
                Supplier<RowT> lowerBound,
                Supplier<RowT> upperBound
        ) {
            super(RuntimeSortedIndex.this.ectx, rowType, idx, filter, lowerBound, upperBound, null);
        }

        /** {@inheritDoc} */
        @Override
        protected RowT row2indexRow(RowT bound) {
            return bound;
        }

        /** {@inheritDoc} */
        @Override
        protected RowT indexRow2Row(RowT row) {
            return row;
        }
    }
}
