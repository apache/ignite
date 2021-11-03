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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;

/**
 * Runtime sorted index based on on-heap tree.
 */
public class RuntimeTreeIndex<RowT> implements RuntimeIndex<RowT>, TreeIndex<RowT> {
    /**
     *
     */
    protected final ExecutionContext<RowT> ectx;

    /**
     *
     */
    protected final Comparator<RowT> comp;

    /** Collation. */
    private final RelCollation collation;

    /** Rows. */
    private TreeMap<RowT, List<RowT>> rows;

    /**
     *
     */
    public RuntimeTreeIndex(
            ExecutionContext<RowT> ectx,
            RelCollation collation,
            Comparator<RowT> comp
    ) {
        this.ectx = ectx;
        this.comp = comp;

        assert Objects.nonNull(collation);

        this.collation = collation;
        rows = new TreeMap<>(comp);
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT r) {
        List<RowT> newEqRows = new ArrayList<>();

        List<RowT> eqRows = rows.putIfAbsent(r, newEqRows);

        if (eqRows != null) {
            eqRows.add(r);
        } else {
            newEqRows.add(r);
        }
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
            return new CursorImpl(rows.subMap(lower, true, upper, true));
        } else if (ectx.rowHandler().get(firstCol, lower) == null && ectx.rowHandler().get(firstCol, upper) != null) {
            return new CursorImpl(rows.headMap(upper, true));
        } else if (ectx.rowHandler().get(firstCol, lower) != null && ectx.rowHandler().get(firstCol, upper) == null) {
            return new CursorImpl(rows.tailMap(lower, true));
        } else {
            return new CursorImpl(rows);
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
     *
     */
    private class CursorImpl implements Cursor<RowT> {
        /** Sub map iterator. */
        private final Iterator<Map.Entry<RowT, List<RowT>>> mapIt;

        /** Iterator over rows with equal index keys. */
        private Iterator<RowT> listIt;

        /**
         *
         */
        private RowT row;

        /**
         *
         */
        CursorImpl(SortedMap<RowT, List<RowT>> subMap) {
            mapIt = subMap.entrySet().iterator();
            listIt = null;
        }

        /** {@inheritDoc} */
        @Override
        public RowT next() throws IgniteInternalException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            advance();

            return listIt.next();
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            return listIt != null && listIt.hasNext() || mapIt.hasNext();
        }

        /**
         *
         */
        private void advance() {
            if (listIt == null || !listIt.hasNext()) {
                listIt = mapIt.next().getValue().iterator();
            }
        }

        /** {@inheritDoc} */
        @Override
        public void close() throws Exception {
        }

        /** {@inheritDoc} */
        @NotNull
        @Override
        public Iterator<RowT> iterator() {
            return this;
        }
    }

    /**
     *
     */
    private class IndexScan extends AbstractIndexScan<RowT, RowT> {
        /**
         * @param rowType    Row type.
         * @param idx        Physical index.
         * @param filter     Additional filters.
         * @param lowerBound Lower index scan bound.
         * @param upperBound Upper index scan bound.
         */
        IndexScan(
                RelDataType rowType,
                TreeIndex<RowT> idx,
                Predicate<RowT> filter,
                Supplier<RowT> lowerBound,
                Supplier<RowT> upperBound) {
            super(RuntimeTreeIndex.this.ectx, rowType, idx, filter, lowerBound, upperBound, null);
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
