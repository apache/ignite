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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Runtime sorted index based on on-heap tree.
 */
public class RuntimeHashIndex<Row> implements GridIndex<Row>, AutoCloseable {
    /** */
    protected final ExecutionContext<Row> ectx;

    /** */
    protected final Comparator<Row> comp;

    /** */
    private final ImmutableBitSet keys;

    /** Rows. */
    private HashMap<GroupKey, List<Row>> rows;

    /**
     *
     */
    public RuntimeHashIndex(
        ExecutionContext<Row> ectx,
        ImmutableBitSet keys,
        Comparator<Row> comp
    ) {
        this.ectx = ectx;
        this.comp = comp;

        assert !F.isEmpty(keys);

        this.keys = keys;
        rows = new HashMap<>();
    }

    /**
     * Add row to index.
     */
    public void push(Row r) {
        List<Row> newEqRows = new ArrayList<>();

        List<Row> eqRows = rows.putIfAbsent(new GroupKey(), newEqRows);

        if (eqRows != null)
            eqRows.add(r);
        else
            newEqRows.add(r);
    }

    /** */
    @Override public void close() {
        rows.clear();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<Row> find(Row lower, Row upper, BPlusTree.TreeRowClosure<Row, Row> filterC) {
        assert filterC == null;

        assert lower.equals(upper) :
            "Lower and upper bounds must be equal for hash index: [lower=" + lower + ", upper=" + upper + ']';

        return new Cursor(null);
    }

    /**
     * Return an iterable to scan index range from lower to upper bounds inclusive,
     * filtered by {@code filter} predicate.
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
     *
     */
    private class Cursor implements GridCursor<Row> {
        /** Sub map iterator. */
        private final Iterator<Map.Entry<Row, List<Row>>> mapIt;

        /** Iterator over rows with equal index keys. */
        private Iterator<Row> listIt;

        /** */
        private Row row;

        /** */
        Cursor(SortedMap<Row, List<Row>> subMap) {
            mapIt = subMap.entrySet().iterator();
            listIt = null;
        }

        /** {@inheritDoc} */
        @Override public boolean next() throws IgniteCheckedException {
            if (!hasNext())
                return false;

            next0();

            return true;
        }

        /** */
        private boolean hasNext() {
            return listIt != null && listIt.hasNext() || mapIt.hasNext();
        }

        /** */
        private void next0() {
            if (listIt == null || !listIt.hasNext())
                listIt = mapIt.next().getValue().iterator();

            row = listIt.next();
        }

        /** {@inheritDoc} */
        @Override public Row get() throws IgniteCheckedException {
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
            GridIndex<Row> idx,
            Predicate<Row> filter,
            Supplier<Row> lowerBound,
            Supplier<Row> upperBound) {
            super(RuntimeHashIndex.this.ectx, rowType, idx, filter, lowerBound, upperBound, null);
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
        @Override protected BPlusTree.TreeRowClosure<Row, Row> filterClosure() {
            return null;
        }
    }
}
