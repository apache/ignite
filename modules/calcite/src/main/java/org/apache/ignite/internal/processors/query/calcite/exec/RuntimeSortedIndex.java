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
import java.util.Objects;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;

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

        return new SortedListRangeCursor<>(comp, rows, lowerRow, upperRow, lowerInclude, upperInclude);
    }

    /**
     * Creates iterable on the index.
     */
    public Iterable<Row> scan(
        ExecutionContext<Row> ectx,
        RelDataType rowType,
        RangeIterable<Row> ranges
    ) {
        return new IndexScan(rowType, this, ranges);
    }

    /**
     *
     */
    private class IndexScan extends AbstractIndexScan<Row, Row> {
        /**
         * @param rowType Row type.
         * @param idx Physical index.
         * @param ranges Index scan bounds.
         */
        IndexScan(
            RelDataType rowType,
            TreeIndex<Row> idx,
            RangeIterable<Row> ranges
        ) {
            super(RuntimeSortedIndex.this.ectx, rowType, idx, ranges);
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
