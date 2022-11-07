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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeCondition;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract index scan.
 */
public abstract class AbstractIndexScan<Row, IdxRow> implements Iterable<Row>, AutoCloseable {
    /** */
    private final TreeIndex<IdxRow> idx;

    /** Additional filters. */
    private final Predicate<Row> filters;

    /** Index scan bounds. */
    private final RangeIterable<Row> ranges;

    /** */
    private final Function<Row, Row> rowTransformer;

    /** */
    protected final ExecutionContext<Row> ectx;

    /** */
    protected final RelDataType rowType;

    /**
     * @param ectx Execution context.
     * @param idx Physical index.
     * @param filters Additional filters.
     * @param ranges Index scan bounds.
     */
    protected AbstractIndexScan(
        ExecutionContext<Row> ectx,
        RelDataType rowType,
        TreeIndex<IdxRow> idx,
        Predicate<Row> filters,
        RangeIterable<Row> ranges,
        Function<Row, Row> rowTransformer
    ) {
        this.ectx = ectx;
        this.rowType = rowType;
        this.idx = idx;
        this.filters = filters;
        this.ranges = ranges;
        this.rowTransformer = rowTransformer;
    }

    /** {@inheritDoc} */
    @Override public synchronized Iterator<Row> iterator() {
        if (ranges == null)
            return new IteratorImpl(idx.find(null, null, true, true, indexQueryContext()));

        IgniteClosure<RangeCondition<Row>, IteratorImpl> clo = range -> {
            IdxRow lower = range.lower() == null ? null : row2indexRow(range.lower());
            IdxRow upper = range.upper() == null ? null : row2indexRow(range.upper());

            return new IteratorImpl(
                idx.find(lower, upper, range.lowerInclude(), range.upperInclude(), indexQueryContext()));
        };

        if (!ranges.multiBounds()) {
            Iterator<RangeCondition<Row>> it = ranges.iterator();

            if (it.hasNext())
                return clo.apply(it.next());
            else
                return Collections.emptyIterator();
        }

        return F.flat(F.iterator(ranges, clo, true));
    }

    /** */
    protected abstract IdxRow row2indexRow(Row bound);

    /** */
    protected abstract Row indexRow2Row(IdxRow idxRow) throws IgniteCheckedException;

    /** */
    protected abstract IndexQueryContext indexQueryContext();

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** */
    private class IteratorImpl extends GridIteratorAdapter<Row> {
        /** */
        private final GridCursor<IdxRow> cursor;

        /** Next element. */
        private Row next;

        /** */
        private IteratorImpl(@NotNull GridCursor<IdxRow> cursor) {
            this.cursor = cursor;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() throws IgniteCheckedException {
            advance();

            return next != null;
        }

        /** {@inheritDoc} */
        @Override public Row nextX() throws IgniteCheckedException {
            advance();

            if (next == null)
                throw new NoSuchElementException();

            Row res = next;

            next = null;

            return res;
        }

        /** {@inheritDoc} */
        @Override public void removeX() {
            throw new UnsupportedOperationException("Remove is not supported.");
        }

        /** */
        private void advance() throws IgniteCheckedException {
            assert cursor != null;

            if (next != null)
                return;

            while (next == null && cursor.next()) {
                IdxRow idxRow = cursor.get();

                Row r = indexRow2Row(idxRow);

                if (filters != null && !filters.test(r))
                    continue;

                if (rowTransformer != null)
                    r = rowTransformer.apply(r);

                next = r;
            }
        }
    }
}
