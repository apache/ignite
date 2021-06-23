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

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.util.FilteringIterator;
import org.apache.ignite.internal.processors.query.calcite.util.TransformingIterator;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Abstract index scan.
 */
public abstract class AbstractIndexScan<Row, IdxRow> implements Iterable<Row>, AutoCloseable {
    /** */
    private final TreeIndex<IdxRow> idx;

    /** Additional filters. */
    private final Predicate<Row> filters;

    /** Lower index scan bound. */
    private final Supplier<Row> lowerBound;

    /** Upper index scan bound. */
    private final Supplier<Row> upperBound;

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
     * @param lowerBound Lower index scan bound.
     * @param upperBound Upper index scan bound.
     */
    protected AbstractIndexScan(
        ExecutionContext<Row> ectx,
        RelDataType rowType,
        TreeIndex<IdxRow> idx,
        Predicate<Row> filters,
        Supplier<Row> lowerBound,
        Supplier<Row> upperBound,
        Function<Row, Row> rowTransformer
    ) {
        this.ectx = ectx;
        this.rowType = rowType;
        this.idx = idx;
        this.filters = filters;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.rowTransformer = rowTransformer;
    }

    /** {@inheritDoc} */
    @Override public synchronized Iterator<Row> iterator() {
        IdxRow lower = lowerBound == null ? null : row2indexRow(lowerBound.get());
        IdxRow upper = upperBound == null ? null : row2indexRow(upperBound.get());

        Iterator<Row> it = new TransformingIterator<>(
            idx.find(lower, upper),
            this::indexRow2Row
        );

        it = new FilteringIterator<>(it, filters);

        if (rowTransformer != null)
            it = new TransformingIterator<>(it, rowTransformer);

        return it;
    }

    /** */
    protected abstract IdxRow row2indexRow(Row bound);

    /** */
    protected abstract Row indexRow2Row(IdxRow idxRow) throws IgniteInternalException;

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }
}
