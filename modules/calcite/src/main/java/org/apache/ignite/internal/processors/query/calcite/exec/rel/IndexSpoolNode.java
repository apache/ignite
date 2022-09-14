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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RuntimeHashIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.RuntimeIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.RuntimeSortedIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.BoundsValues;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Index spool node.
 */
public class IndexSpoolNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** Scan. */
    private final ScanNode<Row> scan;

    /** Runtime index */
    private final RuntimeIndex<Row> idx;

    /** */
    private int requested;

    /** */
    private int waiting;

    /**
     * @param ctx Execution context.
     */
    private IndexSpoolNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        RuntimeIndex<Row> idx,
        ScanNode<Row> scan
    ) {
        super(ctx, rowType);

        this.idx = idx;
        this.scan = scan;
    }

    /** */
    @Override public void onRegister(Downstream<Row> downstream) {
        scan.onRegister(downstream);
    }

    /** */
    @Override public Downstream<Row> downstream() {
        return scan.downstream();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        scan.rewind();
    }

    /** {@inheritDoc} */
    @Override public void rewind() {
        rewindInternal();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0;

        checkState();

        if (!indexReady()) {
            requested = rowsCnt;

            requestSource();
        }
        else
            scan.request(rowsCnt);
    }

    /** */
    private void requestSource() throws Exception {
        waiting = IN_BUFFER_SIZE;

        source().request(IN_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        checkState();

        idx.push(row);

        waiting--;

        if (waiting == 0)
            context().execute(this::requestSource, this::onError);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        checkState();

        waiting = -1;

        scan.request(requested);
    }

    /** {@inheritDoc} */
    @Override protected void closeInternal() {
        try {
            scan.close();
        }
        catch (Exception ex) {
            onError(ex);
        }

        try {
            idx.close();
        }
        catch (Exception ex) {
            onError(ex);
        }

        super.closeInternal();
    }

    /** */
    private boolean indexReady() {
        return waiting == -1;
    }

    /** */
    public static <Row> IndexSpoolNode<Row> createTreeSpool(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        RelCollation collation,
        Comparator<Row> comp,
        Predicate<Row> filter,
        Iterable<BoundsValues<Row>> boundsValues,
        Supplier<Row> lowerIdxBound,
        Supplier<Row> upperIdxBound
    ) {
        RuntimeSortedIndex<Row> idx = new RuntimeSortedIndex<>(ctx, collation, comp);

        ScanNode<Row> scan = new ScanNode<>(
            ctx,
            rowType,
            idx.scan(
                ctx,
                rowType,
                filter,
                boundsValues,
                lowerIdxBound,
                upperIdxBound
            )
        );

        return new IndexSpoolNode<>(ctx, rowType, idx, scan);
    }

    /** */
    public static <Row> IndexSpoolNode<Row> createHashSpool(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        ImmutableBitSet keys,
        @Nullable Predicate<Row> filter,
        Supplier<Row> searchRow
    ) {
        RuntimeHashIndex<Row> idx = new RuntimeHashIndex<>(ctx, keys);

        ScanNode<Row> scan = new ScanNode<>(
            ctx,
            rowType,
            idx.scan(searchRow, filter)
        );

        return new IndexSpoolNode<>(ctx, rowType, idx, scan);
    }
}
