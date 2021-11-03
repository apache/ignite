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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.Comparator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RuntimeHashIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.RuntimeIndex;
import org.apache.ignite.internal.processors.query.calcite.exec.RuntimeTreeIndex;

/**
 * Index spool node.
 */
public class IndexSpoolNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    /** Scan. */
    private final ScanNode<RowT> scan;

    /** Runtime index */
    private final RuntimeIndex<RowT> idx;

    /**
     *
     */
    private int requested;

    /**
     *
     */
    private int waiting;

    /**
     * @param ctx Execution context.
     */
    private IndexSpoolNode(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            RuntimeIndex<RowT> idx,
            ScanNode<RowT> scan
    ) {
        super(ctx, rowType);

        this.idx = idx;
        this.scan = scan;
    }

    /**
     *
     */
    @Override
    public void onRegister(Downstream<RowT> downstream) {
        scan.onRegister(downstream);
    }

    /**
     *
     */
    @Override
    public Downstream<RowT> downstream() {
        return scan.downstream();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        scan.rewind();
    }

    /** {@inheritDoc} */
    @Override
    public void rewind() {
        rewindInternal();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0;

        checkState();

        if (!indexReady()) {
            requested = rowsCnt;

            requestSource();
        } else {
            scan.request(rowsCnt);
        }
    }

    /**
     *
     */
    private void requestSource() throws Exception {
        waiting = inBufSize;

        source().request(inBufSize);
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        checkState();

        idx.push(row);

        waiting--;

        if (waiting == 0) {
            context().execute(this::requestSource, this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        checkState();

        waiting = -1;

        scan.request(requested);
    }

    /** {@inheritDoc} */
    @Override
    protected void closeInternal() {
        try {
            scan.close();
        } catch (Exception ex) {
            onError(ex);
        }

        try {
            idx.close();
        } catch (Exception ex) {
            onError(ex);
        }

        super.closeInternal();
    }

    /**
     *
     */
    private boolean indexReady() {
        return waiting == -1;
    }

    /**
     *
     */
    public static <RowT> IndexSpoolNode<RowT> createTreeSpool(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            RelCollation collation,
            Comparator<RowT> comp,
            Predicate<RowT> filter,
            Supplier<RowT> lowerIdxBound,
            Supplier<RowT> upperIdxBound
    ) {
        RuntimeTreeIndex<RowT> idx = new RuntimeTreeIndex<>(ctx, collation, comp);

        ScanNode<RowT> scan = new ScanNode<>(
                ctx,
                rowType,
                idx.scan(
                        ctx,
                        rowType,
                        filter,
                        lowerIdxBound,
                        upperIdxBound
                )
        );

        return new IndexSpoolNode<>(ctx, rowType, idx, scan);
    }

    /**
     *
     */
    public static <RowT> IndexSpoolNode<RowT> createHashSpool(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            ImmutableBitSet keys,
            Supplier<RowT> searchRow
    ) {
        RuntimeHashIndex<RowT> idx = new RuntimeHashIndex<>(ctx, keys);

        ScanNode<RowT> scan = new ScanNode<>(
                ctx,
                rowType,
                idx.scan(searchRow)
        );

        return new IndexSpoolNode<>(ctx, rowType, idx, scan);
    }
}
