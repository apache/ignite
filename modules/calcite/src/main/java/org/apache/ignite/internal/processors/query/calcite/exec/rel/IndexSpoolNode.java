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
import java.util.function.Supplier;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RuntimeTreeIndex;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Index spool node.
 */
public class IndexSpoolNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** Scan. */
    private ScanNode<Row> scan;

    /** Runtime index */
    private RuntimeTreeIndex<Row> idx;

    /** */
    private int requested;

    /** */
    private int waiting;

    /**
     * @param ctx Execution context.
     */
    public IndexSpoolNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        RelCollation collation,
        Comparator<Row> comp,
        Supplier<Row> lowerIdxConditions,
        Supplier<Row> upperIdxConditions
    ) {
        super(ctx, rowType);

        idx = new RuntimeTreeIndex<>(ctx, collation, comp);

        scan = new ScanNode<>(
            ctx,
            rowType,
            idx.scan(
                ctx,
                rowType,
                null,
                lowerIdxConditions,
                upperIdxConditions
            )
        );
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
    @Override public void request(int rowsCnt) {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0;

        try {
            checkState();

            if (!indexReady()) {
                requested = rowsCnt;

                requestSource();
            }
            else
                scan.request(rowsCnt);
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void requestSource() {
        waiting = IN_BUFFER_SIZE;

        source().request(IN_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        try {
            checkState();

            idx.push(row);

            waiting--;

            if (waiting == 0)
                context().execute(this::requestSource);
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        try {
            checkState();

            waiting = -1;

            scan.request(requested);
        }
        catch (Exception e) {
            scan.downstream().onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void closeInternal() {
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
}
