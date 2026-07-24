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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.GridBoundedPriorityQueue;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Sort node.
 */
public class SortNode<Row> extends MemoryTrackingNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** How many rows are requested by downstream. */
    private int requested;

    /** How many rows are we waiting for from the upstream. {@code -1} means end of stream. */
    private int waiting;

    /**  */
    private boolean inLoop;

    /** Rows buffer. */
    private final PriorityQueue<Row> rows;

    /** Reverse-ordered rows in case of limited sort. */
    private ArrayList<Row> reversed;

    /**
     * @param ctx Execution context.
     * @param comp Rows comparator.
     * @param offset Offset.
     * @param fetch Limit.
     */
    public SortNode(
        ExecutionContext<Row> ctx, RelDataType rowType,
        Comparator<Row> comp,
        @Nullable Supplier<BigDecimal> offset,
        @Nullable Supplier<BigDecimal> fetch
    ) {
        super(ctx, rowType);

        BigDecimal offsetVal = offset == null ? BigDecimal.ZERO : offset.get();
        BigDecimal fetchVal = fetch == null ? null : fetch.get();

        BigDecimal rowsToKeep = fetchVal == null ? null : fetchVal.add(offsetVal);

        if (rowsToKeep == null || rowsToKeep.signum() == 0
            || rowsToKeep.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
            rows = new PriorityQueue<>(comp);
        }
        else {
            rows = new GridBoundedPriorityQueue<>(rowsToKeep.intValueExact(),
                comp == null ? (Comparator<Row>)Comparator.reverseOrder() : comp.reversed());

            reversed = new ArrayList<>();
        }
    }

    /**
     * @param ctx Execution context.
     * @param comp Rows comparator.
     */
    public SortNode(ExecutionContext<Row> ctx, RelDataType rowType, Comparator<Row> comp) {
        this(ctx, rowType, comp, null, null);
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        rows.clear();
        if (reversed != null)
            reversed.clear();

        nodeMemoryTracker.reset();
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
        assert rowsCnt > 0 && requested == 0 : "rowsCnt=" + rowsCnt + ", requested=" + requested;
        assert waiting <= 0 : waiting;

        checkState();

        requested = rowsCnt;

        if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
        else if (!inLoop)
            context().execute(this::flush, this::onError);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0 : waiting;
        assert reversed == null || reversed.isEmpty() : reversed.size();

        checkState();

        waiting--;

        int size = rows.size();
        Row top = rows.peek();

        if (rows.add(row)) {
            nodeMemoryTracker.onRowAdded(row);

            if (size == rows.size()) // Row added, but size is not changed means another (top) row is evicted.
                nodeMemoryTracker.onRowRemoved(top);
        }

        if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0 : waiting;

        checkState();

        waiting = -1;

        flush();
    }

    /** */
    private void flush() throws Exception {
        if (isClosed())
            return;

        assert waiting == -1 : waiting;

        int processed = 0;

        inLoop = true;
        try {
            // Prepare final order (reversed).
            if (reversed != null && !rows.isEmpty()) {
                reversed.ensureCapacity(rows.size());

                while (!rows.isEmpty()) {
                    reversed.add(rows.poll());

                    if (++processed >= IN_BUFFER_SIZE) {
                        // Allow the others to do their job.
                        context().execute(this::flush, this::onError);

                        return;
                    }
                }

                processed = 0;
            }

            while (requested > 0 && (reversed == null ? !rows.isEmpty() : !reversed.isEmpty())) {
                checkState();

                requested--;

                Row row = reversed == null ? rows.poll() : reversed.remove(reversed.size() - 1);

                nodeMemoryTracker.onRowRemoved(row);

                downstream().push(row);

                if (++processed >= IN_BUFFER_SIZE && requested > 0) {
                    // allow others to do their job
                    context().execute(this::flush, this::onError);

                    return;
                }
            }

            if (reversed == null ? rows.isEmpty() : reversed.isEmpty()) {
                if (requested > 0)
                    downstream().end();

                requested = 0;
            }
        }
        finally {
            inLoop = false;
        }
    }
}
