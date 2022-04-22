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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Sort node.
 */
public class SortNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** How many rows are requested by downstream. */
    private int requested;

    /** How many rows are we waiting for from the upstream. {@code -1} means end of stream. */
    private int waiting;

    /**  */
    private boolean inLoop;

    /** Rows buffer. */
    private final List<Row> rows;

    /** Rows comparator. */
    private final Comparator<Row> rowsCmp;

    /** SQL select limit. Negative if disabled. */
    private final int limit;

    /**
     * @param ctx Execution context.
     * @param comp Rows comparator.
     * @param offset Offset.
     * @param limit Limit.
     */
    public SortNode(ExecutionContext<Row> ctx, RelDataType rowType, Comparator<Row> comp,
        @Nullable Supplier<Integer> offset, @Nullable Supplier<Integer> limit) {
        super(ctx, rowType);

        assert limit == null || limit.get() >= 0;
        assert offset == null || offset.get() >= 0;

        this.limit = limit == null ? -1 : limit.get() + (offset == null ? 0 : offset.get());

        this.rows = this.limit < 0 ? new ArrayList<>() : new ArrayList<>(this.limit + 1);

        this.rowsCmp = comp == null ? new Comparator<Row>() {
            @Override public int compare(Row o1, Row o2) {
                return ((Comparable<Row>)o1).compareTo(o2);
            }
        } : comp;
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
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

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
        assert waiting > 0;

        checkState();

        waiting--;

        addRow(row);

        if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        flush();
    }

    /** */
    private void flush() throws Exception {
        if (isClosed())
            return;

        assert waiting == -1;

        int processed = 0;

        inLoop = true;
        try {
            while (requested > 0 && !rows.isEmpty()) {
                checkState();

                requested--;

                downstream().push(rows.remove(0));

                if (++processed >= IN_BUFFER_SIZE && requested > 0) {
                    // allow others to do their job
                    context().execute(this::flush, this::onError);

                    return;
                }
            }

            if (rows.isEmpty()) {
                if (requested > 0)
                    downstream().end();

                requested = 0;
            }
        }
        finally {
            inLoop = false;
        }
    }

    /** Adds new row in proper order. */
    private void addRow(Row row) {
        int insertIdx = Collections.binarySearch(rows, row, rowsCmp);

        if (insertIdx < 0)
            rows.add(-insertIdx - 1, row);
        else
            rows.add(insertIdx + 1, row);

        while (limit >= 0 && rows.size() > limit)
            rows.remove(rows.size() - 1);
    }
}
