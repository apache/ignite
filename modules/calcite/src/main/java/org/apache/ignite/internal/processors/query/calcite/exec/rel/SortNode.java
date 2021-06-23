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
import java.util.PriorityQueue;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

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
    private final PriorityQueue<Row> rows;

    /**
     * @param ctx Execution context.
     * @param comp Rows comparator.
     */
    public SortNode(ExecutionContext<Row> ctx, RelDataType rowType, Comparator<Row> comp) {
        super(ctx, rowType);

        rows = comp == null ? new PriorityQueue<>() : new PriorityQueue<>(comp);
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
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        checkState();

        requested = rowsCnt;

        if (waiting == 0)
            source().request(waiting = inBufSize);
        else if (!inLoop)
            context().execute(this::flush, this::onError);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        rows.add(row);

        if (waiting == 0)
            source().request(waiting = inBufSize);
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

                downstream().push(rows.poll());

                if (++processed >= inBufSize && requested > 0) {
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
}
