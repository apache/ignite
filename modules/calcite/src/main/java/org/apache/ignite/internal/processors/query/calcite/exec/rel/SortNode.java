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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

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
    public SortNode(ExecutionContext<Row> ctx, Comparator<Row> comp) {
        super(ctx);

        rows = comp == null ? new PriorityQueue<>() : new PriorityQueue<>(comp);
    }

    /** {@inheritDoc} */
    @Override protected void onRewind() {
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
    @Override public void request(int rowsCnt) {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        try {
            checkState();

            requested = rowsCnt;

            if (waiting == 0)
                source().request(waiting = IN_BUFFER_SIZE);
            else if (!inLoop)
                context().execute(this::doFlush);
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void doFlush() {
        try {
            flush();
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) {
        assert downstream() != null;
        assert waiting > 0;

        try {
            checkState();

            waiting--;

            rows.add(row);

            if (waiting == 0)
                source().request(waiting = IN_BUFFER_SIZE);
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        assert downstream() != null;
        assert waiting > 0;

        try {
            checkState();

            waiting = -1;

            flush();
        }
        catch (Exception e) {
            downstream().onError(e);
        }
    }

    /** */
    private void flush() throws IgniteCheckedException {
        assert waiting == -1;

        int processed = 0;

        inLoop = true;
        try {
            while (requested > 0 && !rows.isEmpty()) {
                checkState();

                requested--;

                downstream().push(rows.poll());

                if (++processed >= IN_BUFFER_SIZE && requested > 0) {
                    // allow others to do their job
                    context().execute(this::doFlush);

                    return;
                }
            }

            if (requested >= 0) {
                downstream().end();
                requested = 0;
            }
        }
        finally {
            inLoop = false;
        }
    }
}
