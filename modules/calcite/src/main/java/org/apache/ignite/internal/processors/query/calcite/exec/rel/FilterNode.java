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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Predicate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

/**
 *
 */
public class FilterNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    private final Predicate<Row> pred;

    /** */
    private final Deque<Row> inBuf = new ArrayDeque<>(inBufSize);

    /** */
    private int requested;

    /** */
    private int waiting;

    /** */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     * @param pred Predicate.
     */
    public FilterNode(ExecutionContext<Row> ctx, RelDataType rowType, Predicate<Row> pred) {
        super(ctx, rowType);

        this.pred = pred;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (!inLoop)
            context().execute(this::doFilter, this::onError);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        if (pred.test(row))
            inBuf.add(row);

        filter();
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        filter();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        inBuf.clear();
    }

    /** */
    private void doFilter() throws Exception {
        checkState();

        filter();
    }

    /** */
    private void filter() throws Exception {
        inLoop = true;
        try {
            while (requested > 0 && !inBuf.isEmpty()) {
                checkState();

                requested--;
                downstream().push(inBuf.remove());
            }
        }
        finally {
            inLoop = false;
        }

        if (inBuf.isEmpty() && waiting == 0)
            source().request(waiting = inBufSize);

        if (waiting == -1 && requested > 0) {
            assert inBuf.isEmpty();

            requested = 0;
            downstream().end();
        }
    }
}
