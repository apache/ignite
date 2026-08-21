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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/** Coordinator-side executor for recursive UNION ALL. */
public class RepeatUnionNode<Row> extends AbstractNode<Row> implements Downstream<Row> {
    /** Index of the seed input. */
    private static final int SEED_SOURCE = 0;

    /** Index of the recursive-term input. */
    private static final int RECURSIVE_SOURCE = 1;

    /** Query-local recursive state. */
    private final RecursiveCteState<Row> state;

    /** Maximum number of recursive iterations, or a negative value for no limit. */
    private final int iterationLimit;

    /** Index of the active source. */
    private int curSrc = SEED_SOURCE;

    /** Number of rows still requested by downstream. */
    private int waiting;

    /** Rows produced by the current recursive iteration. */
    private int produced;

    /** Number of completed recursive iterations. */
    private int iteration;

    /** */
    public RepeatUnionNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        RecursiveCteState<Row> state,
        int iterationLimit
    ) {
        super(ctx, rowType);

        this.state = state;
        this.iterationLimit = iterationLimit;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && waiting == 0;

        checkState();

        waiting = rowsCnt;
        source().request(rowsCnt);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        if (curSrc == RECURSIVE_SOURCE)
            produced++;

        downstream().push(row);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        if (curSrc == SEED_SOURCE) {
            if (iterationLimit == 0) {
                finish();

                return;
            }

            curSrc = RECURSIVE_SOURCE;
            produced = 0;
            source().request(waiting);

            return;
        }

        if (produced == 0) {
            finish();

            return;
        }

        iteration++;

        if (iterationLimit >= 0 && iteration == iterationLimit) {
            finish();

            return;
        }

        produced = 0;
        source().rewind();
        source().request(waiting);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        assert idx >= 0 && idx < 2;

        return this;
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        curSrc = SEED_SOURCE;
        waiting = 0;
        produced = 0;
        iteration = 0;
        state.clear();
    }

    /** {@inheritDoc} */
    @Override protected void closeInternal() {
        state.clear();

        super.closeInternal();
    }

    /** */
    private Node<Row> source() {
        return sources().get(curSrc);
    }

    /** */
    private void finish() throws Exception {
        waiting = -1;
        state.clear();
        downstream().end();
    }
}
