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

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.query.calcite.DistributedCalciteConfiguration.RECURSIVE_CTE_ITERATION_LIMIT_PROPERTY_NAME;

/** Coordinator-side executor for recursive union. */
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

    /** Number of rows still requested from the active source. */
    private int pending;

    /** Number of completed recursive iterations. */
    private int iteration;

    /** */
    public RepeatUnionNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        boolean all,
        int iterationLimit
    ) {
        super(ctx, rowType);

        state = new RecursiveCteState<>(ctx, all);
        this.iterationLimit = iterationLimit;
    }

    /** {@inheritDoc} */
    @Override public void register(List<Node<Row>> sources) {
        assert sources.size() == 2;

        bindRecursiveScans(sources.get(RECURSIVE_SOURCE));

        super.register(sources);
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && waiting == 0;

        checkState();

        waiting = rowsCnt;
        requestSource();
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0 && pending > 0 :
            "Received a row without outstanding demand [waiting=" + waiting + ", pending=" + pending + ']';

        checkState();

        pending--;

        if (state.add(row)) {
            waiting--;
            downstream().push(row);
        }

        if (pending == 0 && waiting > 0)
            context().execute(this::requestSource, this::onError);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        pending = 0;
        state.commit();

        if (state.isEmpty()) {
            finish();

            return;
        }

        if (curSrc == RECURSIVE_SOURCE)
            iteration++;

        if (iterationLimit >= 0 && iteration == iterationLimit)
            throw iterationLimitExceeded();

        if (curSrc == SEED_SOURCE)
            curSrc = RECURSIVE_SOURCE;
        else
            source().rewind();

        // Let the previous scan leave its push loop before requesting the next iteration.
        context().execute(this::requestSource, this::onError);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        assert idx >= 0 && idx < 2;

        return this;
    }

    /** Current delta visible to recursive scans owned by this union. */
    Iterable<Row> current() {
        return state.current();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        curSrc = SEED_SOURCE;
        waiting = 0;
        pending = 0;
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

    /** Binds recursive scans in this union's recursive term without crossing nested recursive unions. */
    private void bindRecursiveScans(Node<Row> node) {
        if (node instanceof RecursiveTableScanNode) {
            ((RecursiveTableScanNode<Row>)node).bind(this);

            return;
        }

        // Nested recursive unions bind their own scans when registering their sources.
        if (node instanceof RepeatUnionNode)
            return;

        if (!F.isEmpty(node.sources())) {
            for (Node<Row> src : node.sources())
                bindRecursiveScans(src);
        }
    }

    /** Requests the remaining downstream demand from the active input. */
    private void requestSource() throws Exception {
        checkState();

        assert pending == 0 : "Cannot request more rows while the source request is pending [waiting=" + waiting +
                ", pending=" + pending + ']';

        source().request(pending = waiting);
    }

    /** */
    private void finish() throws Exception {
        waiting = -1;
        state.clear();
        downstream().end();
    }

    /** */
    private IgniteSQLException iterationLimitExceeded() {
        return new IgniteSQLException(
            "Recursive CTE iteration limit exceeded [limit=" + iterationLimit +
                ", property=" + RECURSIVE_CTE_ITERATION_LIMIT_PROPERTY_NAME + ']',
            IgniteQueryErrorCode.QUERY_CANCELED
        );
    }
}
