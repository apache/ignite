/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.math.BigDecimal;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** Offset, fetch|limit support node. */
public class LimitNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** Offset if its present, otherwise 0. */
    private final BigDecimal offset;

    /** Fetch if its present, otherwise all rows. */
    private final @Nullable BigDecimal fetch;

    /** Already processed (pushed to upstream) rows count. */
    private BigDecimal rowsProcessed = BigDecimal.ZERO;

    /** Number of upstream rows that remain to be processed for the current downstream request. */
    private BigDecimal waiting = BigDecimal.ZERO;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowType Row type.
     */
    public LimitNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        @Nullable Supplier<BigDecimal> offsetNode,
        @Nullable Supplier<BigDecimal> fetchNode
    ) {
        super(ctx, rowType);

        offset = offsetNode == null ? BigDecimal.ZERO : offsetNode.get();
        fetch = fetchNode == null ? null : fetchNode.get();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 : rowsCnt;

        if (fetchNone()) {
            end();

            return;
        }

        waiting = BigDecimal.valueOf(rowsCnt);

        if (rowsProcessed.compareTo(offset) < 0)
            waiting = waiting.add(offset.subtract(rowsProcessed));

        requestNextBatch();
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        if (waiting.signum() < 0)
            return;

        rowsProcessed = rowsProcessed.add(BigDecimal.ONE);

        waiting = waiting.subtract(BigDecimal.ONE);

        checkState();

        boolean endAfterRow = fetchNone() && waiting.signum() > 0;
        boolean reqNextAfterRow = !endAfterRow && waiting.signum() > 0
            && rowsProcessed.remainder(BigDecimal.valueOf(IN_BUFFER_SIZE)).signum() == 0;

        if (rowsProcessed.compareTo(offset) > 0
            && (fetch == null || rowsProcessed.compareTo(offset.add(fetch)) <= 0)) {
            downstream().push(row);
        }

        if (endAfterRow)
            end();
        else if (reqNextAfterRow)
            requestNextBatch();
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        if (waiting.signum() < 0)
            return;

        assert downstream() != null;

        waiting = BigDecimal.ONE.negate();

        downstream().end();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        rowsProcessed = BigDecimal.ZERO;
        waiting = BigDecimal.ZERO;
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@code True} if requested 0 results, or all already processed. */
    private boolean fetchNone() {
        return fetch != null && (fetch.signum() == 0 || rowsProcessed.compareTo(offset.add(fetch)) >= 0);
    }

    /** Requests the next upstream batch, taking large decimal offsets into account. */
    private void requestNextBatch() throws Exception {
        BigDecimal bufSize = BigDecimal.valueOf(IN_BUFFER_SIZE);
        BigDecimal rowsAvailable = waiting;

        if (fetch != null)
            rowsAvailable = rowsAvailable.min(offset.add(fetch).subtract(rowsProcessed));

        BigDecimal rowsToReq = bufSize.subtract(rowsProcessed.remainder(bufSize)).min(rowsAvailable);

        if (rowsToReq.signum() == 0) {
            end();

            return;
        }

        checkState();

        source().request(rowsToReq.intValueExact());
    }
}
