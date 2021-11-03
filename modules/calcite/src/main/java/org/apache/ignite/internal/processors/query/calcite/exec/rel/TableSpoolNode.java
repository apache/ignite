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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;

/**
 * Table spool node.
 */
public class TableSpoolNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    /** How many rows are requested by downstream. */
    private int requested;

    /** How many rows are we waiting for from the upstream. {@code -1} means end of source stream. */
    private int waiting;

    /** Index of the current row to push. */
    private int rowIdx;

    /** Rows buffer. */
    private final List<RowT> rows;

    /**
     * If {@code true} this spool should emit rows as soon as it stored. If {@code false} the spool have to collect all rows from underlying
     * input.
     */
    private final boolean lazyRead;

    /**
     * Flag indicates that spool pushes row to downstream. Need to check a case when a downstream produces requests on push.
     */
    private boolean inLoop;

    /**
     * @param ctx Execution context.
     */
    public TableSpoolNode(ExecutionContext<RowT> ctx, RelDataType rowType, boolean lazyRead) {
        super(ctx, rowType);

        this.lazyRead = lazyRead;

        rows = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        rowIdx = 0;
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

        requested += rowsCnt;

        if ((waiting == -1 || rowIdx < rows.size()) && !inLoop) {
            context().execute(this::doPush, this::onError);
        } else if (waiting == 0) {
            source().request(waiting = inBufSize);
        }
    }

    /**
     *
     */
    private void doPush() throws Exception {
        if (isClosed()) {
            return;
        }

        if (!lazyRead && waiting != -1) {
            return;
        }

        int processed = 0;
        inLoop = true;
        try {
            while (requested > 0 && rowIdx < rows.size() && processed++ < inBufSize) {
                downstream().push(rows.get(rowIdx));

                rowIdx++;
                requested--;
            }
        } finally {
            inLoop = false;
        }

        if (rowIdx >= rows.size() && waiting == -1 && requested > 0) {
            requested = 0;
            downstream().end();
        } else if (requested > 0 && processed >= inBufSize) {
            context().execute(this::doPush, this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        rows.add(row);

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        }

        if (requested > 0 && rowIdx < rows.size()) {
            doPush();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        context().execute(this::doPush, this::onError);
    }
}
