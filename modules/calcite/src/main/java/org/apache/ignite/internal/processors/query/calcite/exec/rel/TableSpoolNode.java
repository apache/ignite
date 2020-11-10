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
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Spool node.
 */
public class TableSpoolNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** How many rows are requested by downstream. */
    private int requested;

    /** How many rows are we waiting for from the upstream. {@code -1} means end of source stream. */
    private int waiting;

    /** Index of the current row to push. */
    private int rowIdx;

    /** Rows buffer. */
    private final List<Row> rows;

    /**
     * @param ctx Execution context.
     */
    public TableSpoolNode(ExecutionContext<Row> ctx, RelDataType rowType) {
        super(ctx, rowType);

        rows = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        rowIdx = 0;
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

            requested += rowsCnt;

            if (waiting == -1)
                context().execute(this::doPush);
            else if (waiting == 0)
                source().request(waiting = IN_BUFFER_SIZE);
        }
        catch (Exception e) {
            onError(e);
        }
    }

    /** */
    private void doPush() {
        while (requested > 0 && rowIdx < rows.size())
            pushToDownstream();
    }

    /** */
    private void pushToDownstream() {
        downstream().push(rows.get(rowIdx));

        rowIdx++;
        requested--;

        if (rowIdx >= rows.size() && waiting == -1 && requested > 0)
            downstream().end();
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

            if (requested > 0 && rowIdx < rows.size())
                pushToDownstream();
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

            if (rowIdx >= rows.size() && waiting == -1)
                downstream().end();
        }
        catch (Exception e) {
            downstream().onError(e);
        }
    }
}
