/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Sort node.
 */
public class SortNode extends AbstractNode<Object[]> implements SingleNode<Object[]>, Downstream<Object[]> {
    /** How many rows are requested by downstream. */
    private int requested;

    /** How many rows are we waiting for from the upstream. {@code -1} means end of stream. */
    private int waiting;

    /**  */
    private boolean inLoop;

    /** Rows comparator. */
    private final Comparator<Object[]> comparator;

    /** Rows buffer. */
    private final List<Object[]> rows = new ArrayList<>();

    /** Index of next row which buffer will return. {@code -1} means buffer is not sorted yet. */
    private int curIdx = -1;

    /**
     * @param ctx Execution context.
     * @param collation Sort collation.
     */
    public SortNode(ExecutionContext ctx, RelCollation collation) {
        super(ctx);
        this.comparator = Commons.comparator(collation);
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Object[]> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCount) {
        checkThread();

        assert !F.isEmpty(sources) && sources.size() == 1;
        assert rowsCount > 0 && requested == 0;

        requested = rowsCount;

        if (waiting == -1 && !inLoop)
            context().execute(this::flushFromBuffer);
        else if (waiting == 0)
            F.first(sources).request(waiting = IN_BUFFER_SIZE);
        else
            throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public void push(Object[] row) {
        checkThread();

        assert downstream != null;
        assert waiting > 0;

        waiting--;

        try {
            rows.add(row);

            if (waiting == 0)
                F.first(sources).request(waiting = IN_BUFFER_SIZE);
        }
        catch (Exception e) {
            downstream.onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void end() {
        checkThread();

        assert downstream != null;
        assert waiting > 0;

        waiting = -1;

        try {
            flushFromBuffer();
        }
        catch (Exception e) {
            downstream.onError(e);
        }
    }

    @Override public void onError(Throwable e) {
        checkThread();

        assert downstream != null;

        downstream.onError(e);
    }

    /** */
    private void flushFromBuffer() {
        assert waiting == -1;

        if (curIdx == -1 && comparator != null)
            rows.sort(comparator);

        inLoop = true;

        try {
            int processed = 0;

            while (requested > 0 && curIdx != rows.size()) {
                int toSend = Math.min(requested, IN_BUFFER_SIZE - processed);

                for (int i = 0; i < toSend; i++) {
                    requested--;
                    curIdx++;

                    if (curIdx == rows.size())
                        break;

                    Object[] row = rows.get(curIdx);
                    rows.set(curIdx, null);

                    downstream.push(row);

                    processed++;
                }

                if (processed >= IN_BUFFER_SIZE && requested > 0) {
                    // allow others to do their job
                    context().execute(this::flushFromBuffer);

                    return;
                }
            }

            if (requested >= 0) {
                downstream.end();
                requested = 0;
            }

        }
        finally {
            inLoop = false;
        }
    }
}
