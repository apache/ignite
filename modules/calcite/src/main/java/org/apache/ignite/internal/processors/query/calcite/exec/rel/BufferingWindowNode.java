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
import java.util.Comparator;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.BufferingWindowPartition;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.RowTracker;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.query.calcite.exec.rel.MemoryTrackingNode.DFLT_ROW_OVERHEAD;

/** Window node. */
public class BufferingWindowNode<Row> extends AbstractNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    private final Comparator<Row> partCmp;

    /** */
    private final BufferingWindowPartition<Row> part;

    /** */
    private final RowHandler.RowFactory<Row> rowFactory;

    /** */
    private List<Row> inBuf;

    /** */
    private RowTracker<Row> inBufTracker;

    /** */
    private int requested;

    /** */
    private int waiting;

    /** */
    private Row prevRow;

    /** */
    private boolean inLoop;

    /** */
    public BufferingWindowNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        Comparator<Row> partCmp,
        BufferingWindowPartition<Row> part,
        RowHandler.RowFactory<Row> rowFactory
    ) {
        super(ctx, rowType);
        this.partCmp = partCmp;
        this.part = part;
        this.rowFactory = rowFactory;
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (inLoop)
            return;

        if (part.ready() > 0)
            context().execute(this::flush, this::onError);
        else if (waiting == 0)
            source().request(waiting = IN_BUFFER_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        if (prevRow != null && partCmp != null && partCmp.compare(prevRow, row) != 0) {
            part.appendPartition(inBuf, inBufTracker::reset);
            inBuf = null;
            inBufTracker = null;
        }

        if (inBuf == null) {
            inBuf = new ArrayList<>(IN_BUFFER_SIZE);
            inBufTracker = context().createNodeMemoryTracker(DFLT_ROW_OVERHEAD);
        }

        inBuf.add(row);
        inBufTracker.onRowAdded(row);

        prevRow = row;

        if (inLoop)
            return;

        if (part.ready() > 0)
            flush();
        else if (waiting == 0)
            context().execute(() -> source().request(waiting = IN_BUFFER_SIZE), this::onError);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;
        if (waiting < 0)
            return;

        waiting = -1;

        checkState();

        // append partition for remaining rows if any.
        if (!F.isEmpty(inBuf)) {
            part.appendPartition(inBuf, inBufTracker::reset);
            inBuf = null;
            inBufTracker = null;
        }

        flush();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        prevRow = null;
        part.reset();
        if (inBuf != null) {
            inBufTracker.reset();
            inBuf = null;
            inBufTracker = null;
        }
    }

    /** {@inheritDoc} */
    @Override protected void closeInternal() {
        part.reset();
        if (inBuf != null)
            inBufTracker.reset();

        super.closeInternal();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }

    /** */
    private void flush() throws Exception {
        inLoop = true;
        try {
            int processed = 0;
            while (requested > 0) {
                Row result = part.nextRow(rowFactory);
                if (result == null)
                    break;

                requested--;

                downstream().push(result);

                processed++;

                if (processed == IN_BUFFER_SIZE && requested > 0) {
                    // Allow others to do their job.
                    context().execute(this::flush, this::onError);
                    return;
                }
            }
        }
        finally {
            inLoop = false;
        }

        if (waiting == 0 && requested > 0)
            context().execute(() -> source().request(waiting = IN_BUFFER_SIZE), this::onError);

        if (waiting < 0 && requested > 0 && part.ready() == 0) {
            requested = 0;
            downstream().end();
        }
    }
}
