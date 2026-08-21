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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.StreamingWindowPartition;
import org.apache.ignite.internal.util.typedef.F;

/** Non-buffering implementation of Window node. */
public class StreamingWindowNode<Row> extends MemoryTrackingNode<Row> implements SingleNode<Row>, Downstream<Row> {
    /** */
    private final Comparator<Row> partCmp;

    /** */
    private final RowHandler.RowFactory<Row> rowFactory;

    /** */
    private final StreamingWindowPartition<Row> part;

    /** */
    private final boolean hasAggAccum;

    /** */
    private Row prevRow;

    /** */
    public StreamingWindowNode(
        ExecutionContext<Row> ctx,
        RelDataType rowType,
        Comparator<Row> partCmp,
        StreamingWindowPartition<Row> part,
        RowHandler.RowFactory<Row> rowFactory
    ) {
        super(ctx, rowType, DFLT_ROW_OVERHEAD);
        this.partCmp = partCmp;
        this.part = part;
        this.rowFactory = rowFactory;
        hasAggAccum = part.hasAggAccumulators();
    }

    /** {@inheritDoc} */
    @Override public void request(int rowsCnt) throws Exception {
        assert !F.isEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0;

        checkState();

        source().request(rowsCnt);
    }

    /** {@inheritDoc} */
    @Override public void push(Row row) throws Exception {
        assert downstream() != null;

        checkState();

        if (prevRow != null && partCmp != null && partCmp.compare(prevRow, row) != 0) {
            part.reset();
            nodeMemoryTracker.reset();
        }

        Row result = part.eval(row, rowFactory);

        if (hasAggAccum)
            nodeMemoryTracker.onRowAdded(row);

        prevRow = row;

        downstream().push(result);
    }

    /** {@inheritDoc} */
    @Override public void end() throws Exception {
        assert downstream() != null;

        checkState();

        downstream().end();
    }

    /** {@inheritDoc} */
    @Override protected void rewindInternal() {
        prevRow = null;
        part.reset();
        nodeMemoryTracker.reset();
    }

    /** {@inheritDoc} */
    @Override protected Downstream<Row> requestDownstream(int idx) {
        if (idx != 0)
            throw new IndexOutOfBoundsException();

        return this;
    }
}
