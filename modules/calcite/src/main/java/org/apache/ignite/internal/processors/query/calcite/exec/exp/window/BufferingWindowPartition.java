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

package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** Buffering implementation of the ROWS / RANGE window partition. */
final class BufferingWindowPartition<Row> extends WindowPartitionBase<Row> {
    /** Rows in partition. */
    private final List<Row> buf;

    /** Frame within partition. */
    private final WindowFunctionFrame<Row> frame;

    /** */
    BufferingWindowPartition(
        Comparator<Row> peerCmp,
        WindowFunctionFactory<Row> funcFactory,
        RowHandler.RowFactory<Row> rowFactory,
        ExecutionContext<Row> ctx,
        Window.Group grp,
        RelDataType inputRowType
    ) {
        super(peerCmp, funcFactory, rowFactory);
        buf = new ArrayList<>();
        frame = createFrame(ctx, grp, peerCmp, inputRowType, buf);
    }

    /** {@inheritDoc} */
    @Override public void add(Row row) {
        buf.add(row);
        onRowAdded(row);
    }

    /** {@inheritDoc} */
    @Override public void evalTo(RowHandler.RowFactory<Row> factory, Consumer<Row> output) {
        if (buf.isEmpty())
            return;

        List<WindowFunctionWrapper<Row>> accumulators = createWrappers();
        Object[] accResults = new Object[accumulators.size()];

        int size = buf.size();
        Row prevRow = null;
        int peerIdx = -1;
        for (int rowIdx = 0; rowIdx < size; rowIdx++) {
            Row currRow = buf.get(rowIdx);
            if (isNewPeer(currRow, prevRow))
                peerIdx++;

            int accIdx = 0;
            for (WindowFunctionWrapper<Row> acc : accumulators) {
                Object accResult = acc.callBuffering(currRow, rowIdx, peerIdx, frame);
                accResults[accIdx++] = accResult;
            }

            Row resultRow = createResultRow(factory, currRow, accResults);
            output.accept(resultRow);

            prevRow = currRow;
        }
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        buf.forEach(this::onRowRemoved);
        buf.clear();
        frame.reset();
    }

    /** {@inheritDoc} */
    @Override public boolean isStreaming() {
        return false;
    }

    /** Creates frame for partition. */
    private static <Row> WindowFunctionFrame<Row> createFrame(
        ExecutionContext<Row> ctx,
        Window.Group grp,
        Comparator<Row> peerCmp,
        RelDataType inputRowType,
        List<Row> buf
    ) {
        if (grp.isRows)
            return new RowWindowPartitionFrame<>(buf, ctx, grp, inputRowType);
        else
            return new RangeWindowPartitionFrame<>(buf, ctx, peerCmp, grp, inputRowType);
    }
}
