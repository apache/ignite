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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** Buffering implementation of the ROWS / RANGE window partition */
final class BufWindowPartition<Row> extends WindowPartitionBase<Row> {
    private final List<Row> buffer;
    // frame within partition.
    private final WindowFunctionFrame<Row> frame;

    /**  */
    BufWindowPartition(
        Comparator<Row> peerCmp,
        Supplier<List<WindowFunctionWrapper<Row>>> accFactory,
        RowHandler.RowFactory<Row> accRowFactory,
        ExecutionContext<Row> ctx,
        Window.Group group,
        RelDataType inputRowType
    ) {
        super(peerCmp, accFactory, accRowFactory);
        buffer = new ArrayList<>();
        frame = createFrame(ctx, peerCmp, group, inputRowType, buffer);
    }

    /** {@inheritDoc} */
    @Override public boolean add(Row row) {
        buffer.add(row);
        return false;
    }

    /** {@inheritDoc} */
    @Override public void drainTo(RowHandler.RowFactory<Row> factory, Collection<Row> output) {
        if (buffer.isEmpty()) {
            return;
        }

        List<WindowFunctionWrapper<Row>> accumulators = createWrappers();

        int size = buffer.size();
        Row prevRow = null;
        int peerIdx = -1;
        for (int rowIdx = 0; rowIdx < size; rowIdx++) {
            Row currRow = buffer.get(rowIdx);
            if (isNewPeer(currRow, prevRow)) {
                peerIdx++;
            }

            int accIdx = 0;
            Object[] accResults = new Object[accumulators.size()];
            for (WindowFunctionWrapper<Row> acc : accumulators) {
                Object accResult = acc.call(currRow, rowIdx, peerIdx, frame);
                accResults[accIdx++] = accResult;
            }

            Row resultRow = createResultRow(factory, currRow, accResults);
            output.add(resultRow);

            prevRow = currRow;
        }
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        buffer.clear();
        frame.reset();
    }

    /** Creates frame for partition */
    private static <Row> WindowFunctionFrame<Row> createFrame(
        ExecutionContext<Row> ctx,
        Comparator<Row> peerCmp,
        Window.Group group,
        RelDataType inputRowType,
        List<Row> buffer
    ) {
        if (group.isRows)
            return new RowWindowPartitionFrame<>(buffer, ctx, group, inputRowType);
        else
            return new RangeWindowPartitionFrame<>(buffer, ctx, peerCmp, group, inputRowType);
    }
}
