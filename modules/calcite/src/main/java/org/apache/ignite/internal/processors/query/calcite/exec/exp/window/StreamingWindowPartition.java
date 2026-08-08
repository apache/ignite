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

import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** Non-buffering implementation of the ROWS / RANGE window partition. */
public final class StreamingWindowPartition<Row> extends WindowPartitionBase<Row> {
    /** */
    private Row prevRow;

    /** */
    private int rowIdx = -1;

    /** */
    private int peerIdx = -1;

    /** */
    private List<WindowFunctionWrapper<Row>> accumulators;

    /** */
    private Object[] accResults;

    /** */
    StreamingWindowPartition(
        Comparator<Row> peerCmp,
        WindowFunctionFactory<Row> funcFactory,
        RowHandler.RowFactory<Row> rowFactory
    ) {
        super(peerCmp, funcFactory, rowFactory);
    }

    /** Evaluates window functions for the given row. */
    public Row eval(Row row, RowHandler.RowFactory<Row> factory) {
        if (accumulators == null) {
            accumulators = createWrappers();
            accResults = new Object[accumulators.size()];
        }

        rowIdx++;
        if (isNewPeer(row, prevRow))
            peerIdx++;

        int accIdx = 0;
        for (WindowFunctionWrapper<Row> acc : accumulators) {
            Object accResult = acc.callStreaming(row, rowIdx, peerIdx);
            accResults[accIdx++] = accResult;
        }

        Row resultRow = createResultRow(factory, row, accResults);
        prevRow = row;
        return resultRow;
    }

    /** Returns true if any of the window functions is an accumulator stores incomig row. */
    public boolean hasAggAccumulators() {
        List<WindowFunctionWrapper<Row>> accumulators = this.accumulators;
        if (accumulators == null)
            accumulators = createWrappers();

        return accumulators.stream().anyMatch(WindowFunctionWrapper::isAggAccumulator);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        prevRow = null;
        rowIdx = -1;
        peerIdx = -1;
        accumulators = null;
    }
}
