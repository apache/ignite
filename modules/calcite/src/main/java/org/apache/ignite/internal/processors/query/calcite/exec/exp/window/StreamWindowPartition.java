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

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** Non-buffering implementation of the ROWS / RANGE window partition */
final class StreamWindowPartition<Row> extends WindowPartitionBase<Row> {
    private Row prevRow;
    private Row currRow;
    private int rowIdx = -1;
    private int peerIdx = -1;
    private List<WindowFunctionWrapper<Row>> accumulators;

    /**  */
    StreamWindowPartition(
        Comparator<Row> peerCmp,
        Supplier<List<WindowFunctionWrapper<Row>>> accFactory,
        RowHandler.RowFactory<Row> accRowFactory
    ) {
        super(peerCmp, accFactory, accRowFactory);
    }

    /** {@inheritDoc} */
    @Override public boolean add(Row row) {
        assert currRow == null : "StreamingWindowPartition can only hold one row";
        currRow = row;
        return true;
    }

    /** {@inheritDoc} */
    @Override public void drainTo(RowHandler.RowFactory<Row> factory, Collection<Row> output) {
        if (currRow == null) {
            return;
        }

        if (accumulators == null) {
            accumulators = createWrappers();
        }

        rowIdx++;
        if (isNewPeer(currRow, prevRow))
            peerIdx++;

        int accIdx = 0;
        Object[] accResults = new Object[accumulators.size()];
        for (WindowFunctionWrapper<Row> acc : accumulators) {
            Object accResult = acc.call(currRow, rowIdx, peerIdx);
            accResults[accIdx++] = accResult;
        }

        Row resultRow = createResultRow(factory, currRow, accResults);
        output.add(resultRow);

        prevRow = currRow;
        currRow = null;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        currRow = null;
        prevRow = null;
        rowIdx = -1;
        peerIdx = -1;
        accumulators = null;
    }
}
