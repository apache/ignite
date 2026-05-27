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

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.jetbrains.annotations.Nullable;

/** Buffering implementation of the ROWS / RANGE window partition. */
public final class BufferingWindowPartition<Row> extends WindowPartitionBase<Row> {
    /** */
    private final ExecutionContext<Row> ctx;

    /** */
    private final Window.Group grp;

    /** */
    private final RelDataType inputRowType;

    /** Slices in partition. */
    private final Deque<FrameHolder> frames;

    /** Number of rows, can be evaluted right now. */
    private int ready;

    /** Current frame for row evaluation. */
    private FrameHolder currFrame;

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
        this.ctx = ctx;
        this.grp = grp;
        this.inputRowType = inputRowType;
        frames = new ArrayDeque<>();
    }

    /**
     * Appends rows to the partition.
     * Important: the buffer must not be modified after being passed to this method.
     **/
    public void appendPartition(List<Row> buf, Runnable onBufRemoved) {
        if (buf.isEmpty())
            return;

        WindowPartitionFrame<Row> frame = createFrame(ctx, grp, peerCmp, inputRowType, buf);
        ready += frame.size();
        frames.add(new FrameHolder(frame, onBufRemoved));
    }

    /**
     * Evaluates next row in the window partition.
     * @return Result row or {@code null} if there are no more rows.
     */
    public @Nullable Row nextRow(RowHandler.RowFactory<Row> factory) {
        if (currFrame == null || currFrame.consumed()) {
            if (currFrame != null)
                currFrame.release();
            currFrame = frames.pollFirst();
        }

        if (currFrame == null) {
            assert ready == 0;
            return null;
        }

        assert ready > 0;
        Row resultRow = currFrame.nextRow(factory);
        ready--;
        return resultRow;
    }

    /** Returns the number of rows can be evaluted. */
    public int ready() {
        return ready;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        FrameHolder holder;
        while ((holder = frames.poll()) != null)
            holder.release();
        ready = 0;
    }

    /** Creates frame for partition. */
    private static <Row> WindowPartitionFrame<Row> createFrame(
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

    /** */
    private final class FrameHolder {
        /** */
        private final WindowPartitionFrame<Row> frame;

        /** */
        private final Runnable onFrameRemoved;

        /** Index of the current row in the frame. */
        private int rowIdx;

        /** Index of the current peer in the frame. */
        private int peerIdx = -1;

        /** */
        private Row prevRow;

        /** */
        private List<WindowFunctionWrapper<Row>> accumulators;

        /** */
        private Object[] accResults;

        /** */
        private FrameHolder(WindowPartitionFrame<Row> frame, Runnable removed) {
            this.frame = frame;
            onFrameRemoved = removed;
        }

        /** */
        Row nextRow(RowHandler.RowFactory<Row> factory) {
            assert !consumed();

            if (accumulators == null) {
                accumulators = createWrappers();
                accResults = new Object[accumulators.size()];
            }

            Row currRow = frame.get(rowIdx);
            if (isNewPeer(currRow, prevRow))
                peerIdx++;

            int accIdx = 0;
            for (WindowFunctionWrapper<Row> acc : accumulators) {
                Object accResult = acc.callBuffering(currRow, rowIdx, peerIdx, currFrame.frame);
                accResults[accIdx++] = accResult;
            }

            Row resultRow = createResultRow(factory, currRow, accResults);

            rowIdx++;
            prevRow = currRow;

            return resultRow;
        }

        /** */
        boolean consumed() {
            return rowIdx == frame.size();
        }

        /** */
        void release() {
            onFrameRemoved.run();
        }
    }
}
