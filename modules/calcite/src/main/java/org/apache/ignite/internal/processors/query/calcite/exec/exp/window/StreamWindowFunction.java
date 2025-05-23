package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import org.jetbrains.annotations.Nullable;

/** Interface for a window function supporting streaming. */
interface StreamWindowFunction<Row> extends WindowFunction<Row> {

    /** */
    @Nullable Object call(Row row, int rowIdx, int peerIdx);

    /** {@inheritDoc} */
    @Override default Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame) {
        return call(row, rowIdx, peerIdx);
    }
}
