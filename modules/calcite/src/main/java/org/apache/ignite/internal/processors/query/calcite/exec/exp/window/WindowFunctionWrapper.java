package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import org.jetbrains.annotations.Nullable;

/** interface for window function wrapper. */
interface WindowFunctionWrapper<Row> {
    /**  */
    @Nullable Object call(Row row, int rowIdx, int peerIdx);

    /**  */
    @Nullable Object call(Row row, int rowIdx, int peerIdx, WindowFunctionFrame<Row> frame);
}
