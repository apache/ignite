package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Collections;
import java.util.List;

/** Rows frame for window function. */
abstract class WindowFunctionFrame<Row> {
    /** Holds immutable refrence to buffered window partition rows */
    protected final List<Row> buffer;

    /**  */
    WindowFunctionFrame(List<Row> buffer) {
        this.buffer = Collections.unmodifiableList(buffer);
    }

    /** Returns row from partition by index. */
    Row get(int idx) {
        assert idx >= 0 && idx < buffer.size() : "Invalid row index";
        return buffer.get(idx);
    }

    /** Returns start frame index in partition for current row peer. */
    abstract int getFrameStart(Row row, int rowIdx, int peerIdx);

    /** Returns end frame index in partition for current row peer. */
    abstract int getFrameEnd(Row row, int rowIdx, int peerIdx);

    /** Return number of peers in current frame */
    abstract int countPeers();

    /** Returns frame size in partition for the current row peer. */
    final int size(int rowIdx, int peerIdx) {
        Row row = get(rowIdx);
        int start = getFrameStart(row, rowIdx, peerIdx);
        int end = getFrameEnd(row, rowIdx, peerIdx);
        if (end >= start)
            return end - start + 1;
        else
            return 0;
    }

    /** Returns row count in partition. */
    final int partitionSize() {
        return buffer.size();
    }

    /** Resets current frame. */
    protected abstract void reset();
}
