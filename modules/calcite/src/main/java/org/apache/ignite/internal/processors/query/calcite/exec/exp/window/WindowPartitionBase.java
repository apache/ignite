package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.jetbrains.annotations.Nullable;

/** Base implementation of window partition */
abstract class WindowPartitionBase<Row> implements WindowPartition<Row> {
    private final Comparator<Row> peerCmp;
    private final Supplier<List<WindowFunctionWrapper<Row>>> accFactory;
    private final RowHandler.RowFactory<Row> accRowFactory;

    /**  */
    WindowPartitionBase(
        Comparator<Row> peerCmp,
        Supplier<List<WindowFunctionWrapper<Row>>> accFactory,
        RowHandler.RowFactory<Row> accRowFactory
    ) {
        this.peerCmp = peerCmp;
        this.accFactory = accFactory;
        this.accRowFactory = accRowFactory;
    }

    /** Creates {@link WindowFunctionWrapper} list */
    final List<WindowFunctionWrapper<Row>> createWrappers() {
        return accFactory.get();
    }

    /** Compares two rows and return true if current row peer not equal to the previous row peer. */
    protected final boolean isNewPeer(Row current, @Nullable Row previous) {
        if (previous == null)
            return true;
        else if (peerCmp != null)
            return peerCmp.compare(previous, current) != 0;
        else
            return false;
    }

    /** Creates row with window function results */
    protected final Row createResultRow(RowHandler.RowFactory<Row> rowFactory, Row source, Object... results) {
        Row resultsRow = accRowFactory.create(results);
        return rowFactory.handler().concat(source, resultsRow);
    }
}
