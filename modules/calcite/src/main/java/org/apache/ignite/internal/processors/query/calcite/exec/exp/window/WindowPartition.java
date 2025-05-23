package org.apache.ignite.internal.processors.query.calcite.exec.exp.window;

import java.util.Collection;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;

/** Partition of rows in window function calculation. */
public interface WindowPartition<Row> {

    /**
     * Adding row to the window partition.
     *
     * @return {@code true} in case {@link #drainTo(RowHandler.RowFactory, Collection)} should be invoked right after add.
     */
    boolean add(Row row);

    /** Drains partition to an output collection */
    void drainTo(RowHandler.RowFactory<Row> factory, Collection<Row> output);

    /** Reset current window partition (i.e., on partition restart) */
    void reset();
}
