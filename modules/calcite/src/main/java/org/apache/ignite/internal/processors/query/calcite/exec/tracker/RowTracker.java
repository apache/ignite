package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

/**
 * Object tracker interface.
 */
public interface RowTracker<Row> {
    /**
     * Add tracked row.
     */
    public void onRowAdded(Row row);

    /**
     * Remove tracked row.
     */
    public void onRowRemoved(Row row);

    /**
     * Clear information about tracked rows.
     */
    public void reset();
}
