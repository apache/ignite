package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

/**
 * Row tracker that does nothing.
 */
public class NoOpRowTracker<Row> implements RowTracker<Row> {
    /** */
    private static final RowTracker<?> INSTANCE = new NoOpRowTracker<>();

    /** */
    public static <Row> RowTracker<Row> instance() {
        return (RowTracker<Row>)INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public void onRowAdded(Row obj) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onRowRemoved(Row obj) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }
}
