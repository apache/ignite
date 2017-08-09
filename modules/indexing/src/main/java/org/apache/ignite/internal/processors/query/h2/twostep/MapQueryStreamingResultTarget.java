package org.apache.ignite.internal.processors.query.h2.twostep;

import org.h2.result.ResultTarget;
import org.h2.value.Value;

/**
 * Map query streaming result target.
 */
public class MapQueryStreamingResultTarget implements ResultTarget {
    /** Row counter. */
    private int rowCnt;

    /** {@inheritDoc} */
    @Override public void addRow(Value[] vals) {
        for (Value val : vals) {
            // TODO: Implement.
        }
    }

    /** {@inheritDoc} */
    @Override public int getRowCount() {
        return rowCnt;
    }
}
