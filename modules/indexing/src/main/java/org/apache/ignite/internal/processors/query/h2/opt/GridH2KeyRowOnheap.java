package org.apache.ignite.internal.processors.query.h2.opt;

import org.h2.value.Value;

/**
 * Heap-based key-only row for remove operations.
 */
public class GridH2KeyRowOnheap extends GridH2Row {
    /** */
    private Value key;

    /**
     * @param key Key.
     */
    public GridH2KeyRowOnheap(Value key) {
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int idx) {
        assert idx == 0 : idx;

        return key;
    }

    /** {@inheritDoc} */
    @Override public void setValue(int idx, Value v) {
        assert idx == 0 : idx;

        key = v;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return 0;
    }
}
