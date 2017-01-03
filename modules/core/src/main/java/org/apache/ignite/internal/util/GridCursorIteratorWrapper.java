package org.apache.ignite.internal.util;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.lang.*;

import java.util.*;

/**
 * Wrap {@code Iterator} and adapt it to {@code GridCursor}.
 */
public class GridCursorIteratorWrapper<V> implements GridCursor<V> {
    /** Iterator. */
    private Iterator<V> iter;

    /** Next. */
    private V next;

    /**
     * @param iter Iterator.
     */
    public GridCursorIteratorWrapper(Iterator<V> iter) {
        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override public V get() throws IgniteCheckedException {
        return next;
    }

    /** {@inheritDoc} */
    @Override public boolean next() throws IgniteCheckedException {
        next = iter.hasNext() ? iter.next() : null;

        return next != null;
    }
}
