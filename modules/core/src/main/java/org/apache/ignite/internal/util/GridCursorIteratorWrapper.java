package org.apache.ignite.internal.util;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.lang.*;

import java.util.*;

/**
 * Wrap {@code Iterator} and adapt it to {@code GridCursor}.
 */
public class GridCursorIteratorWrapper<V> implements GridCursor<V> {
    private Iterator<V> iter;
    private V next;

    public GridCursorIteratorWrapper(Iterator<V> iter) {
        this.iter = iter;
    }

    @Override public V get() throws IgniteCheckedException {
        return next;
    }

    @Override public boolean next() throws IgniteCheckedException {
        next = iter.hasNext() ? iter.next() : null;

        return next != null;
    }
}
