package org.gridgain.grid.util;

import org.gridgain.grid.*;
import org.gridgain.grid.util.lang.*;

import java.lang.ref.*;
import java.util.*;

/**
 * Weak iterator.
 */
public class GridWeakIterator<T> extends WeakReference<Iterator<T>> {
    /** Nested closeable iterator. */
    private final GridCloseableIterator<T> it;

    /**
     * @param ref Referent.
     * @param it Closeable iterator.
     * @param q Referent queue.
     */
    public GridWeakIterator(Iterator<T> ref, GridCloseableIterator<T> it,
        ReferenceQueue<Iterator<T>> q) {
        super(ref, q);

        assert it != null;

        this.it = it;
    }

    /**
     * Closes iterator.
     *
     * @throws GridException If failed.
     */
    public void close() throws GridException {
        it.close();
    }
}