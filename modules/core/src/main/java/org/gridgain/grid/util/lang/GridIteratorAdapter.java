/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.apache.ignite.*;

import java.util.*;

/**
 * Convenient adapter for "rich" iterator interface.
 */
public abstract class GridIteratorAdapter<T> implements GridIterator<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @SuppressWarnings("IteratorNextCanNotThrowNoSuchElementException")
    @Override public final T next() {
        try {
            return nextX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public final boolean hasNext() {
        try {
            return hasNextX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public final void remove() {
        try {
            removeX();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public final Iterator<T> iterator() {
        return this;
    }
}
