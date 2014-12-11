/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.apache.ignite.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;

/**
 * Convenient adapter for closeable iterator.
 */
public abstract class GridCloseableIteratorAdapter<T> extends GridIteratorAdapter<T> implements
    GridCloseableIterator<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Closed flag. */
    private boolean closed;

    /** {@inheritDoc} */
    @Override public final T nextX() throws IgniteCheckedException {
        if (!hasNextX())
            throw new NoSuchElementException();

        return onNext();
    }

    /**
     * @return Next element.
     * @throws IgniteCheckedException If failed.
     * @throws NoSuchElementException If no element found.
     */
    protected abstract T onNext() throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public final boolean hasNextX() throws IgniteCheckedException {
        return !closed && onHasNext();
    }

    /**
     * @return {@code True} if iterator has next element.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract boolean onHasNext() throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public final void removeX() throws IgniteCheckedException {
        checkClosed();

        onRemove();
    }

    /**
     * Called on remove from iterator.
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void onRemove() throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public final void close() throws IgniteCheckedException {
        if (!closed) {
            onClose();

            closed = true;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed;
    }

    /**
     * Invoked on iterator close.
     *
     * @throws IgniteCheckedException If closing failed.
     */
    protected void onClose() throws IgniteCheckedException {
        // No-op.
    }

    /**
     * Throws {@link NoSuchElementException} if iterator has been closed.
     *
     * @throws NoSuchElementException If iterator has already been closed.
     */
    protected final void checkClosed() throws NoSuchElementException {
        if (closed)
            throw new NoSuchElementException("Iterator has been closed.");
    }
}
