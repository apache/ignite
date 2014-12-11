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
import java.util.concurrent.atomic.*;

/**
 * Adapter for closeable iterator that can be safely closed concurrently.
 */
public abstract class GridCloseableIteratorAdapterEx<T> extends GridIteratorAdapter<T>
    implements GridCloseableIterator<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override public final T nextX() throws IgniteCheckedException {
        if (closed.get())
            return null;

        try {
            if (!onHasNext())
                throw new NoSuchElementException();

            return onNext();
        }
        catch (IgniteCheckedException e) {
            if (closed.get())
                return null;
            else
                throw e;
        }
    }

    /**
     * @return Next element.
     * @throws IgniteCheckedException If failed.
     * @throws NoSuchElementException If no element found.
     */
    protected abstract T onNext() throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public final boolean hasNextX() throws IgniteCheckedException {
        if (closed.get())
            return false;

        try {
            return onHasNext();
        }
        catch (IgniteCheckedException e) {
            if (closed.get())
                return false;
            else
                throw e;
        }
    }

    /**
     * @return {@code True} if iterator has next element.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract boolean onHasNext() throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public final void removeX() throws IgniteCheckedException {
        if (closed.get())
            throw new NoSuchElementException("Iterator has been closed.");

        try {
            onRemove();
        }
        catch (IgniteCheckedException e) {
            if (!closed.get())
                throw e;
        }
    }

    /**
     * Called on remove from iterator.
     *
     * @throws IgniteCheckedException If failed.
     */
    protected void onRemove() throws IgniteCheckedException {
        throw new UnsupportedOperationException("Remove is not supported.");
    }

    /** {@inheritDoc} */
    @Override public final void close() throws IgniteCheckedException {
        if (closed.compareAndSet(false, true))
            onClose();
    }

    /**
     * Invoked on iterator close.
     *
     * @throws IgniteCheckedException If closing failed.
     */
    protected void onClose() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed.get();
    }
}
