// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;

import java.io.*;
import java.util.*;

/**
 *
 * @param <R> Result type.
 */
class GridCacheErrorQueryFuture<R> extends GridFutureAdapter<Collection<R>> implements GridCacheQueryFuture<R> {
    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheErrorQueryFuture() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param th Error.
     */
    GridCacheErrorQueryFuture(GridKernalContext ctx, Throwable th) {
        super(ctx);

        onDone(th);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean available() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridIterator<R> iterator() {
        return new GridEmptyIterator<>();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public R next() {
        throw new NoSuchElementException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws GridException {
        return hasNext();
    }

    /** {@inheritDoc} */
    @Override public R nextX() throws GridException {
        return next();
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException("Remove is not supported.");
    }

    /** {@inheritDoc} */
    @Override public void removeX() throws GridException {
        throw new UnsupportedOperationException("Remove is not supported.");
    }
}
