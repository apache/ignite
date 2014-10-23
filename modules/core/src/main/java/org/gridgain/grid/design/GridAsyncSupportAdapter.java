// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.*;

/**
 * TODO: Add class description.
 */
public class GridAsyncSupportAdapter<T extends GridAsyncSupportAdapter> implements GridAsyncSupport<T>, Cloneable {
    /** Future holder. */
    protected final ThreadLocal<GridFuture<?>> curFut = new ThreadLocal<>();

    /** Async flag. */
    private boolean async;

    /**
     * Default constructor.
     */
    GridAsyncSupportAdapter() {
        // No-op.
    }

    /**
     * @param async Async enabled flag.
     */
    public GridAsyncSupportAdapter(boolean async) {
        this.async = async;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public T enableAsync() {
        try {
            GridAsyncSupportAdapter<T> clone = (GridAsyncSupportAdapter<T>)clone();

            clone.async = true;

            return (T)clone;
        }
        catch (CloneNotSupportedException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return async;
    }

    /** {@inheritDoc} */
    @Override public <R> GridFuture<R> future() {
        GridFuture<?> fut = curFut.get();

        if (fut == null)
            throw new IllegalStateException();

        curFut.set(null);

        return (GridFuture<R>)fut;
    }

    /**
     * @param fut Future.
     * @return If async mode is enabled returns {@code null}, otherwise returns future result.
     * @throws GridException If asynchronous mode is disabled and execution failed.
     */
    public <R> R result(GridFuture<R> fut) throws GridException {
        if (async) {
            curFut.set(fut);

            return null;
        }
        else
            return fut.get();
    }

    /** {@inheritDoc} */
    @Override public GridAsyncSupportAdapter clone() throws CloneNotSupportedException {
        return (GridAsyncSupportAdapter)super.clone();
    }
}
