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
public class GridAsyncSupportAdapter<T extends GridAsyncSupport> implements GridAsyncSupport<T>, Cloneable {
    /** Future holder. */
    protected ThreadLocal<GridFuture<?>> curFut;

    /**
     * Default constructor.
     */
    public GridAsyncSupportAdapter() {
        // No-op.
    }

    /**
     * @param async Async enabled flag.
     */
    public GridAsyncSupportAdapter(boolean async) {
        if (async)
            curFut = new ThreadLocal<>();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public T enableAsync() {
        try {
            GridAsyncSupportAdapter<T> clone = (GridAsyncSupportAdapter<T>)clone();

            clone.curFut = new ThreadLocal<>();

            return (T)clone;
        }
        catch (CloneNotSupportedException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return curFut != null;
    }

    /** {@inheritDoc} */
    @Override public <R> GridFuture<R> future() {
        if (curFut == null)
            throw new IllegalStateException();

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
        if (curFut != null) {
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
