// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.lang;

import org.gridgain.grid.*;

/**
 * Adapter for {@link IgniteAsyncSupport}.
 */
public class IgniteAsyncSupportAdapter implements IgniteAsyncSupport, Cloneable {
    /** Future for previous asynchronous operation. */
    protected ThreadLocal<GridFuture<?>> curFut;

    /**
     * Default constructor.
     */
    public IgniteAsyncSupportAdapter() {
        // No-op.
    }

    /**
     * @param async Async enabled flag.
     */
    public IgniteAsyncSupportAdapter(boolean async) {
        if (async)
            curFut = new ThreadLocal<>();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteAsyncSupport enableAsync() {
        try {
            if (isAsync())
                return this;

            IgniteAsyncSupportAdapter clone = clone();

            clone.curFut = new ThreadLocal<>();

            return clone;
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
            throw new IllegalStateException("Asynchronous mode is disabled.");

        GridFuture<?> fut = curFut.get();

        if (fut == null)
            throw new IllegalStateException("Asynchronous operation not started.");

        curFut.set(null);

        return (GridFuture<R>)fut;
    }

    /**
     * @param fut Future.
     * @return If async mode is enabled saves future and returns {@code null},
     *         otherwise waits for future and returns result.
     * @throws GridException If asynchronous mode is disabled and future failed.
     */
    public <R> R saveOrGet(GridFuture<R> fut) throws GridException {
        if (curFut != null) {
            curFut.set(fut);

            return null;
        }
        else
            return fut.get();
    }

    /** {@inheritDoc} */
    @Override public IgniteAsyncSupportAdapter clone() throws CloneNotSupportedException {
        return (IgniteAsyncSupportAdapter)super.clone();
    }
}
