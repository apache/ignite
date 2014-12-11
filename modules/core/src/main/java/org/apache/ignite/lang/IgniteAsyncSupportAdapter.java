// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.lang;

import org.apache.ignite.*;

/**
 * Adapter for {@link IgniteAsyncSupport}.
 */
public class IgniteAsyncSupportAdapter implements IgniteAsyncSupport {
    /** Future for previous asynchronous operation. */
    protected ThreadLocal<IgniteFuture<?>> curFut;

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
    @Override public IgniteAsyncSupport enableAsync() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return curFut != null;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        if (curFut == null)
            throw new IllegalStateException("Asynchronous mode is disabled.");

        IgniteFuture<?> fut = curFut.get();

        if (fut == null)
            throw new IllegalStateException("Asynchronous operation not started.");

        curFut.set(null);

        return (IgniteFuture<R>)fut;
    }

    /**
     * @param fut Future.
     * @return If async mode is enabled saves future and returns {@code null},
     *         otherwise waits for future and returns result.
     * @throws IgniteCheckedException If asynchronous mode is disabled and future failed.
     */
    public <R> R saveOrGet(IgniteFuture<R> fut) throws IgniteCheckedException {
        if (curFut != null) {
            curFut.set(fut);

            return null;
        }
        else
            return fut.get();
    }
}
