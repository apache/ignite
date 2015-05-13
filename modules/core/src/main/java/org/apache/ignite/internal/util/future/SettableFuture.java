/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.util.future;

import java.util.concurrent.*;

/**
 * Simple implementation of {@link Future}
 */
public class SettableFuture<T> implements Future<T> {
    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /** Result of computation. */
    private T res;

    /** Exception threw during the computation. */
    private ExecutionException err;

    /** {@inheritDoc} */
    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return latch.getCount() == 0;
    }

    /** {@inheritDoc} */
    @Override public T get() throws InterruptedException, ExecutionException {
        latch.await();

        if (err != null)
            throw err;

        return res;
    }

    /** {@inheritDoc} */
    @Override public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
        TimeoutException {

        if (!latch.await(timeout, unit))
            throw new TimeoutException();

        if (err != null)
            throw err;

        return res;
    }

    /**
     * Computation is done successful.
     *
     * @param res Result of computation.
     */
    public void set(T res) {
        this.res = res;

        latch.countDown();
    }

    /**
     * Computation failed.
     *
     * @param throwable Error.
     */
    public void setException(Throwable throwable) {
        err = new ExecutionException(throwable);

        latch.countDown();
    }
}
