/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl;

import org.gridgain.client.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Future adapter.
 */
public class GridClientFutureAdapter<R> implements GridClientFuture<R> {
    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientFutureAdapter.class.getName());

    /** This future done callbacks. */
    private final ConcurrentLinkedQueue<DoneCallback> cbs = new ConcurrentLinkedQueue<>();

    /** Done flag. */
    private final AtomicBoolean done = new AtomicBoolean(false);

    /** Latch. */
    private final CountDownLatch doneLatch = new CountDownLatch(1);

    /** Result. */
    private R res;

    /** Error. */
    private Throwable err;

    /**
     * Creates not-finished future without any result.
     */
    public GridClientFutureAdapter() {
        // No-op.
    }

    /**
     * Creates succeeded finished future with given result.
     *
     * @param res Future result.
     */
    public GridClientFutureAdapter(R res) {
        this.res = res;

        done.set(true);
        doneLatch.countDown();
    }

    /**
     * Creates failed finished future with given error.
     *
     * @param err Future error.
     */
    public GridClientFutureAdapter(Throwable err) {
        this.err = err;

        done.set(true);
        doneLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public R get() throws GridClientException {
        try {
            if (doneLatch.getCount() > 0)
                doneLatch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridClientException("Operation was interrupted.", e);
        }

        return getResult();
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws GridClientException {
        A.ensure(timeout >= 0, "timeout >= 0");

        try {
            if (doneLatch.getCount() > 0 && !doneLatch.await(timeout, unit))
                throw new GridClientFutureTimeoutException("Failed to get future result due to waiting timed out.");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridClientException("Operation was interrupted.", e);
        }

        return getResult();
    }

    /**
     * Get future result.
     *
     * @return Future result.
     * @throws GridClientException In case of error.
     */
    private R getResult() throws GridClientException {
        assert doneLatch.getCount() == 0;

        if (err == null)
            return res;

        if (err instanceof Error)
            throw (Error)err;

        if (err instanceof GridClientException)
            throw (GridClientException)err;

        throw new GridClientException(err);
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return done.get();
    }

    /**
     * Callback to notify that future is finished successfully.
     *
     * @param res Result (can be {@code null}).
     */
    public void onDone(@Nullable R res) {
        if (done.compareAndSet(false, true)) {
            this.res = res;

            doneLatch.countDown();

            fireDone();
        }
    }

    /**
     * Callback to notify that future is finished with error.
     *
     * @param err Error (can't be {@code null}).
     */
    public void onDone(Throwable err) {
        assert err != null;

        if (done.compareAndSet(false, true)) {
            this.err = err;

            doneLatch.countDown();

            fireDone();
        }
    }

    /**
     * Register new listeners for notification when future completes.
     *
     * Note that current implementations are calling listeners in
     * the completing thread.
     *
     * @param lsnrs Listeners to be registered.
     */
    @Override public void listenAsync(final GridClientFutureListener<R>... lsnrs) {
        assert lsnrs != null;

        for (GridClientFutureListener<R> lsnr : lsnrs)
            cbs.add(new DoneCallback<R>(null, lsnr, null));

        if (isDone())
            fireDone();
    }

    /**
     * Removes listeners registered before.
     *
     * @param lsnrs Listeners to be removed.
     */
    @Override public void stopListenAsync(GridClientFutureListener<R>... lsnrs) {
        Collection<GridClientFutureListener<R>> lsnrsCol = lsnrs == null ? null : Arrays.asList(lsnrs);

        for (Iterator<DoneCallback> it = cbs.iterator(); it.hasNext();) {
            DoneCallback cb = it.next();

            if (cb.lsnr == null)
                continue;

            // Remove all listeners, if passed listeners collection is 'null'.
            if (lsnrsCol == null || lsnrsCol.contains(cb.lsnr))
                it.remove();
        }
    }

    /**
     * Creates future's chain and completes chained future, when this future finishes.
     *
     * @param cb Future callback to convert this future result into expected format.
     * @param <T> New future format to convert this finished future to.
     * @return Chained future with new format.
     */
    public <T> GridClientFutureAdapter<T> chain(GridClientFutureCallback<R, T> cb) {
        GridClientFutureAdapter<T> fut = new GridClientFutureAdapter<>();

        cbs.add(new DoneCallback<>(cb, null, fut));

        if (isDone())
            fireDone();

        return fut;
    }

    /**
     * Fire event this future has been finished.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private void fireDone() {
        assert isDone();

        DoneCallback cb;

        Error err = null;

        while ((cb = cbs.poll()) != null)
            try {
                cb.proceed();
            }
            catch (Error e) {
                if (err == null)
                    err = e;
                else
                    log.log(Level.WARNING, "Failed to notify future callback due to unhandled error.", e);
            }

        if (err != null)
            throw err;
    }

    /** This future finished notification callback. */
    private class DoneCallback<T> {
        /** Done future callback. */
        private final GridClientFutureCallback<R, T> cb;

        /** Done future listener. */
        private final GridClientFutureListener<R> lsnr;

        /** Chained future. */
        private final GridClientFutureAdapter<T> chainedFut;

        /**
         * Constructs future finished notification callback.
         *
         * @param cb Future finished callback.
         * @param chainedFut Chained future to set callback result to.
         */
        private DoneCallback(GridClientFutureCallback<R, T> cb, GridClientFutureListener<R> lsnr,
            GridClientFutureAdapter<T> chainedFut) {
            this.cb = cb;
            this.lsnr = lsnr;
            this.chainedFut = chainedFut;
        }

        /**
         * Proceed this future callback.
         */
        public void proceed() {
            GridClientFutureAdapter<R> fut = GridClientFutureAdapter.this;

            assert fut.isDone();

            try {
                if (lsnr != null)
                    lsnr.onDone(fut);

                T res = null;

                if (cb != null)
                    res = cb.onComplete(fut);

                if (chainedFut != null)
                    chainedFut.onDone(res);
            }
            catch (GridClientException e) {
                if (chainedFut != null)
                    chainedFut.onDone(e);

                if (log.isLoggable(Level.FINE)) {
                    log.log(Level.FINE, "Failed to notify chained callback due to unhandled client exception" +
                        " [fut=" + fut + ", cb=" + cb + ", chainedFut=" + chainedFut + ']', e);
                }
            }
            catch (RuntimeException e) {
                if (chainedFut != null)
                    chainedFut.onDone(e);

                log.log(Level.WARNING, "Failed to notify chained callback due to unhandled runtime exception" +
                    " [fut=" + fut + ", cb=" + cb + ", chainedFut=" + chainedFut + ']', e);
            }
            catch (Error e) {
                if (chainedFut != null)
                    chainedFut.onDone(e);

                throw e;
            }
        }
    }
}
