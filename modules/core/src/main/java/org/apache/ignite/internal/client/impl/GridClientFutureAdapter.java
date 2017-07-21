/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.impl;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFuture;
import org.apache.ignite.internal.client.GridClientFutureListener;
import org.apache.ignite.internal.client.GridClientFutureTimeoutException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Future adapter.
 */
public class GridClientFutureAdapter<R> extends AbstractQueuedSynchronizer implements GridClientFuture<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Initial state. */
    private static final int INIT = 0;

    /** Done state. */
    private static final int DONE = 1;

    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientFutureAdapter.class.getName());

    /** This future done callbacks. */
    private final ConcurrentLinkedQueue<DoneCallback> cbs = new ConcurrentLinkedQueue<>();

    /** Result. */
    private R res;

    /** Error. */
    private Throwable err;

    /** */
    private volatile boolean done;

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
        onDone(res, null);
    }

    /**
     * Creates failed finished future with given error.
     *
     * @param err Future error.
     */
    public GridClientFutureAdapter(Throwable err) {
        onDone(null, err);
    }

    /** {@inheritDoc} */
    @Override public R get() throws GridClientException {
        try {
            if (!done)
                acquireSharedInterruptibly(0);

            return getResult();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridClientException("Operation was interrupted.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws GridClientException {
        A.ensure(timeout >= 0, "timeout >= 0");

        try {
            if (!done && !tryAcquireSharedNanos(0, unit.toNanos(timeout)))
                throw new GridClientFutureTimeoutException("Failed to get future result due to waiting timed out.");
        }
        catch (InterruptedException e) {
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
        assert getState() == DONE;

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
        return getState() != INIT;
    }

    /**
     * Callback to notify that future is finished successfully.
     *
     * @param res Result (can be {@code null}).
     */
    public void onDone(@Nullable R res) {
        onDone(res, null);
    }

    /**
     * Callback to notify that future is finished with error.
     *
     * @param err Error (can't be {@code null}).
     */
    public void onDone(Throwable err) {
        assert err != null;

        onDone(null, err);
    }

    /**
     * @param res Result.
     * @param err Error.
     * @return {@code True} if result was set by this call.
     */
    private boolean onDone(@Nullable R res, @Nullable Throwable err) {
        boolean notify = false;

        try {
            if (compareAndSetState(INIT, DONE)) {
                this.res = res;
                this.err = err;

                notify = true;

                releaseShared(0);

                return true;
            }

            return false;
        }
        finally {
            if (notify)
                fireDone();
        }
    }

    /** {@inheritDoc} */
    @Override protected final int tryAcquireShared(int ignore) {
        return done ? 1 : -1;
    }

    /** {@inheritDoc} */
    @Override protected final boolean tryReleaseShared(int ignore) {
        done = true;

        // Always signal after setting final done status.
        return true;
    }

    /**
     * Register new listeners for notification when future completes.
     *
     * Note that current implementations are calling listeners in
     * the completing thread.
     *
     * @param lsnrs Listeners to be registered.
     */
    @Override public void listen(final GridClientFutureListener<R>... lsnrs) {
        assert lsnrs != null;

        for (GridClientFutureListener<R> lsnr : lsnrs)
            cbs.add(new DoneCallback<R>(null, lsnr, null));

        if (isDone())
            fireDone();
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