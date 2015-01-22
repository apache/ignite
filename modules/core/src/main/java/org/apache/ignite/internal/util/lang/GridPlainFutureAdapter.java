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

package org.apache.ignite.internal.util.lang;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Plain future adapter.
 */
public class GridPlainFutureAdapter<R> implements GridPlainFuture<R> {
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
    public GridPlainFutureAdapter() {
        // No-op.
    }

    /**
     * Creates succeeded finished future with given result.
     *
     * @param res Future result.
     */
    public GridPlainFutureAdapter(R res) {
        onDone(res);
    }

    /**
     * Creates failed finished future with given error.
     *
     * @param err Future error.
     */
    public GridPlainFutureAdapter(Throwable err) {
        onDone(err);
    }

    /** {@inheritDoc} */
    @Override public R get() throws IgniteCheckedException {
        try {
            if (doneLatch.getCount() > 0)
                doneLatch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteCheckedException("Operation was interrupted.", e);
        }

        return getResult();
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws IgniteCheckedException {
        A.ensure(timeout >= 0, "timeout >= 0");

        try {
            if (doneLatch.getCount() > 0 && !doneLatch.await(timeout, unit))
                throw new IgniteFutureTimeoutException("Failed to get future result due to waiting timed out.");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteCheckedException("Operation was interrupted.", e);
        }

        return getResult();
    }

    /**
     * Get future result.
     *
     * @return Future result.
     * @throws IgniteCheckedException In case of error.
     */
    private R getResult() throws IgniteCheckedException {
        assert doneLatch.getCount() == 0;

        if (err == null)
            return res;

        if (err instanceof Error)
            throw (Error)err;

        if (err instanceof IgniteCheckedException)
            throw (IgniteCheckedException)err;

        throw new IgniteCheckedException(err);
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
    public void onDone(R res) {
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
    @Override public void listenAsync(final GridPlainInClosure<GridPlainFuture<R>>... lsnrs) {
        assert lsnrs != null;

        for (GridPlainInClosure<GridPlainFuture<R>> lsnr : lsnrs)
            cbs.add(new DoneCallback<R>(null, lsnr, null));

        if (isDone())
            fireDone();
    }

    /**
     * Removes listeners registered before.
     *
     * @param lsnrs Listeners to be removed.
     */
    @Override public void stopListenAsync(GridPlainInClosure<GridPlainFuture<R>>... lsnrs) {
        Collection<GridPlainInClosure<GridPlainFuture<R>>> lsnrsCol = lsnrs == null ? null : Arrays.asList(lsnrs);

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
    @Override public <T> GridPlainFutureAdapter<T> chain(GridPlainClosure<GridPlainFuture<R>, T> cb) {
        GridPlainFutureAdapter<T> fut = new GridPlainFutureAdapter<>();

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
            }

        if (err != null)
            throw err;
    }

    /** This future finished notification callback. */
    private class DoneCallback<T> {
        /** Done future callback. */
        private final GridPlainClosure<GridPlainFuture<R>, T> cb;

        /** Done future listener. */
        private final GridPlainInClosure<GridPlainFuture<R>> lsnr;

        /** Chained future. */
        private final GridPlainFutureAdapter<T> chainedFut;

        /**
         * Constructs future finished notification callback.
         *
         * @param cb Future finished callback.
         * @param chainedFut Chained future to set callback result to.
         */
        private DoneCallback(GridPlainClosure<GridPlainFuture<R>, T> cb, GridPlainInClosure<GridPlainFuture<R>> lsnr,
            GridPlainFutureAdapter<T> chainedFut) {
            this.cb = cb;
            this.lsnr = lsnr;
            this.chainedFut = chainedFut;
        }

        /**
         * Proceed this future callback.
         */
        public void proceed() {
            GridPlainFutureAdapter<R> fut = GridPlainFutureAdapter.this;

            assert fut.isDone();

            try {
                if (lsnr != null)
                    lsnr.apply(fut);

                T res = null;

                if (cb != null)
                    res = cb.apply(fut);

                if (chainedFut != null)
                    chainedFut.onDone(res);
            }
            catch (IgniteCheckedException | RuntimeException e) {
                if (chainedFut != null)
                    chainedFut.onDone(e);
            }
            catch (Error e) {
                if (chainedFut != null)
                    chainedFut.onDone(e);

                throw e;
            }
        }
    }
}

