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

package org.apache.ignite.internal.util.future;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Future adapter.
 */
public class GridFutureAdapter<R> extends AbstractQueuedSynchronizer implements IgniteInternalFuture<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Initial state. */
    private static final int INIT = 0;

    /** Cancelled state. */
    private static final int CANCELLED = 1;

    /** Done state. */
    private static final int DONE = 2;

    /** */
    private static final byte ERR = 1;

    /** */
    private static final byte RES = 2;

    /** */
    private byte resFlag;

    /** Result. */
    @GridToStringInclude(sensitive = true)
    private Object res;

    /** Future start time. */
    private final long startTime = U.currentTimeMillis();

    /** Future end time. */
    private volatile long endTime;

    /** */
    private boolean ignoreInterrupts;

    /** */
    @GridToStringExclude
    private IgniteInClosure<? super IgniteInternalFuture<R>> lsnr;

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        long endTime = this.endTime;

        return endTime == 0 ? U.currentTimeMillis() - startTime : endTime - startTime;
    }

    /**
     * @param ignoreInterrupts Ignore interrupts flag.
     */
    public void ignoreInterrupts(boolean ignoreInterrupts) {
        this.ignoreInterrupts = ignoreInterrupts;
    }

    /**
     * @return Future end time.
     */
    public long endTime() {
        return endTime;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        return (resFlag == ERR) ? (Throwable)res : null;
    }

    /** {@inheritDoc} */
    @Override public R result() {
        return resFlag == RES ? (R)res : null;
    }

    /** {@inheritDoc} */
    @Override public R get() throws IgniteCheckedException {
        return get0(ignoreInterrupts);
    }

    /** {@inheritDoc} */
    @Override public R getUninterruptibly() throws IgniteCheckedException {
        return get0(true);
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout) throws IgniteCheckedException {
        // Do not replace with static import, as it may not compile.
        return get(timeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws IgniteCheckedException {
        A.ensure(timeout >= 0, "timeout cannot be negative: " + timeout);
        A.notNull(unit, "unit");

        try {
            return get0(unit.toNanos(timeout));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException("Got interrupted while waiting for future to complete.", e);
        }
    }

    /**
     * Internal get routine.
     *
     * @param ignoreInterrupts Whether to ignore interrupts.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private R get0(boolean ignoreInterrupts) throws IgniteCheckedException {
        try {
            if (endTime == 0) {
                if (ignoreInterrupts)
                    acquireShared(0);
                else
                    acquireSharedInterruptibly(0);
            }

            if (getState() == CANCELLED)
                throw new IgniteFutureCancelledCheckedException("Future was cancelled: " + this);

            assert resFlag != 0;

            if (resFlag == ERR)
                throw U.cast((Throwable)res);

            return (R)res;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * @param nanosTimeout Timeout (nanoseconds).
     * @return Result.
     * @throws InterruptedException If interrupted.
     * @throws IgniteFutureTimeoutCheckedException If timeout reached before computation completed.
     * @throws IgniteCheckedException If error occurred.
     */
    @Nullable protected R get0(long nanosTimeout) throws InterruptedException, IgniteCheckedException {
        if (endTime == 0 && !tryAcquireSharedNanos(0, nanosTimeout))
            throw new IgniteFutureTimeoutCheckedException("Timeout was reached before computation completed.");

        if (getState() == CANCELLED)
            throw new IgniteFutureCancelledCheckedException("Future was cancelled: " + this);

        assert resFlag != 0;

        if (resFlag == ERR)
            throw U.cast((Throwable)res);

        return (R)res;
    }

    /** {@inheritDoc} */
    @Override public void listen(IgniteInClosure<? super IgniteInternalFuture<R>> lsnr0) {
        assert lsnr0 != null;

        boolean done = isDone();

        if (!done) {
            synchronized (this) {
                done = isDone(); // Double check.

                if (!done) {
                    if (lsnr == null)
                        lsnr = lsnr0;
                    else if (lsnr instanceof ArrayListener)
                        ((ArrayListener)lsnr).add(lsnr0);
                    else
                        lsnr = (IgniteInClosure)new ArrayListener<IgniteInternalFuture>(lsnr, lsnr0);

                    return;
                }
            }
        }

        assert done;

        notifyListener(lsnr0);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<T> chain(final IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb) {
        return new ChainFuture<>(this, doneCb, null);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<T> chain(final IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb,
        Executor exec) {
        return new ChainFuture<>(this, doneCb, exec);
    }

    /**
     * Notifies all registered listeners.
     */
    private void notifyListeners() {
        IgniteInClosure<? super IgniteInternalFuture<R>> lsnr0;

        synchronized (this) {
            lsnr0 = lsnr;

            if (lsnr0 == null)
                return;

            lsnr = null;
        }

        assert lsnr0 != null;

        notifyListener(lsnr0);
    }

    /**
     * Notifies single listener.
     *
     * @param lsnr Listener.
     */
    private void notifyListener(IgniteInClosure<? super IgniteInternalFuture<R>> lsnr) {
        assert lsnr != null;

        try {
            lsnr.apply(this);
        }
        catch (IllegalStateException e) {
            U.error(logger(), "Failed to notify listener (is grid stopped?) [fut=" + this +
                ", lsnr=" + lsnr + ", err=" + e.getMessage() + ']', e);
        }
        catch (RuntimeException | Error e) {
            U.error(logger(), "Failed to notify listener: " + lsnr, e);

            throw e;
        }
    }

    /**
     * Default no-op implementation that always returns {@code false}.
     * Futures that do support cancellation should override this method
     * and call {@link #onCancelled()} callback explicitly if cancellation
     * indeed did happen.
     */
    @Override public boolean cancel() throws IgniteCheckedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        // Don't check for "valid" here, as "done" flag can be read
        // even in invalid state.
        return endTime != 0;
    }

    /**
     * @return Checks is future is completed with exception.
     */
    public boolean isFailed() {
        // Must read endTime first.
        return endTime != 0 && resFlag == ERR;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return getState() == CANCELLED;
    }

    /**
     * Callback to notify that future is finished with {@code null} result.
     * This method must delegate to {@link #onDone(Object, Throwable)} method.
     *
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone() {
        return onDone(null, null);
    }

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(Object, Throwable)} method.
     *
     * @param res Result.
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone(@Nullable R res) {
        return onDone(res, null);
    }

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(Object, Throwable)} method.
     *
     * @param err Error.
     * @return {@code True} if result was set by this call.
     */
    public final boolean onDone(@Nullable Throwable err) {
        return onDone(null, err);
    }

    /**
     * Callback to notify that future is finished. Note that if non-{@code null} exception is passed in
     * the result value will be ignored.
     *
     * @param res Optional result.
     * @param err Optional error.
     * @return {@code True} if result was set by this call.
     */
    public boolean onDone(@Nullable R res, @Nullable Throwable err) {
        return onDone(res, err, false);
    }

    /**
     * @param res Result.
     * @param err Error.
     * @param cancel {@code True} if future is being cancelled.
     * @return {@code True} if result was set by this call.
     */
    private boolean onDone(@Nullable R res, @Nullable Throwable err, boolean cancel) {
        boolean notify = false;

        try {
            if (compareAndSetState(INIT, cancel ? CANCELLED : DONE)) {
                if (err != null) {
                    resFlag = ERR;
                    this.res = err;
                }
                else {
                    resFlag = RES;
                    this.res = res;
                }

                notify = true;

                releaseShared(0);

                return true;
            }

            return false;
        }
        finally {
            if (notify)
                notifyListeners();
        }
    }

    /**
     * Callback to notify that future is cancelled.
     *
     * @return {@code True} if cancel flag was set by this call.
     */
    public boolean onCancelled() {
        return onDone(null, null, true);
    }

    /** {@inheritDoc} */
    @Override protected final int tryAcquireShared(int ignore) {
        return endTime != 0 ? 1 : -1;
    }

    /** {@inheritDoc} */
    @Override protected final boolean tryReleaseShared(int ignore) {
        endTime = U.currentTimeMillis();

        // Always signal after setting final done status.
        return true;
    }

    /**
     * @return String representation of state.
     */
    private String state() {
        int s = getState();

        return s == INIT ? "INIT" : s == CANCELLED ? "CANCELLED" : "DONE";
    }

    /**
     * @return Logger instance.
     */
    @Nullable public IgniteLogger logger() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFutureAdapter.class, this, "state", state());
    }

    /**
     *
     */
    private static class ArrayListener<R> implements IgniteInClosure<IgniteInternalFuture<R>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private IgniteInClosure<? super IgniteInternalFuture<R>>[] arr;

        /**
         * @param lsnrs Listeners.
         */
        private ArrayListener(IgniteInClosure... lsnrs) {
            this.arr = lsnrs;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteInternalFuture<R> fut) {
            for (int i = 0; i < arr.length; i++)
                arr[i].apply(fut);
        }

        /**
         * @param lsnr Listener.
         */
        void add(IgniteInClosure<? super IgniteInternalFuture<R>> lsnr) {
            arr = Arrays.copyOf(arr, arr.length + 1);

            arr[arr.length - 1] = lsnr;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ArrayListener.class, this, "arrSize", arr.length);
        }
    }

    /**
     *
     */
    private static class ChainFuture<R, T> extends GridFutureAdapter<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private GridFutureAdapter<R> fut;

        /** */
        private IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb;

        /**
         *
         */
        public ChainFuture() {
            // No-op.
        }

        /**
         * @param fut Future.
         * @param doneCb Closure.
         * @param cbExec Optional executor to run callback.
         */
        ChainFuture(
            GridFutureAdapter<R> fut,
            IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb,
            @Nullable Executor cbExec
        ) {
            this.fut = fut;
            this.doneCb = doneCb;

            fut.listen(new GridFutureChainListener<>(this, doneCb, cbExec));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ChainFuture [orig=" + fut + ", doneCb=" + doneCb + ']';
        }
    }
}
