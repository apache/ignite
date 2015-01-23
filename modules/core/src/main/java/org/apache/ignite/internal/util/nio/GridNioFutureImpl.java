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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Default future implementation.
 */
public class GridNioFutureImpl<R> extends AbstractQueuedSynchronizer implements GridNioFuture<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Initial state. */
    private static final int INIT = 0;

    /** Cancelled state. */
    private static final int CANCELLED = 1;

    /** Done state. */
    private static final int DONE = 2;

    /** Result. */
    @GridToStringInclude
    private R res;

    /** Error. */
    private Throwable err;

    /** Future start time. */
    protected final long startTime = U.currentTimeMillis();

    /** */
    protected volatile long endTime;

    /** */
    protected boolean msgThread;

    /** Asynchronous listeners. */
    private Collection<IgniteInClosure<? super GridNioFuture<R>>> lsnrs;

    /** */
    private final Object mux = new Object();

    /**
     * @return Value of error.
     */
    protected Throwable error() {
        return err;
    }

    /**
     * @return Value of result.
     */
    protected R result() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public R get() throws IOException, IgniteCheckedException {
        try {
            if (endTime == 0)
                acquireSharedInterruptibly(0);

            if (getState() == CANCELLED)
                throw new IgniteFutureCancelledException("Future was cancelled: " + this);

            if (err != null)
                throw U.cast(err);

            return res;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout) throws IOException, IgniteCheckedException {
        // Do not replace with static import, as it may not compile.
        return get(timeout, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout, TimeUnit unit) throws IOException, IgniteCheckedException {
        A.ensure(timeout >= 0, "timeout cannot be negative: " + timeout);
        A.notNull(unit, "unit");

        try {
            return get0(unit.toNanos(timeout));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException("Got interrupted while waiting for future to complete.", e);
        }
    }

    /**
     * @param nanosTimeout Timeout (nanoseconds).
     * @return Result.
     * @throws InterruptedException If interrupted.
     * @throws IgniteFutureTimeoutException If timeout reached before computation completed.
     * @throws IgniteCheckedException If error occurred.
     */
    @Nullable protected R get0(long nanosTimeout) throws InterruptedException, IgniteCheckedException {
        if (endTime == 0 && !tryAcquireSharedNanos(0, nanosTimeout))
            throw new IgniteFutureTimeoutException("Timeout was reached before computation completed.");

        if (getState() == CANCELLED)
            throw new IgniteFutureCancelledException("Future was cancelled: " + this);

        if (err != null)
            throw U.cast(err);

        return res;
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
        return getState() != INIT;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return getState() == CANCELLED;
    }

    /** {@inheritDoc} */
    @Override public void listenAsync(@Nullable final IgniteInClosure<? super GridNioFuture<R>> lsnr) {
        if (lsnr != null) {
            boolean done = isDone();

            if (!done) {
                synchronized (mux) {
                    done = isDone(); // Double check.

                    if (!done) {
                        if (lsnrs == null)
                            lsnrs = new ArrayList<>();

                        lsnrs.add(lsnr);
                    }
                }
            }

            if (done)
                lsnr.apply(this);
        }
    }

    /** {@inheritDoc} */
    @Override public void messageThread(boolean msgThread) {
        this.msgThread = msgThread;
    }

    /** {@inheritDoc} */
    @Override public boolean messageThread() {
        return msgThread;
    }

    /**
     * Notifies all registered listeners.
     */
    private void notifyListeners() {
        final Collection<IgniteInClosure<? super GridNioFuture<R>>> lsnrs0;

        synchronized (mux) {
            lsnrs0 = lsnrs;

            if (lsnrs0 == null)
                return;

            lsnrs = null;
        }

        assert !lsnrs0.isEmpty();

        for (IgniteInClosure<? super GridNioFuture<R>> lsnr : lsnrs0)
            lsnr.apply(this);
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
     * Callback to notify that future is finished. Note that if non-{@code null} exception is passed in
     * the result value will be ignored.
     *
     * @param res Optional result.
     * @param err Optional error.
     * @param cancel {@code True} if future was cancelled.
     * @return {@code True} if result was set by this call.
     */
    private boolean onDone(@Nullable R res, @Nullable Throwable err, boolean cancel) {
        boolean notify = false;

        try {
            if (compareAndSetState(INIT, cancel ? CANCELLED : DONE)) {
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

    /** {@inheritDoc} */
    @Override public boolean skipRecovery() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioFutureImpl.class, this);
    }
}
