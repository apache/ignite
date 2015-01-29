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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Future adapter without kernal context.
 */
public class GridFutureAdapterEx<R> extends AbstractQueuedSynchronizer implements IgniteInternalFuture<R>, Externalizable {
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
    private final long startTime = U.currentTimeMillis();

    /** Future end time. */
    private volatile long endTime;

    /** Set to {@code false} on deserialization whenever incomplete future is serialized. */
    private boolean valid = true;

    /** Asynchronous listener. */
    private final ConcurrentLinkedDeque8<IgniteInClosure<? super IgniteInternalFuture<R>>>
        lsnrs = new ConcurrentLinkedDeque8<>();

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    @SuppressWarnings("RedundantNoArgConstructor")
    public GridFutureAdapterEx() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        long endTime = this.endTime;

        return endTime == 0 ? U.currentTimeMillis() - startTime : endTime - startTime;
    }

    /** {@inheritDoc} */
    @Override public boolean concurrentNotify() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void concurrentNotify(boolean concurNotify) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean syncNotify() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void syncNotify(boolean syncNotify) {
        // No-op
    }

    /**
     * Checks that future is in usable state.
     */
    protected void checkValid() {
        if (!valid)
            throw new IllegalStateException("Incomplete future was serialized and cannot " +
                "be used after deserialization.");
    }

    /**
     * @return Valid flag.
     */
    protected boolean isValid() {
        return valid;
    }

    /**
     * @return Value of error.
     */
    protected Throwable error() {
        checkValid();

        return err;
    }

    /**
     * @return Value of result.
     */
    protected R result() {
        checkValid();

        return res;
    }

    /** {@inheritDoc} */
    @Override public R get() throws IgniteCheckedException {
        checkValid();

        try {
            if (endTime == 0)
                acquireSharedInterruptibly(0);

            if (getState() == CANCELLED)
                throw new IgniteFutureCancelledCheckedException("Future was cancelled: " + this);

            if (err != null)
                throw U.cast(err);

            return res;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
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

        checkValid();

        try {
            return get0(unit.toNanos(timeout));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException("Got interrupted while waiting for future to complete.", e);
        }
    }

    /**
     * @param nanosTimeout Timeout (nanoseconds).
     * @return Result.
     * @throws InterruptedException If interrupted.
     * @throws org.apache.ignite.internal.IgniteFutureTimeoutCheckedException If timeout reached before computation completed.
     * @throws IgniteCheckedException If error occurred.
     */
    @Nullable protected R get0(long nanosTimeout) throws InterruptedException, IgniteCheckedException {
        if (endTime == 0 && !tryAcquireSharedNanos(0, nanosTimeout))
            throw new IgniteFutureTimeoutCheckedException("Timeout was reached before computation completed.");

        if (getState() == CANCELLED)
            throw new IgniteFutureCancelledCheckedException("Future was cancelled: " + this);

        if (err != null)
            throw U.cast(err);

        return res;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    @Override public void listenAsync(@Nullable final IgniteInClosure<? super IgniteInternalFuture<R>> lsnr) {
        if (lsnr != null) {
            checkValid();

            boolean done;

            IgniteInClosure<? super IgniteInternalFuture<R>> lsnr0 = lsnr;

            done = isDone();

            if (!done) {
                lsnr0 = new IgniteInClosure<IgniteInternalFuture<R>>() {
                    private final AtomicBoolean called = new AtomicBoolean();

                    @Override public void apply(IgniteInternalFuture<R> t) {
                        if (called.compareAndSet(false, true))
                            lsnr.apply(t);
                    }

                    @Override public boolean equals(Object o) {
                        return o != null && (o == this || o == lsnr || o.equals(lsnr));
                    }

                    @Override public String toString() {
                        return lsnr.toString();
                    }
                };

                lsnrs.add(lsnr0);

                done = isDone(); // Double check.
            }

            if (done)
                notifyListener(lsnr0);
        }
    }

    /** {@inheritDoc} */
    @Override public void stopListenAsync(@Nullable IgniteInClosure<? super IgniteInternalFuture<R>>... lsnr) {
        if (lsnr == null || lsnr.length == 0)
            lsnrs.clear();
        else {
            // Iterate through the whole list, removing all occurrences, if any.
            for (Iterator<IgniteInClosure<? super IgniteInternalFuture<R>>> it = lsnrs.iterator(); it.hasNext(); ) {
                IgniteInClosure<? super IgniteInternalFuture<R>> l1 = it.next();

                for (IgniteInClosure<? super IgniteInternalFuture<R>> l2 : lsnr)
                    // Must be l1.equals(l2), not l2.equals(l1), because of the way listeners are added.
                    if (l1.equals(l2))
                        it.remove();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<T> chain(final IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb) {
        final GridFutureAdapterEx<T> fut = new GridFutureAdapterEx<T>() {
            @Override public String toString() {
                return "ChainFuture[orig=" + GridFutureAdapterEx.this + ", doneCb=" + doneCb + ']';
            }
        };

        listenAsync(new IgniteInClosure<IgniteInternalFuture<R>>() {
            @Override public void apply(IgniteInternalFuture<R> t) {
                try {
                    fut.onDone(doneCb.apply(t));
                }
                catch (GridClosureException e) {
                    fut.onDone(e.unwrap());
                }
                catch (RuntimeException e) {
                    U.warn(null, "Failed to notify chained future (is grid stopped?) [, doneCb=" + doneCb +
                        ", err=" + e.getMessage() + ']');

                    fut.onDone(e);

                    throw e;
                }
                catch (Error e) {
                    U.warn(null, "Failed to notify chained future (is grid stopped?) [doneCb=" + doneCb +
                        ", err=" + e.getMessage() + ']');

                    fut.onDone(e);

                    throw e;
                }
            }
        });

        return fut;
    }

    /**
     * Notifies all registered listeners.
     */
    private void notifyListeners() {
        if (lsnrs.isEmptyx())
            return;

        for (IgniteInClosure<? super IgniteInternalFuture<R>> lsnr : lsnrs)
            notifyListener(lsnr);
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
            U.warn(null, "Failed to notify listener (is grid stopped?) [lsnr=" + lsnr +
                ", err=" + e.getMessage() + ']');
        }
        catch (RuntimeException | Error e) {
            U.error(null, "Failed to notify listener: " + lsnr, e);

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
        checkValid();

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        // Don't check for "valid" here, as "done" flag can be read
        // even in invalid state.
        return endTime != 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        checkValid();

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
        checkValid();

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

    /**
     * @return String representation of state.
     */
    private String state() {
        int s = getState();

        return s == INIT ? "INIT" : s == CANCELLED ? "CANCELLED" : "DONE";
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        int state0 = getState();

        out.writeByte(state0);

        // Don't write any further if not done, as deserialized future
        // will be invalid anyways.
        if (state0 != INIT) {
            try {
                acquireSharedInterruptibly(0);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IOException("Thread has been interrupted.", e);
            }

            out.writeObject(res);
            out.writeObject(err);
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int state0 = in.readByte();

        setState(state0);

        if (state0 == INIT)
            valid = false;
        else {
            res = (R)in.readObject();
            err = (Throwable)in.readObject();

            // Prevent any thread from being locked on deserialized future.
            // This will also set 'endTime'.
            releaseShared(0);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFutureAdapterEx.class, this, "state", state());
    }
}
