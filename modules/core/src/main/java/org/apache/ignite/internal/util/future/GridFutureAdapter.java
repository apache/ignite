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

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Future adapter.
 */
public class GridFutureAdapter<R> implements IgniteInternalFuture<R> {
    /** Done state representation. */
    private static final String DONE = "DONE";

    /** Initial state. */
    private static final Node INIT = new Node(null);

    /** Cancelled state. */
    private static final Object CANCELLED = new Object();

    /** */
    private static final AtomicReferenceFieldUpdater<GridFutureAdapter, Object> stateUpdater =
        AtomicReferenceFieldUpdater.newUpdater(GridFutureAdapter.class, Object.class, "state");

    /*
     * https://bugs.openjdk.java.net/browse/JDK-8074773
     */
    static {
        @SuppressWarnings("unused")
        Class<?> ensureLoaded = LockSupport.class;
    }

    /**
     * Stack node.
     */
    private static final class Node {
        /** */
        private final Object val;

        /** */
        private volatile Node next;

        /**
         * @param val Node value.
         */
        Node(Object val) {
            this.val = val;
        }
    }

    /** */
    private static final class ErrorWrapper {
        /** */
        private final Throwable error;

        /**
         * @param error Error.
         */
        ErrorWrapper(Throwable error) {
            this.error = error;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return String.valueOf(error);
        }
    }

    /** */
    private volatile boolean ignoreInterrupts;

    /** */
    @GridToStringExclude
    private volatile Object state = INIT;

    /**
     * Determines whether the future will ignore interrupts while waiting for result in {@code get()} methods.
     * This call should <i>happen before</i> subsequent {@code get()} in order to have guaranteed effect.
     */
    public void ignoreInterrupts() {
        ignoreInterrupts = true;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        Object state0 = state;

        if (state0 != null && state0.getClass() == ErrorWrapper.class)
            return ((ErrorWrapper)state0).error;

        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public R result() {
        Object state0 = state;

        if(state0 == null ||                           // It is DONE state
           (state0.getClass() != Node.class &&         // It is not INIT state
            state0.getClass() != ErrorWrapper.class && // It is not FAILED
            state0 != CANCELLED))                      // It is not CANCELLED
            return (R)state0;

        return null;
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

        return get0(ignoreInterrupts, unit.toNanos(timeout));
    }

    /**
     * Internal get routine.
     *
     * @param ignoreInterrupts Whether to ignore interrupts.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private R get0(boolean ignoreInterrupts) throws IgniteCheckedException {
        if (isDone() || !registerWaiter(Thread.currentThread()))
            return resolve();

        boolean interrupted = false;

        try {
            while (true) {
                LockSupport.park();

                if (Thread.interrupted()) {
                    interrupted = true;

                    if (!ignoreInterrupts) {
                        unregisterWaiter();

                        throw new IgniteInterruptedCheckedException("Got interrupted while waiting for future to complete.");
                    }
                }

                if (isDone())
                    return resolve();
            }
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * @param ignoreInterrupts Whether to ignore interrupts.
     * @param nanosTimeout Timeout (nanoseconds).
     * @return Result.
     * @throws IgniteFutureTimeoutCheckedException If timeout reached before computation completed.
     * @throws IgniteCheckedException If error occurred.
     */
    @Nullable private R get0(boolean ignoreInterrupts, long nanosTimeout) throws IgniteCheckedException {
        if (isDone() || !registerWaiter(Thread.currentThread()))
            return resolve();

        long deadlineNanos = System.nanoTime() + nanosTimeout;

        boolean interrupted = false;

        try {
            long nanosTimeout0 = nanosTimeout;

            while (nanosTimeout0 > 0) {
                LockSupport.parkNanos(nanosTimeout0);

                nanosTimeout0 = deadlineNanos - System.nanoTime();

                if (Thread.interrupted()) {
                    interrupted = true;

                    if (!ignoreInterrupts) {
                        unregisterWaiter();

                        throw new IgniteInterruptedCheckedException("Got interrupted while waiting for future to complete.");
                    }
                }

                if (isDone())
                    return resolve();
            }
        }
        finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }

        unregisterWaiter();

        throw new IgniteFutureTimeoutCheckedException("Timeout was reached before computation completed.");
    }

    /**
     * Resolves the value to result or exception.
     *
     * @return Result.
     * @throws IgniteCheckedException If resolved to exception.
     */
    @SuppressWarnings("unchecked")
    private R resolve() throws IgniteCheckedException {
        if(state == CANCELLED)
            throw new IgniteFutureCancelledCheckedException("Future was cancelled: " + this);

        if(state == null || state.getClass() != ErrorWrapper.class)
            return (R)state;

        throw U.cast(((ErrorWrapper)state).error);
    }

    /**
     * @param waiter Waiter to register.
     * @return {@code True} if was registered successfully.
     */
    private boolean registerWaiter(Object waiter) {
        Node node = null;

        while (true) {
            final Object oldState = state;

            if (isDone(oldState))
                return false;

            if(node == null)
                node = new Node(waiter);

            if(oldState != INIT && oldState.getClass() == Node.class)
                node.next = (Node)oldState;

            if (compareAndSetState(oldState, node))
                return true;
        }
    }

    /**
     * Unregisters current thread from waiters list.
     */
    private void unregisterWaiter() {
        Node prev = null;
        Object cur = state;

        while (cur != null) {
            if(cur.getClass() != Node.class)
                return;

            Object curWaiter = ((Node)cur).val;
            Node next = ((Node)cur).next;

            if (curWaiter == Thread.currentThread()) {
                if (prev == null) {
                    Object n = next == null ? INIT : next;

                    cur = compareAndSetState(cur, n) ? null : state;
                }
                else {
                    prev.next = next;

                    cur = null;
                }
            }
            else {
                prev = (Node)cur;

                cur = next;
            }
        }
    }

    /**
     * @param exp Expected state.
     * @param newState New state.
     * @return {@code True} if success
     */
    private boolean compareAndSetState(Object exp, Object newState) {
        return stateUpdater.compareAndSet(this, exp, newState);
    }

    /**
     * @param head Head of waiters stack.
     */
    @SuppressWarnings("unchecked")
    private void unblockAll(Node head) {
        while (head != null) {
            unblock(head.val);
            head = head.next;
        }
    }

    /**
     * @param waiter Waiter to unblock
     */
    private void unblock(Object waiter) {
        if(waiter instanceof Thread)
            LockSupport.unpark((Thread)waiter);
        else
            notifyListener((IgniteInClosure<? super IgniteInternalFuture<R>>)waiter);
    }

    /** {@inheritDoc} */
    @Override public void listen(IgniteInClosure<? super IgniteInternalFuture<R>> lsnr) {
        if (!registerWaiter(lsnr))
            notifyListener(lsnr);
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
     * @return Logger instance.
     */
    @Nullable public IgniteLogger logger() {
        return null;
    }

    /**
     * Notifies listener.
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
        return isDone(state);
    }

    /**
     * @param state State to check.
     * @return {@code True} if future is done.
     */
    private boolean isDone(Object state) {
        return state == null || state.getClass() != Node.class;
    }

    /**
     * @return {@code True} if future is completed with exception.
     */
    public boolean isFailed() {
        Object state0 = state;

        return state0 != null && state0.getClass() == ErrorWrapper.class;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return state == CANCELLED;
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
    protected boolean onDone(@Nullable R res, @Nullable Throwable err, boolean cancel) {
        Object newState = cancel ? CANCELLED : err != null ? new ErrorWrapper(err) : res;

        while (true) {
            final Object oldState = state;

            if (isDone(oldState))
                return false;

            if (compareAndSetState(oldState, newState)) {

                if(oldState != INIT)
                    unblockAll((Node)oldState);

                return true;
            }
        }
    }

    /**
     * Resets future for subsequent reuse.
     */
    public void reset() {
        final Object oldState = state;

        if (oldState == INIT)
            return;

        if (!isDone(oldState))
            throw new IgniteException("Illegal state");

        compareAndSetState(oldState, INIT);
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
    @SuppressWarnings("StringEquality")
    @Override public String toString() {
        Object state0 = state;

        String stateStr = stateStr(state0);
        String resStr = stateStr == DONE ? String.valueOf(state0) : null;

        return S.toString(
            GridFutureAdapter.class, this,
            "state", stateStr, false,
            "res", resStr, true,
            "hash", System.identityHashCode(this), false);
    }

    /**
     * @param s State.
     * @return State string representation.
     */
    private String stateStr(Object s) {
        return s == CANCELLED ? "CANCELLED" : s != null && s.getClass() == Node.class ? "INIT" : DONE;
    }

    /**
     *
     */
    private static class ChainFuture<R, T> extends GridFutureAdapter<T> {
        /** */
        private GridFutureAdapter<R> fut;

        /** */
        private IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb;

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
