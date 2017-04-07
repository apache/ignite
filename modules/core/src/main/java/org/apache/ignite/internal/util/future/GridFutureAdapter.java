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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCheckedException;
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
    /*
     * https://bugs.openjdk.java.net/browse/JDK-8074773
     */
    static {
        @SuppressWarnings("unused")
        Class<?> ensureLoaded = LockSupport.class;
    }

    /**
     * No result representation (in case the future has not completed yet).
     */
    private static final Object NONE = new Object();

    /**
     * Future state.
     */
    private enum State {
        /** */
        INIT,
        /** */
        CANCELLED,
        /** */
        FAILED,
        /** */
        DONE
    }

    /**
     * Stack node.
     */
    private static final class Node {
        /** */
        private final Object val;

        /** */
        private volatile Object next;

        /**
         * @param val Node value.
         */
        Node(Object val) {
            this.val = val;
        }
    }

    /** */
    private boolean ignoreInterrupts;

    /** */
    @GridToStringExclude
    private final AtomicReference<Object> state = new AtomicReference<Object>(State.INIT);

    /** */
    @GridToStringExclude
    private volatile Object res = NONE;

    /**
     * Determines whether the future will ignore interrupts.
     */
    public void ignoreInterrupts() {
        ignoreInterrupts = true;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        return State.FAILED == state.get() ? (Throwable)result0() : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public R result() {
        return State.DONE == state.get() ? (R)result0() : null;
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

        return get0(unit.toNanos(timeout));
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
                        unregisterWaiter(Thread.currentThread());

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
     * @param nanosTimeout Timeout (nanoseconds).
     * @return Result.
     * @throws IgniteFutureTimeoutCheckedException If timeout reached before computation completed.
     * @throws IgniteCheckedException If error occurred.
     */
    @Nullable private R get0(long nanosTimeout) throws IgniteCheckedException {
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
                        unregisterWaiter(Thread.currentThread());

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
        Object state0 = state.get();

        if (state0 == State.DONE)
            return (R)result0();
        else if (state0 == State.FAILED)
            throw U.cast((Throwable)result0());
        else if (state0 == State.CANCELLED)
            throw new IgniteFutureCancelledCheckedException("Future was cancelled: " + this);

        throw new IllegalStateException("Illegal state: " + stateStr(state0));
    }

    /**
     * @return Future result.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    private Object result0() {
        assert isDone(state.get());

        while (res == NONE); // Wait for result if necessary

        return res;
    }

    /**
     * @param waiter Waiter to register.
     * @return {@code True} if was registered successfully.
     */
    private boolean registerWaiter(Object waiter) {
        Node node = null;

        while (true) {
            final Object oldState = state.get();

            if (isDone(oldState))
                return false;

            Object newState;

            if (oldState == State.INIT)
                newState = waiter;

            else {
                if (node == null)
                    node = new Node(waiter);

                node.next = oldState;

                newState = node;
            }

            if (state.compareAndSet(oldState, newState))
                return true;
        }
    }

    /**
     * @param waiter Waiter to unregister.
     */
    private void unregisterWaiter(Thread waiter) {
        Node prev = null;
        Object cur = state.get();

        while (cur != null) {
            boolean isNode = cur.getClass() == Node.class;

            Object curWaiter = isNode ? ((Node)cur).val : cur;
            Object next = isNode ? ((Node)cur).next : null;

            if (curWaiter == waiter) {
                if (prev == null) {
                    Object n = next == null ? State.INIT : next;

                    cur = state.compareAndSet(cur, n) ? null : state.get();
                }
                else {
                    prev.next = next;

                    cur = null;
                }
            }
            else {
                prev = isNode ? (Node)cur : null;

                cur = next;
            }
        }
    }

    /**
     * @param waiter Head of waiters stack.
     */
    @SuppressWarnings("unchecked")
    private void unblockAll(Object waiter) {
        while (waiter != null) {
            if (waiter instanceof Thread) {
                LockSupport.unpark((Thread)waiter);

                return;
            }
            else if (waiter instanceof IgniteInClosure) {
                notifyListener((IgniteInClosure<? super IgniteInternalFuture<R>>)waiter);

                return;
            }
            else if (waiter.getClass() == Node.class) {
                Node node = (Node) waiter;

                unblockAll(node.val);

                waiter = node.next;
            }
            else
                return;
        }
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
        return res != NONE;
    }

    /**
     * @param state State to check.
     * @return {@code True} if future is done.
     */
    private boolean isDone(Object state) {
        return State.INIT != state && state.getClass() == State.class;
    }

    /**
     * @return Checks is future is completed with exception.
     */
    public boolean isFailed() {
        return state.get() == State.FAILED;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return state.get() == State.CANCELLED;
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
        Object val;
        State newState;

        if (cancel) {
            newState = State.CANCELLED;
            val = null;
        }
        else if (err != null) {
            newState = State.FAILED;
            val = err;
        }
        else {
            newState = State.DONE;
            val = res;
        }

        while (true) {
            final Object oldState = state.get();

            if (isDone(oldState))
                return false;

            if (state.compareAndSet(oldState, newState)) {
                this.res = val;

                if(oldState != State.INIT)
                    unblockAll(oldState);

                return true;
            }
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
    @Override public String toString() {
        return S.toString(
            GridFutureAdapter.class, this,
            "state", stateStr(state.get()), false,
            "res", resStr(res), true,
            "hash", System.identityHashCode(this), false);
    }

    /**
     * @param s State.
     * @return State string representation.
     */
    private String stateStr(Object s) {
        return s.getClass() == State.class ? s.toString() : "INIT";
    }

    /**
     * @param res Result.
     * @return Result string representation.
     */
    private String resStr(Object res) {
        return res == NONE ? "null" : String.valueOf(res);
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