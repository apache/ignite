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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

/**
 * Future adapter.
 */
public class GridFutureAdapter<R> implements IgniteInternalFuture<R> {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    private static final long RESULT;
    private static final long WAITERS;
    private static final long COMPLETIONS;

    static {
        try {
            Class<?> k = GridFutureAdapter.class;
            RESULT = UNSAFE.objectFieldOffset
                (k.getDeclaredField("result"));
            WAITERS = UNSAFE.objectFieldOffset
                (k.getDeclaredField("waiters"));
            COMPLETIONS = UNSAFE.objectFieldOffset
                (k.getDeclaredField("completions"));
        } catch (Exception e) {
            throw new Error(e);
        }
    }

    static final class AltResult {
        final Throwable ex; // null only for NIL
        AltResult(Throwable ex) { this.ex = ex; }
    }

    static final AltResult NIL = new AltResult(null);
    static final AltResult CANCEL = new AltResult(null);

    // Fields

    volatile Object result;    // Either the result or boxed AltResult
    volatile WaitNode waiters; // Treiber stack of threads blocked on get()
    volatile CompletionNode completions; // list (Treiber stack) of completions

    // Basic utilities for triggering and processing completions

    /**
     * Removes and signals all waiting threads and runs all completions.
     */
    final void postComplete() {
        WaitNode q; Thread t;
        while ((q = waiters) != null) {
            if (UNSAFE.compareAndSwapObject(this, WAITERS, q, q.next) &&
                (t = q.thread) != null) {
                q.thread = null;
                LockSupport.unpark(t);
            }
        }

        CompletionNode h; IgniteInClosure<? super IgniteInternalFuture<R>> c;
        while ((h = completions) != null) {
            if (UNSAFE.compareAndSwapObject(this, COMPLETIONS, h, h.next) &&
                (c = h.completion) != null)
                c.apply(this);
        }
    }

    /**
     * Triggers completion with the encoding of the given arguments:
     * if the exception is non-null, encodes it as a wrapped
     * CompletionException unless it is one already.  Otherwise uses
     * the given result, boxed as NIL if null.
     */
    final boolean internalComplete(R v, Throwable ex, boolean cancel) {
        boolean ret = false;

        if (result == null)
            ret = UNSAFE.compareAndSwapObject
                (this, RESULT, null,
                    cancel ?
                        CANCEL :
                        (ex == null) ?
                            (v == null) ?
                                NIL : v : new AltResult(ex));

        postComplete(); // help out even if not triggered

        return ret;
    }

    /**
     * If triggered, helps release and/or process completions.
     */
    final void helpPostComplete() {
        if (result != null)
            postComplete();
    }

    /* ------------- waiting for completions -------------- */

    /**
     * Simple linked list nodes to record completions, used in
     * basically the same way as WaitNodes. (We separate nodes from
     * the Completions themselves mainly because for the And and Or
     * methods, the same Completion object resides in two lists.)
     */
    static final class CompletionNode<R> {
        final IgniteInClosure<? super IgniteInternalFuture<R>> completion;
        volatile CompletionNode next;
        CompletionNode(IgniteInClosure<? super IgniteInternalFuture<R>> completion) { this.completion = completion; }
    }

    /**
     * Linked nodes to record waiting threads in a Treiber stack.  See
     * other classes such as Phaser and SynchronousQueue for more
     * detailed explanation. This class implements ManagedBlocker to
     * avoid starvation when blocking actions pile up in
     * ForkJoinPools.
     */
    static final class WaitNode implements ForkJoinPool.ManagedBlocker {
        long nanos;          // wait time if timed
        final long deadline; // non-zero if timed
        volatile int interruptControl; // > 0: interruptible, < 0: interrupted
        volatile Thread thread;
        volatile WaitNode next;
        WaitNode(boolean interruptible, long nanos, long deadline) {
            this.thread = Thread.currentThread();
            this.interruptControl = interruptible ? 1 : 0;
            this.nanos = nanos;
            this.deadline = deadline;
        }
        public boolean isReleasable() {
            if (thread == null)
                return true;
            if (Thread.interrupted()) {
                int i = interruptControl;
                interruptControl = -1;
                if (i > 0)
                    return true;
            }
            if (deadline != 0L &&
                (nanos <= 0L || (nanos = deadline - System.nanoTime()) <= 0L)) {
                thread = null;
                return true;
            }
            return false;
        }
        public boolean block() {
            if (isReleasable())
                return true;
            else if (deadline == 0L)
                LockSupport.park(this);
            else if (nanos > 0L)
                LockSupport.parkNanos(this, nanos);
            return isReleasable();
        }
    }

    /**
     * Returns raw result after waiting, or null if interruptible and
     * interrupted.
     */
    private Object waitingGet(boolean interruptible) {
        WaitNode q = null;
        boolean queued = false;
        for (Object r;;) {
            if ((r = result) != null) {
                if (q != null) { // suppress unpark
                    q.thread = null;
                    if (q.interruptControl < 0) {
                        if (interruptible) {
                            removeWaiter(q);
                            return null;
                        }
                        Thread.currentThread().interrupt();
                    }
                }
                postComplete(); // help release others
                return r;
            }
            else if (q == null)
                q = new WaitNode(interruptible, 0L, 0L);
            else if (!queued)
                queued = UNSAFE.compareAndSwapObject(this, WAITERS,
                    q.next = waiters, q);
            else if (interruptible && q.interruptControl < 0) {
                removeWaiter(q);
                return null;
            }
            else if (q.thread != null && result == null) {
                try {
                    ForkJoinPool.managedBlock(q);
                } catch (InterruptedException ex) {
                    q.interruptControl = -1;
                }
            }
        }
    }

    /**
     * Awaits completion or aborts on interrupt or timeout.
     *
     * @param nanos time to wait
     * @return raw result
     */
    private Object timedAwaitDone(long nanos)
        throws InterruptedException, IgniteFutureTimeoutCheckedException {
        WaitNode q = null;
        boolean queued = false;
        for (Object r;;) {
            if ((r = result) != null) {
                if (q != null) {
                    q.thread = null;
                    if (q.interruptControl < 0) {
                        removeWaiter(q);
                        throw new InterruptedException();
                    }
                }
                postComplete();
                return r;
            }
            else if (q == null) {
                if (nanos <= 0L)
                    throw new IgniteFutureTimeoutCheckedException("Timeout was reached before computation completed.");
                long d = System.nanoTime() + nanos;
                q = new WaitNode(true, nanos, d == 0L ? 1L : d); // avoid 0
            }
            else if (!queued)
                queued = UNSAFE.compareAndSwapObject(this, WAITERS,
                    q.next = waiters, q);
            else if (q.interruptControl < 0) {
                removeWaiter(q);
                throw new InterruptedException();
            }
            else if (q.nanos <= 0L) {
                if (result == null) {
                    removeWaiter(q);
                    throw new IgniteFutureTimeoutCheckedException("Timeout was reached before computation completed.");
                }
            }
            else if (q.thread != null && result == null) {
                try {
                    ForkJoinPool.managedBlock(q);
                } catch (InterruptedException ex) {
                    q.interruptControl = -1;
                }
            }
        }
    }

    /**
     * Tries to unlink a timed-out or interrupted wait node to avoid
     * accumulating garbage.  Internal nodes are simply unspliced
     * without CAS since it is harmless if they are traversed anyway
     * by releasers.  To avoid effects of unsplicing from already
     * removed nodes, the list is retraversed in case of an apparent
     * race.  This is slow when there are a lot of nodes, but we don't
     * expect lists to be long enough to outweigh higher-overhead
     * schemes.
     */
    private void removeWaiter(WaitNode node) {
        if (node != null) {
            node.thread = null;
            retry:
            for (;;) {          // restart on removeWaiter race
                for (WaitNode pred = null, q = waiters, s; q != null; q = s) {
                    s = q.next;
                    if (q.thread != null)
                        pred = q;
                    else if (pred != null) {
                        pred.next = s;
                        if (pred.thread == null) // check for race
                            continue retry;
                    }
                    else if (!UNSAFE.compareAndSwapObject(this, WAITERS, q, s))
                        continue retry;
                }
                break;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        return -1;
    }

    /**
     * @param ignoreInterrupts Ignore interrupts flag.
     */
    public void ignoreInterrupts(boolean ignoreInterrupts) {
        //this.ignoreInterrupts = ignoreInterrupts;
    }

    /**
     * @return Future end time.
     */
    public long endTime() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        Object result0 = result;

        return result0 instanceof AltResult ? ((AltResult)result0).ex : null;
    }

    /** {@inheritDoc} */
    @Override public R result() {
        Object result0 = result;

        if (result0 == NIL)
            return null;

        if (result0 == CANCEL)
            return null;

        if (result instanceof AltResult)
            return null;

        return (R)result0;
    }

    /** {@inheritDoc} */
    @Override public R get() throws IgniteCheckedException {
        return get0(false);
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
        Object r;

        Throwable ex;

        if ((r = result) == null && (r = waitingGet(!ignoreInterrupts)) == null)
            throw new IgniteInterruptedCheckedException("Interrupted.");

        if (r == CANCEL)
            throw new IgniteFutureCancelledCheckedException("Future was cancelled: " + this);

        if (!(r instanceof AltResult)) {
            R tr = (R)r;

            return tr;
        }

        if ((ex = ((AltResult)r).ex) == null)
            return null;

        throw U.cast(ex);
    }

    /**
     * @param nanosTimeout Timeout (nanoseconds).
     * @return Result.
     * @throws InterruptedException If interrupted.
     * @throws IgniteFutureTimeoutCheckedException If timeout reached before computation completed.
     * @throws IgniteCheckedException If error occurred.
     */
    @Nullable protected R get0(long nanosTimeout) throws InterruptedException, IgniteCheckedException {
        Object r; Throwable ex;
        if (Thread.interrupted())
            throw new InterruptedException();

        if ((r = result) == null)
            r = timedAwaitDone(nanosTimeout);

        if (!(r instanceof AltResult)) {
            R tr = (R)r;

            return tr;
        }

        if (r == CANCEL)
            throw new IgniteFutureCancelledCheckedException("Future was cancelled: " + this);

        if ((ex = ((AltResult)r).ex) == null)
            return null;

        throw U.cast(ex);
    }

    /** {@inheritDoc} */
    @Override public void listen(IgniteInClosure<? super IgniteInternalFuture<R>> lsnr0) {
        assert lsnr0 != null;

        CompletionNode<R> cn = new CompletionNode<>(lsnr0);

        for (;;) {
            CompletionNode completions1 = completions;

            cn.next = completions1;

            if (UNSAFE.compareAndSwapObject(this, COMPLETIONS, completions1, cn))
                break;
        }

        if (isDone())
            postComplete();
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteInternalFuture<T> chain(final IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb) {
        return new ChainFuture<>(this, doneCb);
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
            U.error(null, "Failed to notify listener (is grid stopped?) [fut=" + this +
                ", lsnr=" + lsnr + ", err=" + e.getMessage() + ']', e);
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
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return result != null;
    }

    /**
     * @return Checks is future is completed with exception.
     */
    public boolean isFailed() {
        Object result0 = result;

        return result0 instanceof AltResult && result0 != NIL;
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return result == CANCEL;
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
        return internalComplete(res, err, cancel);
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
        return S.toString(GridFutureAdapter.class, this, "res", result);
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
         */
        ChainFuture(
            GridFutureAdapter<R> fut,
            IgniteClosure<? super IgniteInternalFuture<R>, T> doneCb
        ) {
            this.fut = fut;
            this.doneCb = doneCb;

            fut.listen(new GridFutureChainListener<>(this, doneCb));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ChainFuture [orig=" + fut + ", doneCb=" + doneCb + ']';
        }
    }
}
