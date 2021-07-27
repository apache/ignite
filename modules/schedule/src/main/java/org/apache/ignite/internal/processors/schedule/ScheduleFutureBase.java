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
package org.apache.ignite.internal.processors.schedule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.future.AsyncFutureListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/** Base class for all ScheduleFuture classes */
public abstract class ScheduleFutureBase<R> implements SchedulerFuture<R> {
    /** Empty time array. */
    static final long[] EMPTY_TIMES = new long[] {};

    /** No next execution time constant. **/
    static final long NO_NEXT_EXECUTION_TIME = 0;

    /** Number of maximum task calls parsed from pattern. */
    int maxCalls;

    /** Cancelled flag. */
    boolean cancelled;

    /** Done flag. */
    boolean done;

    /** Task calls counter. */
    int callCnt;

    /** De-schedule flag. */
    final AtomicBoolean descheduled = new AtomicBoolean(false);

    /** Listeners. */
    Collection<IgniteInClosure<? super IgniteFuture<R>>> lsnrs = new ArrayList<>(1);

    /** Statistics. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    GridScheduleStatistics stats = new GridScheduleStatistics();

    /** Latch synchronizing fetch of the next execution result. */
    @GridToStringExclude
    CountDownLatch resLatch = new CountDownLatch(1);

    /** Result of the last execution of scheduled task. */
    @GridToStringExclude
    R lastRes;

    /** Keeps last execution exception or {@code null} if the last execution was successful. */
    @GridToStringExclude
    Throwable lastErr;

    /** Listener call count. */
    int lastLsnrExecCnt;

    /** Mutex. */
    final Object mux = new Object();

    /** Grid logger. */
    IgniteLogger log;

    /** Processor registry. */
    @GridToStringExclude
    GridKernalContext ctx;

    /** Creates descriptor for task scheduling. To start scheduling call schedule on child class */
    ScheduleFutureBase(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;
        log = ctx.log(getClass());
    }

    /**
     * @param latch Latch.
     * @param res Result.
     * @param err Error.
     * @param initErr Init error flag.
     * @return {@code False} if future should be unscheduled.
     */
    protected boolean onEnd(CountDownLatch latch, R res, Throwable err, boolean initErr) {
        assert latch != null;

        boolean notifyLsnr = false;

        CountDownLatch resLatchCp = null;

        try {
            synchronized (mux) {
                lastRes = res;
                lastErr = err;

                if (initErr) {
                    assert err != null;

                    notifyLsnr = true;
                }
                else {
                    stats.onEnd();

                    int cnt = stats.getExecutionCount();

                    if (lastLsnrExecCnt != cnt) {
                        notifyLsnr = true;

                        lastLsnrExecCnt = cnt;
                    }
                }

                if ((callCnt == maxCalls && maxCalls > 0) || cancelled || initErr) {
                    done = true;

                    resLatchCp = resLatch;

                    resLatch = null;

                    return false;
                }

                resLatch = new CountDownLatch(1);

                return true;
            }
        }
        finally {
            // Unblock all get() invocations.
            latch.countDown();

            // Make sure that none will be blocked on new latch if this
            // future will not be executed any more.
            if (resLatchCp != null)
                resLatchCp.countDown();

            if (notifyLsnr)
                notifyListeners(res, err);
        }
    }

    /**
     * @param lsnr Listener to notify.
     * @param res Last execution result.
     * @param err Last execution error.
     */
    void notifyListener(final IgniteInClosure<? super IgniteFuture<R>> lsnr, R res, Throwable err) {
        assert lsnr != null;
        assert !Thread.holdsLock(mux);
        assert ctx != null;

        lsnr.apply(snapshot(res, err));
    }

    /**
     * @param res Last execution result.
     * @param err Last execution error.
     */
    protected void notifyListeners(R res, Throwable err) {
        final Collection<IgniteInClosure<? super IgniteFuture<R>>> tmp;

        synchronized (mux) {
            tmp = new ArrayList<>(lsnrs);
        }

        final SchedulerFuture<R> snapshot = snapshot(res, err);

        for (IgniteInClosure<? super IgniteFuture<R>> lsnr : tmp)
            lsnr.apply(snapshot);
    }

    /** {@inheritDoc} */
    @Override public long nextExecutionTime() {
        long[] execTimes = nextExecutionTimes(1, U.currentTimeMillis());

        return execTimes == EMPTY_TIMES ? NO_NEXT_EXECUTION_TIME : execTimes[0];
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        synchronized (mux) {
            if (done)
                return false;

            if (cancelled)
                return true;

            if (!stats.isRunning())
                done = true;

            cancelled = true;
        }

        deschedule();

        return true;
    }

    /**
     * Checks that the future is in valid state for get operation.
     *
     * @return Latch or {@code null} if future has been finished.
     * @throws IgniteFutureCancelledException If was cancelled.
     */
    @Nullable CountDownLatch ensureGet() throws IgniteFutureCancelledException {
        synchronized (mux) {
            if (cancelled)
                throw new IgniteFutureCancelledException("Scheduling has been cancelled: " + this);

            if (done)
                return null;

            return resLatch;
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public R get() {
        CountDownLatch latch = ensureGet();

        if (latch != null) {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (isCancelled())
                    throw new IgniteFutureCancelledException(e);

                if (isDone())
                    return last();

                throw new IgniteInterruptedException(e);
            }
        }

        return last();
    }

    /** {@inheritDoc} */
    @Override public R get(long timeout) {
        return get(timeout, MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Nullable @Override public R get(long timeout, TimeUnit unit) throws IgniteException {
        CountDownLatch latch = ensureGet();

        if (latch != null) {
            try {
                if (latch.await(timeout, unit))
                    return last();
                else
                    throw new IgniteFutureTimeoutException("Timed out waiting for completion of next " +
                            "scheduled computation: " + this);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (isCancelled())
                    throw new IgniteFutureCancelledException(e);

                if (isDone())
                    return last();

                throw new IgniteInterruptedException(e);
            }
        }

        return last();
    }

    /** {@inheritDoc} */
    @Override public long createTime() {
        synchronized (mux) {
            return stats.getCreateTime();
        }
    }

    /** {@inheritDoc} */
    @Override public long lastStartTime() {
        synchronized (mux) {
            return stats.getLastStartTime();
        }
    }

    /** {@inheritDoc} */
    @Override public long lastFinishTime() {
        synchronized (mux) {
            return stats.getLastEndTime();
        }
    }

    /** {@inheritDoc} */
    @Override public double averageExecutionTime() {
        synchronized (mux) {
            return stats.getLastExecutionTime();
        }
    }

    /** {@inheritDoc} */
    @Override public long lastIdleTime() {
        synchronized (mux) {
            return stats.getLastIdleTime();
        }
    }

    /** {@inheritDoc} */
    @Override public double averageIdleTime() {
        synchronized (mux) {
            return stats.getAverageIdleTime();
        }
    }

    /** {@inheritDoc} */
    @Override public int count() {
        synchronized (mux) {
            return stats.getExecutionCount();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isRunning() {
        synchronized (mux) {
            return stats.isRunning();
        }
    }

    /** {@inheritDoc} */
    @Override public R last() throws IgniteException {
        synchronized (mux) {
            if (lastErr != null)
                throw U.convertException(U.cast(lastErr));

            return lastRes;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        synchronized (mux) {
            return cancelled;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        synchronized (mux) {
            return done;
        }
    }

    /** {@inheritDoc} */
    @Override public void listen(IgniteInClosure<? super IgniteFuture<R>> lsnr) {
        A.notNull(lsnr, "lsnr");

        Throwable err;
        R res;

        boolean notifyLsnr = false;

        synchronized (mux) {
            lsnrs.add(lsnr);

            err = lastErr;
            res = lastRes;

            int cnt = stats.getExecutionCount();

            if (cnt > 0 && lastLsnrExecCnt != cnt) {
                lastLsnrExecCnt = cnt;

                notifyLsnr = true;
            }
        }

        // Avoid race condition in case if listener was added after
        // first execution completed.
        if (notifyLsnr)
            notifyListener(lsnr, res, err);
    }

    /** {@inheritDoc} */
    @Override public void listenAsync(IgniteInClosure<? super IgniteFuture<R>> lsnr, Executor exec) {
        A.notNull(lsnr, "lsnr");
        A.notNull(exec, "exec");

        listen(new AsyncFutureListener<>(lsnr, exec));
    }

    /**
     * @param doneCb Done callback.
     * @param exec Executor.
     * @param className Class name of the child class
     * @return Chained future.
     */
    protected <T> IgniteFuture<T> chain(final IgniteClosure<? super IgniteFuture<R>, T> doneCb, @Nullable Executor exec,
                                         String className) {
        final GridFutureAdapter<T> fut = new GridFutureAdapter<T>() {
            @Override public String toString() {
                return "ChainFuture[orig=" + className + ", doneCb=" + doneCb + ']';
            }
        };

        IgniteInClosure<? super IgniteFuture<R>> lsnr = new CI1<IgniteFuture<R>>() {
            @Override public void apply(IgniteFuture<R> fut0) {
                try {
                    fut.onDone(doneCb.apply(fut0));
                }
                catch (GridClosureException e) {
                    fut.onDone(e.unwrap());
                }
                catch (IgniteException e) {
                    fut.onDone(e);
                }
                catch (RuntimeException | Error e) {
                    U.warn(null, "Failed to notify chained future (is grid stopped?) [igniteInstanceName=" +
                            ctx.igniteInstanceName() + ", doneCb=" + doneCb + ", err=" + e.getMessage() + ']');

                    fut.onDone(e);

                    throw e;
                }
            }
        };

        if (exec != null)
            lsnr = new AsyncFutureListener<>(lsnr, exec);

        listen(lsnr);

        return new IgniteFutureImpl<>(fut);
    }

    /**
     * Creates a snapshot of this future with fixed last result.
     *
     * @param res Last result.
     * @param err Last error.
     * @return Future snapshot.
     */
    private SchedulerFuture<R> snapshot(R res, Throwable err) {
        return new ScheduleFutureSnapshot<R>(this, res, err);
    }

    /**
     * Future snapshot.
     *
     * @param <R>
     */
    protected static class ScheduleFutureSnapshot<R> implements SchedulerFuture<R> {
        /** */
        private ScheduleFutureBase<R> ref;

        /** */
        private R res;

        /** */
        private Throwable err;

        /**
         *
         * @param ref Referenced implementation.
         * @param res Last result.
         * @param err Throwable.
         */
        ScheduleFutureSnapshot(ScheduleFutureBase<R> ref, R res, Throwable err) {
            assert ref != null;

            this.ref = ref;
            this.res = res;
            this.err = err;
        }

        /** {@inheritDoc} */
        @Override public R last() {
            if (err != null)
                throw U.convertException(U.cast(err));

            return res;
        }

        /** {@inheritDoc} */
        @Override public String id() {
            return ref.id();
        }

        /** {@inheritDoc} */
        @Override public String pattern() {
            return ref.pattern();
        }

        /** {@inheritDoc} */
        @Override public long createTime() {
            return ref.createTime();
        }

        /** {@inheritDoc} */
        @Override public long lastStartTime() {
            return ref.lastStartTime();
        }

        /** {@inheritDoc} */
        @Override public long lastFinishTime() {
            return ref.lastFinishTime();
        }

        /** {@inheritDoc} */
        @Override public double averageExecutionTime() {
            return ref.averageExecutionTime();
        }

        /** {@inheritDoc} */
        @Override public long lastIdleTime() {
            return ref.lastIdleTime();
        }

        /** {@inheritDoc} */
        @Override public double averageIdleTime() {
            return ref.averageIdleTime();
        }

        /** {@inheritDoc} */
        @Override public long[] nextExecutionTimes(int cnt, long start) {
            return ref.nextExecutionTimes(cnt, start);
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return ref.count();
        }

        /** {@inheritDoc} */
        @Override public boolean isRunning() {
            return ref.isRunning();
        }

        /** {@inheritDoc} */
        @Override public long nextExecutionTime() {
            return ref.nextExecutionTime();
        }

        /** {@inheritDoc} */
        @Nullable
        @Override public R get() {
            return ref.get();
        }

        /** {@inheritDoc} */
        @Override public R get(long timeout) {
            return ref.get(timeout);
        }

        /** {@inheritDoc} */
        @Nullable @Override public R get(long timeout, TimeUnit unit) {
            return ref.get(timeout, unit);
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return ref.cancel();
        }

        /** {@inheritDoc} */
        @Override public boolean isDone() {
            return ref.isDone();
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return ref.isCancelled();
        }

        /** {@inheritDoc} */
        @Override public void listen(IgniteInClosure<? super IgniteFuture<R>> lsnr) {
            ref.listen(lsnr);
        }

        /** {@inheritDoc} */
        @Override public void listenAsync(IgniteInClosure<? super IgniteFuture<R>> lsnr, Executor exec) {
            ref.listenAsync(lsnr, exec);
        }

        /** {@inheritDoc} */
        @Override public <T> IgniteFuture<T> chain(IgniteClosure<? super IgniteFuture<R>, T> doneCb) {
            return ref.chain(doneCb);
        }

        /** {@inheritDoc} */
        @Override public <T> IgniteFuture<T> chainAsync(IgniteClosure<? super IgniteFuture<R>, T> doneCb,
                                                        Executor exec) {
            return ref.chainAsync(doneCb, exec);
        }
    }

    /**
     * Deschedule a scheduled task
     */
    public abstract void deschedule();
}
