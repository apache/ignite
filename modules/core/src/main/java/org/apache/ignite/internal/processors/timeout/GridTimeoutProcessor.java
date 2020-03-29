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

package org.apache.ignite.internal.processors.timeout;

import java.io.Closeable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 * Detects timeout events and processes them.
 */
public class GridTimeoutProcessor extends GridProcessorAdapter {
    /** */
    private final TimeoutWorker timeoutWorker;

    /** Time-based sorted set for timeout objects. */
    private final GridConcurrentSkipListSet<GridTimeoutObject> timeoutObjs =
        new GridConcurrentSkipListSet<>(new Comparator<GridTimeoutObject>() {
            /** {@inheritDoc} */
            @Override public int compare(GridTimeoutObject o1, GridTimeoutObject o2) {
                int res = Long.compare(o1.endTime(), o2.endTime());

                if (res != 0)
                    return res;

                return o1.timeoutId().compareTo(o2.timeoutId());
            }
        });

    /** */
    private final Object mux = new Object();

    /**
     * @param ctx Kernal context.
     */
    public GridTimeoutProcessor(GridKernalContext ctx) {
        super(ctx);

        timeoutWorker = new TimeoutWorker();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        new IgniteThread(timeoutWorker).start();

        if (log.isDebugEnabled())
            log.debug("Timeout processor started.");
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        timeoutWorker.cancel();
        U.join(timeoutWorker);

        if (log.isDebugEnabled())
            log.debug("Timeout processor stopped.");
    }

    /**
     * @param timeoutObj Timeout object.
     * @return {@code True} if object was added.
     */
    @SuppressWarnings({"NakedNotify", "CallToNotifyInsteadOfNotifyAll"})
    public boolean addTimeoutObject(GridTimeoutObject timeoutObj) {
        if (timeoutObj.endTime() <= 0 || timeoutObj.endTime() == Long.MAX_VALUE)
            // Timeout will never happen.
            return false;

        boolean added = timeoutObjs.add(timeoutObj);

        assert added : "Duplicate timeout object found: " + timeoutObj;

        if (timeoutObjs.firstx() == timeoutObj) {
            synchronized (mux) {
                mux.notify(); // No need to notifyAll since we only have one thread.
            }
        }

        return true;
    }

    /**
     * Schedule the specified timer task for execution at the specified
     * time with the specified period, in milliseconds.
     *
     * @param task Task to execute.
     * @param delay Delay to first execution in milliseconds.
     * @param period Period for execution in milliseconds or -1.
     * @return Cancelable to cancel task.
     */
    public CancelableTask schedule(Runnable task, long delay, long period) {
        assert delay >= 0 : delay;
        assert period > 0 || period == -1 : period;

        CancelableTask obj = new CancelableTask(task, U.currentTimeMillis() + delay, period);

        addTimeoutObject(obj);

        return obj;
    }

    /**
     * @param timeoutObj Timeout object.
     * @return {@code True} if timeout object was removed.
     */
    public boolean removeTimeoutObject(GridTimeoutObject timeoutObj) {
        return timeoutObjs.remove(timeoutObj);
    }

    /**
     * Wait for a future (listen with timeout).
     * @param fut Future.
     * @param timeout Timeout millis. -1 means expired timeout, 0 means waiting without timeout.
     * @param clo Finish closure. First argument contains error on future or null if no errors,
     * second is {@code true} if wait timed out or passed timeout argument means expired timeout.
     */
    public void waitAsync(final IgniteInternalFuture<?> fut,
        long timeout,
        IgniteBiInClosure<IgniteCheckedException, Boolean> clo) {
        if (timeout == -1) {
            clo.apply(null, true);

            return;
        }

        if (fut == null || fut.isDone())
            clo.apply(null, false);
        else {
            WaitFutureTimeoutObject timeoutObj = null;

            if (timeout > 0) {
                timeoutObj = new WaitFutureTimeoutObject(fut, timeout, clo);

                addTimeoutObject(timeoutObj);
            }

            final WaitFutureTimeoutObject finalTimeoutObj = timeoutObj;

            fut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> fut) {
                    if (finalTimeoutObj != null && !finalTimeoutObj.finishGuard.compareAndSet(false, true))
                        return;

                    try {
                        fut.get();

                        clo.apply(null, false);
                    }
                    catch (IgniteCheckedException e) {
                        clo.apply(e, false);
                    }
                    finally {
                        if (finalTimeoutObj != null)
                            removeTimeoutObject(finalTimeoutObj);
                    }
                }
            });
        }
    }

    /**
     * Handles job timeouts.
     */
    private class TimeoutWorker extends GridWorker {
        /**
         *
         */
        TimeoutWorker() {
            super(
                ctx.config().getIgniteInstanceName(),
                "grid-timeout-worker",
                GridTimeoutProcessor.this.log,
                ctx.workersRegistry()
            );
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            Throwable err = null;

            try {
                while (!isCancelled()) {
                    updateHeartbeat();

                    long now = U.currentTimeMillis();

                    onIdle();

                    for (Iterator<GridTimeoutObject> iter = timeoutObjs.iterator(); iter.hasNext(); ) {
                        GridTimeoutObject timeoutObj = iter.next();

                        if (timeoutObj.endTime() <= now) {
                            try {
                                boolean rmvd = timeoutObjs.remove(timeoutObj);

                                if (log.isDebugEnabled())
                                    log.debug("Timeout has occurred [obj=" + timeoutObj + ", process=" + rmvd + ']');

                                if (rmvd)
                                    timeoutObj.onTimeout();
                            }
                            catch (Throwable e) {
                                if (isCancelled() && !(e instanceof Error)) {
                                    if (log.isDebugEnabled())
                                        log.debug("Error when executing timeout callback: " + timeoutObj);

                                    return;
                                }

                                U.error(log, "Error when executing timeout callback: " + timeoutObj, e);

                                if (e instanceof Error)
                                    throw e;
                            }
                        }
                        else
                            break;
                    }

                    synchronized (mux) {
                        while (!isCancelled()) {
                            // Access of the first element must be inside of
                            // synchronization block, so we don't miss out
                            // on thread notification events sent from
                            // 'addTimeoutObject(..)' method.
                            GridTimeoutObject first = timeoutObjs.firstx();

                            if (first != null) {
                                long waitTime = first.endTime() - U.currentTimeMillis();

                                if (waitTime > 0) {
                                    blockingSectionBegin();

                                    try {
                                        mux.wait(waitTime);
                                    }
                                    finally {
                                        blockingSectionEnd();
                                    }
                                }
                                else
                                    break;
                            }
                            else {
                                blockingSectionBegin();

                                try {
                                    mux.wait(5000);
                                }
                                finally {
                                    blockingSectionEnd();
                                }
                            }
                        }
                    }
                }
            }
            catch (Throwable t) {
                if (!(t instanceof InterruptedException))
                    err = t;

                throw t;
            }
            finally {
                if (err == null && !isCancelled)
                    err = new IllegalStateException("Thread " + name() + " is terminated unexpectedly.");

                if (err instanceof OutOfMemoryError)
                    ctx.failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }

        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Timeout processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>   timeoutObjsSize: " + timeoutObjs.size());
    }

    /**
     *
     */
    public class CancelableTask implements GridTimeoutObject, Closeable {
        /** */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** */
        private long endTime;

        /** */
        private final long period;

        /** */
        private volatile boolean cancel;

        /** */
        @GridToStringInclude
        private final Runnable task;

        /**
         * @param task Task to execute.
         * @param firstTime First time.
         * @param period Period.
         */
        CancelableTask(Runnable task, long firstTime, long period) {
            this.task = task;
            endTime = firstTime;
            this.period = period;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public synchronized void onTimeout() {
            if (cancel)
                return;

            try {
                task.run();
            }
            finally {
                if (!cancel && period > 0) {
                    endTime = U.currentTimeMillis() + period;

                    addTimeoutObject(this);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void close() {
            cancel = true;

            synchronized (this) {
                // Just waiting for task execution end to make sure that task will not be executed anymore.
                removeTimeoutObject(this);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CancelableTask.class, this);
        }
    }

    /**
     *
     */
    private static class WaitFutureTimeoutObject extends GridTimeoutObjectAdapter {
        /** */
        private final IgniteInternalFuture<?> fut;

        /** */
        private final AtomicBoolean finishGuard = new AtomicBoolean();

        /** */
        private final IgniteBiInClosure<IgniteCheckedException, Boolean> clo;

        /**
         * @param fut Future.
         * @param timeout Timeout.
         * @param clo Closure to call on timeout.
         */
        WaitFutureTimeoutObject(IgniteInternalFuture<?> fut, long timeout,
            IgniteBiInClosure<IgniteCheckedException, Boolean> clo) {
            super(timeout);

            this.fut = fut;

            this.clo = clo;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (!fut.isDone() && finishGuard.compareAndSet(false, true))
                clo.apply(null, true);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(WaitFutureTimeoutObject.class, this);
        }
    }
}
