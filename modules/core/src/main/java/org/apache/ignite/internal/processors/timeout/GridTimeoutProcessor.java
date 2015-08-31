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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;

/**
 * Detects timeout events and processes them.
 */
public class GridTimeoutProcessor extends GridProcessorAdapter {
    /** */
    private final IgniteThread timeoutWorker;

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

        timeoutWorker = new IgniteThread(ctx.config().getGridName(), "grid-timeout-worker",
            new TimeoutWorker());
    }

    /** {@inheritDoc} */
    @Override public void start() {
        timeoutWorker.start();

        if (log.isDebugEnabled())
            log.debug("Timeout processor started.");
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        U.interrupt(timeoutWorker);
        U.join(timeoutWorker);

        if (log.isDebugEnabled())
            log.debug("Timeout processor stopped.");
    }

    /**
     * @param timeoutObj Timeout object.
     */
    @SuppressWarnings({"NakedNotify", "CallToNotifyInsteadOfNotifyAll"})
    public void addTimeoutObject(GridTimeoutObject timeoutObj) {
        if (timeoutObj.endTime() <= 0 || timeoutObj.endTime() == Long.MAX_VALUE)
            // Timeout will never happen.
            return;

        boolean added = timeoutObjs.add(timeoutObj);

        assert added : "Duplicate timeout object found: " + timeoutObj;

        if (timeoutObjs.firstx() == timeoutObj) {
            synchronized (mux) {
                mux.notify(); // No need to notifyAll since we only have one thread.
            }
        }
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
     */
    public void removeTimeoutObject(GridTimeoutObject timeoutObj) {
        timeoutObjs.remove(timeoutObj);
    }

    /**
     * Handles job timeouts.
     */
    private class TimeoutWorker extends GridWorker {
        /**
         *
         */
        TimeoutWorker() {
            super(ctx.config().getGridName(), "grid-timeout-worker", GridTimeoutProcessor.this.log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            while (!isCancelled()) {
                long now = U.currentTimeMillis();

                for (Iterator<GridTimeoutObject> iter = timeoutObjs.iterator(); iter.hasNext();) {
                    GridTimeoutObject timeoutObj = iter.next();

                    if (timeoutObj.endTime() <= now) {
                        iter.remove();

                        if (log.isDebugEnabled())
                            log.debug("Timeout has occurred: " + timeoutObj);

                        try {
                            timeoutObj.onTimeout();
                        }
                        catch (Throwable e) {
                            U.error(log, "Error when executing timeout callback: " + timeoutObj, e);

                            if (e instanceof Error)
                                throw e;
                        }
                    }
                    else
                        break;
                }

                synchronized (mux) {
                    while (true) {
                        // Access of the first element must be inside of
                        // synchronization block, so we don't miss out
                        // on thread notification events sent from
                        // 'addTimeoutObject(..)' method.
                        GridTimeoutObject first = timeoutObjs.firstx();

                        if (first != null) {
                            long waitTime = first.endTime() - U.currentTimeMillis();

                            if (waitTime > 0)
                                mux.wait(waitTime);
                            else
                                break;
                        }
                        else
                            mux.wait(5000);
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Timeout processor memory stats [grid=" + ctx.gridName() + ']');
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
}