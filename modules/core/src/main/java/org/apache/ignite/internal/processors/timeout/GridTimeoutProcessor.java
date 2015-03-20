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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.thread.*;

import java.util.*;

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
                long time1 = o1.endTime();
                long time2 = o2.endTime();

                return time1 < time2 ? -1 : time1 > time2 ? 1 : o1.timeoutId().compareTo(o2.timeoutId());
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
}
