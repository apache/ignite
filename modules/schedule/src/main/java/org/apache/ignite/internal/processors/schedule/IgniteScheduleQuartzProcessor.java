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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.jetbrains.annotations.Nullable;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.Callable;

/** Scheduler which allows scheduling jobs using Quartz scheduler */
public class IgniteScheduleQuartzProcessor extends IgniteScheduleProcessorAdapter {
    /** Quartz scheduler. */
    private Scheduler sched;

    /** Schedule futures. */
    private Set<SchedulerFuture<?>> schedFuts = new GridConcurrentHashSet<>();

    /**
     * @param ctx Kernal context.
     */
    public IgniteScheduleQuartzProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> schedule(final Runnable c, String ptrn) {
        throw new NotImplementedException();
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> schedule(Callable<R> c, String pattern) {
        throw new NotImplementedException();
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> schedule(Runnable c, String jobName, Date startTime,
                                                 int repeatCount, long repeatIntervalInMS, int delayInSeconds) {
        assert c != null;
        assert jobName != null;
        assert startTime != null;

        IgniteScheduledJobInfo igniteScheduledJobInfo = new IgniteScheduledJobInfo(jobName, startTime,
                repeatCount, repeatIntervalInMS, delayInSeconds);

        ScheduleFutureUsingQuartzImpl<Object> fut = new ScheduleFutureUsingQuartzImpl<>(sched, ctx, igniteScheduledJobInfo);

        fut.schedule(new IgniteCallable<Object>() {
            @Nullable @Override public Object call() {
                c.run();

                return null;
            }
        });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> schedule(Callable<R> c, String jobName, Date startTime,
                                                     int repeatCount, long repeatIntervalInMS, int delayInSeconds) {
        assert c != null;
        assert jobName != null;
        assert startTime != null;

        IgniteScheduledJobInfo igniteScheduledJobInfo = new IgniteScheduledJobInfo(jobName, startTime,
                repeatCount, repeatIntervalInMS, delayInSeconds);

        ScheduleFutureUsingQuartzImpl<R> fut = new ScheduleFutureUsingQuartzImpl<>(sched, ctx, igniteScheduledJobInfo);

        fut.schedule(c);

        return fut;
    }

    /**
     *
     * @return Future objects of currently scheduled active(not finished) tasks.
     */
    public Collection<SchedulerFuture<?>> getScheduledFutures() {
        return Collections.unmodifiableList(new ArrayList<>(schedFuts));
    }

    /**
     * Removes future object from the collection of scheduled futures.
     *
     * @param fut Future object.
     */
    @Override public void onDescheduled(SchedulerFuture<?> fut) {
        assert fut != null;

        if (!(fut instanceof ScheduleFutureUsingQuartzImpl)) {
            return;
        }

        schedFuts.remove(fut);
    }

    /**
     * Adds future object to the collection of scheduled futures.
     *
     * @param fut Future object.
     */
    @Override public void onScheduled(SchedulerFuture<?> fut) {
        assert fut != null;

        if (!(fut instanceof ScheduleFutureUsingQuartzImpl)) {
            return;
        }

        schedFuts.add(fut);
    }

    /** {@inheritDoc} */
    @Override public void start() {
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();

        try {
            sched = schedulerFactory.getScheduler();
            sched.start();
        } catch (SchedulerException e) {
            log.error(e.getMessage());
            return;
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        try {
            if (sched.isStarted())
                sched.shutdown();
        } catch (SchedulerException e) {
            log.error(e.getMessage());
            return;
        }

        sched = null;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Schedule processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>   schedFutsSize: " + schedFuts.size());
    }
}
