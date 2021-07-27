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

import java.util.Date;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.scheduler.SchedulerFuture;

/**  Wrapper processor which schedules given task using the appropriate scheduler */
public class IgniteScheduleCombinedProcessor extends IgniteScheduleProcessorAdapter {
    /** Cron based scheduler */
    private final IgniteScheduleProcessor igniteScheduleProcessor;

    /** Quartz based scheduler */
    private final IgniteScheduleQuartzProcessor igniteScheduleQuartzProcessor;

    /**
     * @param ctx Kernal context.
     */
    public IgniteScheduleCombinedProcessor(GridKernalContext ctx) {
        super(ctx);

        igniteScheduleProcessor = new IgniteScheduleProcessor(ctx);
        igniteScheduleQuartzProcessor = new IgniteScheduleQuartzProcessor(ctx);
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> schedule(Runnable c, String pattern) {
        return igniteScheduleProcessor.schedule(c, pattern);
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> schedule(Callable<R> c, String pattern) {
        return igniteScheduleProcessor.schedule(c, pattern);
    }

    /** {@inheritDoc} */
    @Override public SchedulerFuture<?> schedule(Runnable c, String jobName, Date startTime,
                                                 int repeatCount, long repeatIntervalInMS, int delayInSeconds) {
        return igniteScheduleQuartzProcessor.schedule(c, jobName, startTime,
                repeatCount, repeatIntervalInMS, delayInSeconds);
    }

    /** {@inheritDoc} */
    @Override public <R> SchedulerFuture<R> schedule(Callable<R> c, String jobName,
                                                     Date startTime, int repeatCount,
                                                     long repeatIntervalInMS, int delayInSeconds) {
        return igniteScheduleQuartzProcessor.schedule(c, jobName, startTime,
                repeatCount, repeatIntervalInMS, delayInSeconds);
    }

    /** {@inheritDoc} */
    @Override public void onScheduled(SchedulerFuture<?> fut) {
        igniteScheduleProcessor.onScheduled(fut);
        igniteScheduleQuartzProcessor.onScheduled(fut);
    }

    /** {@inheritDoc} */
    @Override public void onDescheduled(SchedulerFuture<?> fut) {
        igniteScheduleProcessor.onScheduled(fut);
        igniteScheduleQuartzProcessor.onScheduled(fut);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        igniteScheduleProcessor.start();
        igniteScheduleQuartzProcessor.start();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        igniteScheduleProcessor.stop(cancel);
        igniteScheduleQuartzProcessor.stop(cancel);
    }
}
