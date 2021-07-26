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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.scheduler.SchedulerFuture;

/**
 * Schedules cron-based or Quartz execution of grid tasks and closures. Abstract class was introduced to
 * avoid mandatory runtime dependency on the libraries.
 */
public abstract class IgniteScheduleProcessorAdapter extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    protected IgniteScheduleProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param c Closure to schedule to run as a background cron-based job.
     * @param pattern Scheduling pattern in UNIX cron format with prefix "{n1, n2} " where n1 is delay of scheduling
     *      and n2 is the number of task calls.
     * @return Descriptor of the scheduled execution.
     */
    public abstract SchedulerFuture<?> schedule(final Runnable c, String pattern);

    /**
     * @param c Closure to schedule to run as a background cron-based job.
     * @param pattern Scheduling pattern in UNIX cron format with prefix "{n1, n2} " where n1 is delay of scheduling
     *      and n2 is the number of task calls.
     * @return Descriptor of the scheduled execution.
     */
    public abstract <R> SchedulerFuture<R> schedule(Callable<R> c, String pattern);

    /**
     * @param c Closure to schedule to run as a background cron-based job.
     * @param jobName Name of the job
     * @param startTime Start time of the job
     * @param repeatCount Repeat count of the job
     * @param repeatInterval Repeat interval of the job
     * @param delay Delay in execution of the job
     * @return Descriptor of the scheduled execution.
     */
    public abstract SchedulerFuture<?> schedule(final Runnable c, String jobName, Date startTime,
                                                int repeatCount, long repeatInterval, int delay);

    /**
     * @param c Closure to schedule to run as a background cron-based job.
     * @param jobName Name of the job
     * @param startTime Start time of the job
     * @param repeatCount Repeat count of the job
     * @param repeatInterval Repeat interval of the job
     * @param delay Delay in execution of the job
     * @return Descriptor of the scheduled execution.
     */
    public abstract <R> SchedulerFuture<R> schedule(Callable<R> c, String jobName, Date startTime,
                                                    int repeatCount, long repeatInterval, int delay);


    /**
     *
     * @param fut Future to use for scheduling
     */
    public abstract void onScheduled(SchedulerFuture<?> fut);

    /**
     *
     * @param fut Future to use for scheduling
     */
    public abstract void onDescheduled(SchedulerFuture<?> fut);
}
