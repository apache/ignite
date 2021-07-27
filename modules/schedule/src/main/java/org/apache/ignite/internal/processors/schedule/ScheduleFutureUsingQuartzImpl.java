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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerBuilder;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

public class ScheduleFutureUsingQuartzImpl<R> extends ScheduleFutureBase<R> {
    /** Used to represent CountDownLatch in JobContext **/
    public static final String COUNTDOWNLATCH_KEY = "CountDownLatch";

    /** Used to represent ScheduleFuture in JobContext **/
    public static final String SCHEDULEFUTURE_KEY = "ScheduleFutureUsingQuartzImpl";

    /** Used to represent Task in JobContext **/
    public static final String TASK_KEY = "Task";

    /** Used to represent IgniteLogger in JobContext **/
    public static final String IGNITELOGGER_KEY = "IgniteLogger";

    private static final String TRIGGER_NAME = "StandardTrigger";

    /** Quartz scheduler. */
    @GridToStringExclude
    private Scheduler sched;

    /** Trigger */
    private SimpleTrigger trigger;

    /** Execution task. */
    @GridToStringExclude
    private Callable<R> task;

    /** Schedule info */
    private final IgniteScheduledJobInfo igniteScheduledJobInfo;

    /**
     * Creates descriptor for task scheduling. To start scheduling call {@link #schedule(Callable)}.
     *
     * @param sched Quartz scheduler
     * @param ctx Kernal context.
     * @param igniteScheduledJobInfo Scheduled job info
     */
    ScheduleFutureUsingQuartzImpl(Scheduler sched, GridKernalContext ctx,
                                  IgniteScheduledJobInfo igniteScheduledJobInfo) {
        super(ctx);

        assert sched != null;
        assert ctx != null;
        assert igniteScheduledJobInfo != null;

        this.sched = sched;
        this.ctx = ctx;
        this.igniteScheduledJobInfo = igniteScheduledJobInfo;

        maxCalls = igniteScheduledJobInfo.getRepeatCount();

        log = ctx.log(getClass());
    }

    /**
     * Initialization Method
     */
    public CountDownLatch onStart() {
        synchronized (mux) {
            if (done || cancelled)
                return null;

            if (stats.isRunning()) {
                U.warn(log, "Task got scheduled while previous was not finished: " + this);

                return null;
            }

            if (callCnt == igniteScheduledJobInfo.getRepeatCount() && igniteScheduledJobInfo.getRepeatCount() > 0)
                return null;

            callCnt++;

            stats.onStart();

            assert resLatch != null;

            return resLatch;
        }
    }

    /**
     * Sets execution task.
     *
     * @param task Execution task.
     */
    void schedule(Callable<R> task) {
        assert task != null;
        assert this.task == null;

        // Done future on this step means that there was error on init.
        if (isDone())
            return;

        this.task = task;

        SchedulerContext context = null;
        try {
            context = sched.getContext();
        } catch (SchedulerException e) {
            log.warning("Unable to get context");
            return;
        }

        assert context != null;

        context.put(COUNTDOWNLATCH_KEY, resLatch);
        context.put(SCHEDULEFUTURE_KEY, this);
        context.put(TASK_KEY, task);
        context.put(IGNITELOGGER_KEY, log);

        JobDetail job = JobBuilder.newJob(ExecutableJob.class)
                .withIdentity(igniteScheduledJobInfo.getJobName())
                .build();

        trigger = TriggerBuilder.newTrigger()
                .withIdentity(TRIGGER_NAME)
                .withSchedule(simpleSchedule()
                        .withIntervalInMilliseconds(igniteScheduledJobInfo.getRepeatIntervalInMS())
                        .withRepeatCount(igniteScheduledJobInfo.getRepeatCount()))
                .forJob(igniteScheduledJobInfo.getJobName())
                .build();

        (ctx.schedule()).onScheduled(this);

        if (igniteScheduledJobInfo.getDelayInSeconds() > 0) {
            try {
                sched.startDelayed(igniteScheduledJobInfo.getDelayInSeconds());
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        }

        try {
            sched.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    /**
     * De-schedules scheduled task.
     */
    @Override public void deschedule() {
        if (descheduled.compareAndSet(false, true)) {
            try {
                sched.deleteJob(new JobKey(igniteScheduledJobInfo.getJobName()));
            } catch (SchedulerException e) {
                log.error(e.getMessage());

                return;
            }

            (ctx.schedule()).onDescheduled(this);
        }
    }

    /** {@inheritDoc} */
    @Override public String pattern() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String id() {
        return igniteScheduledJobInfo.getJobName();
    }

    /** {@inheritDoc} */
    @Override public long[] nextExecutionTimes(int cnt, long start) {
        assert cnt > 0;
        assert start > 0;

        if (isDone() || isCancelled())
            return EMPTY_TIMES;

        synchronized (mux) {
            if (igniteScheduledJobInfo.getRepeatCount() > 0)
                cnt = Math.min(cnt, igniteScheduledJobInfo.getRepeatCount());
        }

        long[] times = new long[cnt];

        if (start < createTime() + igniteScheduledJobInfo.getDelayInSeconds() * 1000)
            start = createTime() + igniteScheduledJobInfo.getDelayInSeconds() * 1000;

        long current = start;

        for (int i = 0; i < cnt; i++) {
            current = current + igniteScheduledJobInfo.getRepeatIntervalInMS();
            times[i] = trigger.getFireTimeAfter(new Date(current)).getTime();
        }

        return times;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    @Override public <T> IgniteFuture<T> chain(final IgniteClosure<? super IgniteFuture<R>, T> doneCb) {
        A.notNull(doneCb, "doneCb");

        return chain(doneCb, null, ScheduleFutureUsingQuartzImpl.class.getName());
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> chainAsync(IgniteClosure<? super IgniteFuture<R>, T> doneCb, Executor exec) {
        A.notNull(doneCb, "");
        A.notNull(exec, "exec");

        return chain(doneCb, exec, ScheduleFutureUsingQuartzImpl.class.getName());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ScheduleFutureUsingQuartzImpl.class, this);
    }
}
