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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import static org.apache.ignite.internal.processors.schedule.ScheduleFutureUsingQuartzImpl.COUNTDOWNLATCH_KEY;
import static org.apache.ignite.internal.processors.schedule.ScheduleFutureUsingQuartzImpl.IGNITELOGGER_KEY;
import static org.apache.ignite.internal.processors.schedule.ScheduleFutureUsingQuartzImpl.SCHEDULEFUTURE_KEY;
import static org.apache.ignite.internal.processors.schedule.ScheduleFutureUsingQuartzImpl.TASK_KEY;

/**
 * Represents a Quartz job which is scheduled using the Quartz scheduler
 */
public class ExecutableJob<R> implements Job {

    /** Main execution function */
    @Override public void execute(JobExecutionContext jobExecutionContext) {
        SchedulerContext context;

        try {
            context = jobExecutionContext.getScheduler().getContext();
        } catch (SchedulerException e) {
            return;
        }

        assert context != null;

        CountDownLatch countDownLatch = (CountDownLatch) context.get(COUNTDOWNLATCH_KEY);
        ScheduleFutureUsingQuartzImpl scheduleFutureUsingQuartzImpl= (ScheduleFutureUsingQuartzImpl) context.get(SCHEDULEFUTURE_KEY);
        Callable task = (Callable) context.get(TASK_KEY);
        IgniteLogger log = (IgniteLogger) context.get(IGNITELOGGER_KEY);

        run(countDownLatch, scheduleFutureUsingQuartzImpl, task, log);
    }

    /** Execute a given task */
    private void run(CountDownLatch resLatch, ScheduleFutureUsingQuartzImpl<R> scheduleFutureUsingQuartz,
                     Callable<R> task, IgniteLogger log) {

        if (resLatch == null || scheduleFutureUsingQuartz == null || task == null || log == null)
            return;

        scheduleFutureUsingQuartz.onStart();

        R res = null;

        Throwable err = null;

        try {
            res = task.call();
        }
        catch (Exception e) {
            err = e;
        }
        catch (Error e) {
            err = e;

            U.error(log, "Error occurred while executing scheduled task: " + this, e);
        }
        finally {
            if (!scheduleFutureUsingQuartz.onEnd(resLatch, res, err, false))
                scheduleFutureUsingQuartz.deschedule();
        }
    }
}
