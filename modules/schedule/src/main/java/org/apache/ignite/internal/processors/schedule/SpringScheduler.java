/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.processors.schedule;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.jsr166.ConcurrentHashMap8;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronSequenceGenerator;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.util.StringUtils;

/**
 * Delegates scheduling to Spring {@link ThreadPoolTaskScheduler}
 */
public class SpringScheduler {

    /** Counter to generate Task ida. */
    private AtomicInteger cntr = new AtomicInteger();

    /** Spring Task scheduler implementation. */
    @GridToStringExclude
    private ThreadPoolTaskScheduler taskScheduler;

    /** Schedule futures. */
    private Map<Integer, ScheduledFuture<?>> schedFuts = new ConcurrentHashMap8<>();

    /** Scheduler state */
    private volatile boolean started;

    /**
     * Default constructor.
     */
    public SpringScheduler() {
        this.taskScheduler = new ThreadPoolTaskScheduler();
    }

    /**
     * @param cron pattern
     * @return if day of week is omitted in the pattern adds "?" to satisfy {@link CronSequenceGenerator} requirements
     */
    private static String addDoW(String cron) {
        String[] fields = StringUtils.tokenizeToStringArray(cron, " ");

        if (fields != null && fields.length == 5)
            return cron + " ?"; // add unspecified Day-of-Week
        else
            return cron;
    }

    /**
     * Start scheduler
     */
    public void start() {
        taskScheduler.setThreadNamePrefix("task-scheduler-#");
        taskScheduler.initialize();
        started = true;
    }

    /**
     * @return state of scheduler
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * Stop scheduler
     */
    public void stop() {
        started = false;
        taskScheduler.shutdown();
    }

    /**
     * @param cron expression
     * @param run scheduling code
     * @return task id
     * @throws IgniteException if cron expression is not valid or
     * if the given task was not accepted for internal reasons (e.g. a pool overload handling policy or a pool shutdown in progress)
     */
    public String schedule(String cron, Runnable run) throws IgniteException {
        try {
            CronTrigger trigger = new CronTrigger(addDoW(cron));

            ScheduledFuture<?> fut = taskScheduler.schedule(run, trigger);

            Integer id = this.cntr.incrementAndGet();

            schedFuts.put(id, fut);

            return id.toString();
        }
        catch (IllegalStateException | TaskRejectedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param id Task id to remove from the scheduler
     */
    public void deschedule(String id) {
        ScheduledFuture<?> fut = schedFuts.remove(Integer.valueOf(id));

        if (fut != null)
            fut.cancel(false);
    }

    /**
     * @param cron expression
     * @return true if expression is valid, otherwise false
     */
    public boolean isValid(String cron) {
        try {
            new CronSequenceGenerator(addDoW(cron));

            return true;
        }
        catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * @param cron expression
     * @throws IgniteException if cron expression is not valid
     */
    public void validate(String cron) throws IgniteException {
        try {
            new CronSequenceGenerator(addDoW(cron));
        }
        catch (IllegalArgumentException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param cron expression
     * @param cnt count of executions
     * @param start time in milliseconds
     * @return array long[cnt] of the next execition times in milliseconds
     * @throws IgniteException if cron expression is not valid
     */
    public long[] getNextExecutionTimes(String cron, int cnt,
        long start) throws IgniteException {
        long[] times = new long[cnt];

        try {
            CronSequenceGenerator cronExpr = new CronSequenceGenerator(addDoW(cron));

            Date date = new Date(start);

            for (int i = 0; i < cnt; i++) {
                date = cronExpr.next(date);

                times[i] = date.getTime();
            }
        }
        catch (IllegalArgumentException e) {
            throw new IgniteException(e);
        }
        return times;
    }

}
