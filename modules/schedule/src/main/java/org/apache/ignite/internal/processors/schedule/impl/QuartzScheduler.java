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

package org.apache.ignite.internal.processors.schedule.impl;

import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import org.apache.ignite.internal.processors.schedule.IScheduler;
import org.apache.ignite.internal.processors.schedule.exception.IgniteSchedulerException;
import org.apache.ignite.internal.processors.schedule.exception.IgniteSchedulerParseException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.utils.Key;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class QuartzScheduler implements IScheduler {
    /** Cron scheduler. */
    @GridToStringExclude
    private Scheduler sched;

    public QuartzScheduler() {
        try {
            sched = StdSchedulerFactory.getDefaultScheduler();
        }
        catch (SchedulerException e) {
            throw new IgniteSchedulerException(e);
        }
    }

    //TODO use regex to check DoW exists
    private static String toQuartzCron(String cron) {
        return cron + " ?"; // add unspecified Day-of-Week
    }

    @Override public void start() {
        try {
            sched.start();
        }
        catch (SchedulerException e) {
            throw new IgniteSchedulerException(e);
        }
    }

    @Override public boolean isStarted() {
        try {
            return sched.isStarted();
        }
        catch (SchedulerException e) {
            throw new IgniteSchedulerException(e);
        }
    }

    @Override public void stop() {
        try {
            sched.shutdown();
        }
        catch (SchedulerException e) {
            throw new IgniteSchedulerException(e);
        }
    }

    @Override public String schedule(String cron, Runnable run) throws IgniteSchedulerException {
        cron = toQuartzCron(cron);

        JobDetail jobDetail = newJob(DelegatingJob.class)
            .setJobData(new JobDataMap(Collections.singletonMap("runnable", run)))
            .build();

        String id = Key.createUniqueName(null);
        CronTrigger trigger = newTrigger()
            .withSchedule(cronSchedule(cron))
            .withIdentity(id)
            .build();

        try {
            sched.scheduleJob(jobDetail, trigger);
        }
        catch (SchedulerException e) {
            throw new IgniteSchedulerException(e);
        }
        return id;
    }

    @Override public void deschedule(String id) {
        try {
            sched.unscheduleJob(TriggerKey.triggerKey(id));
        }
        catch (SchedulerException e) {
            throw new IgniteSchedulerException(e);
        }
    }

    @Override public boolean isValid(String cron) {
        return CronExpression.isValidExpression(toQuartzCron(cron));
    }

    @Override public void validate(String cron) throws IgniteSchedulerParseException {
        try {
            CronExpression.validateExpression(toQuartzCron(cron));
        }
        catch (ParseException e) {
            throw new IgniteSchedulerParseException(e);
        }
    }

    @Override
    public long[] getNextExecutionTimes(String cron, int cnt, long start) throws IgniteSchedulerParseException {
        long[] times = new long[cnt];
        try {
            CronExpression cronExpr = new CronExpression(toQuartzCron(cron));
            Date date = new Date(start);
            for (int i = 0; i < cnt; i++) {
                date = cronExpr.getTimeAfter(date);
                times[i] = date.getTime();
            }
        }
        catch (ParseException e) {
            throw new IgniteSchedulerParseException(e);
        }
        return times;
    }
}

