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

import it.sauronsoftware.cron4j.InvalidPatternException;
import it.sauronsoftware.cron4j.Predictor;
import it.sauronsoftware.cron4j.Scheduler;
import it.sauronsoftware.cron4j.SchedulingPattern;
import org.apache.ignite.internal.processors.schedule.IScheduler;
import org.apache.ignite.internal.processors.schedule.exception.IgniteSchedulerException;
import org.apache.ignite.internal.processors.schedule.exception.IgniteSchedulerParseException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;

@Deprecated
public class Cron4JScheduler implements IScheduler {
    /** Cron scheduler. */
    @GridToStringExclude
    private Scheduler sched;

    public Cron4JScheduler() {
        this.sched = new Scheduler();
    }

    @Override public void start() {
        sched.start();
    }

    @Override public boolean isStarted() {
        return sched.isStarted();
    }

    @Override public void stop() {
        sched.stop();
    }

    @Override public String schedule(String cron, Runnable run) throws IgniteSchedulerException {
        try {
            return sched.schedule(cron, run);
        }
        catch (InvalidPatternException e) {
            throw new IgniteSchedulerException(e);
        }
    }

    @Override public void deschedule(String id) {
        sched.deschedule(id);
    }

    @Override public boolean isValid(String cron) {
        return SchedulingPattern.validate(cron);
    }

    @Override public void validate(String cron) throws IgniteSchedulerParseException {
        try {
            new SchedulingPattern(cron);
        }
        catch (InvalidPatternException e) {
            throw new IgniteSchedulerParseException(e);
        }
    }

    @Override
    public long[] getNextExecutionTimes(String cron, int cnt, long start) throws IgniteSchedulerParseException {
        long[] times = new long[cnt];

        try {
            SchedulingPattern ptrn = new SchedulingPattern(cron);
            Predictor p = new Predictor(ptrn, start);

            for (int i = 0; i < cnt; i++)
                times[i] = p.nextMatchingTime();
        }
        catch (InvalidPatternException e) {
            throw new IgniteSchedulerParseException(e);
        }

        return times;
    }
}
