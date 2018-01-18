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

import org.apache.ignite.internal.processors.schedule.exception.IgniteSchedulerException;
import org.apache.ignite.internal.processors.schedule.exception.IgniteSchedulerParseException;

/**
 *
 */
public interface IScheduler {
    /**
     * Start scheduler
     */
    public void start();

    /**
     * @return state of scheduler
     */
    public boolean isStarted();

    /**
     * Stop scheduler
     */
    public void stop();

    /**
     * @param cron expression
     * @param run scheduling code
     * @return task id
     * @throws IgniteSchedulerException if cron expression is not valid or
     * if the given task was not accepted for internal reasons (e.g. a pool overload handling policy or a pool shutdown in progress)
     */
    public String schedule(String cron, Runnable run) throws IgniteSchedulerException;

    /**
     * @param id Task id
     */
    public void deschedule(String id);

    /**
     * @param cron expression
     * @return true if expression is valid, otherwise false
     */
    public boolean isValid(String cron);

    /**
     * @param cron expression
     * @throws IgniteSchedulerParseException if cron expression is not valid
     */
    public void validate(String cron) throws IgniteSchedulerParseException;

    /**
     * @param cron expression
     * @param cnt count of executions
     * @param start time in milliseconds
     * @return array long[cnt] of the next execition times in milliseconds
     * @throws IgniteSchedulerParseException if cron expression is not valid
     */
    public long[] getNextExecutionTimes(String cron, int cnt, long start) throws IgniteSchedulerParseException;
}
