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

/**
 * Metadata for a job scheduled by an Ignite scheduler
 */
public class IgniteScheduledJobInfo {
    /** Job name */
    private final String jobName;

    /** Start time */
    private final Date startTime;

    /** Repeat count */
    private final int repeatCount;

    /** Repeat interval */
    private final long repeatInterval;

    /** Delay */
    private final int delay;

    public IgniteScheduledJobInfo(String jobName, Date startTime, int repeatCount, long repeatInterval, int delay) {
        this.jobName = jobName;
        this.startTime = startTime;
        this.repeatCount = repeatCount;
        this.repeatInterval = repeatInterval;
        this.delay = delay;
    }

    /** Get the job name */
    public String getJobName() {
        return jobName;
    }

    /** Get the start time */
    public Date getStartTime() {
        return startTime;
    }

    /** Get the repeat count */
    public int getRepeatCount() {
        return repeatCount;
    }

    /** Get the repeat interval */
    public long getRepeatInterval() {
        return repeatInterval;
    }

    /** Get delay */
    public int getDelay() {
        return delay;
    }
}
