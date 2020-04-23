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

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Statistic of baseline auto-adjust.
 */
public class BaselineAutoAdjustStatus {
    /** Timeout of task of baseline adjust. */
    private final long timeUntilAutoAdjust;

    /** State of baseline adjust task. */
    private final TaskState taskState;

    /**
     * @param timeout Timeout of baseline adjust.
     */
    private BaselineAutoAdjustStatus(TaskState state, long timeout) {
        timeUntilAutoAdjust = timeout;
        taskState = state;
    }

    /**
     * @param timeout Timeout of task of baseline adjust.
     * @return Created statistic object.
     */
    public static BaselineAutoAdjustStatus scheduled(long timeout) {
        return new BaselineAutoAdjustStatus(TaskState.SCHEDULED, timeout);
    }

    /**
     * @return Created statistic object with 'in progress' status.
     */
    public static BaselineAutoAdjustStatus inProgress() {
        return new BaselineAutoAdjustStatus(TaskState.IN_PROGRESS, -1);
    }

    /**
     * @return Created statistic object with 'not scheduled' status.
     */
    public static BaselineAutoAdjustStatus notScheduled() {
        return new BaselineAutoAdjustStatus(TaskState.NOT_SCHEDULED, -1);
    }

    /**
     * @return Timeout of task of baseline adjust.
     */
    public long getTimeUntilAutoAdjust() {
        return timeUntilAutoAdjust;
    }

    /**
     * @return State of baseline adjust task.
     */
    public TaskState getTaskState() {
        return taskState;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BaselineAutoAdjustStatus.class, this);
    }

    /**
     *
     */
    public enum TaskState {
        /**
         * Baseline is changing now.
         */
        IN_PROGRESS,
        /**
         * Task to baseline adjust was scheduled.
         */
        SCHEDULED,
        /**
         * Task to baseline adjust was not scheduled.
         */
        NOT_SCHEDULED;
    }
}
