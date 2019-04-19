/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

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
