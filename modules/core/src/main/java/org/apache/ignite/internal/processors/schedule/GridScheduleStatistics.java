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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Scheduled task statistics.
 */
class GridScheduleStatistics {
    /** Create time. */
    private final long createTime = U.currentTimeMillis();

    /** Last idle time. */
    private long lastIdleTime;

    /** Total idle time. */
    private long totalIdleTime;

    /** Last execution time. */
    private long lastExecTime;

    /** Total execution time. */
    private long totalExecTime;

    /** Last start time. */
    private long lastStartTime;

    /** Last end time. */
    private long lastEndTime;

    /** Execution count. */
    private int execCnt;

    /** Running flag. */
    private boolean running;

    /**
     * @return Create time.
     */
    long getCreateTime() {
        return createTime;
    }

    /**
     * @return Last idle time.
     */
    long getLastIdleTime() {
        return lastIdleTime == 0 ? U.currentTimeMillis() - createTime : lastIdleTime;
    }

    /**
     * @return Total idle time.
     */
    long getTotalIdleTime() {
        long now = U.currentTimeMillis();

        if (totalIdleTime == 0)
            return now - createTime;

        if (running)
            return totalIdleTime;

        return totalIdleTime + (now - lastEndTime);
    }

    /**
     * @return Average idle time.
     */
    double getAverageIdleTime() {
        long now = U.currentTimeMillis();

        // If first execution has not started.
        if (totalIdleTime == 0)
            return now - createTime;

        return execCnt == 0 ? totalIdleTime : (double)totalIdleTime / execCnt;
    }

    /**
     * @return Last execution time.
     */
    long getLastExecutionTime() {
        return lastExecTime;
    }

    /**
     * @return Total execution time.
     */
    long getTotalExecutionTime() {
        return totalExecTime;
    }

    /**
     * @return Average execution time.
     */
    double getAverageExecutionTime() {
        return execCnt == 0 ? 0 : (double)totalExecTime / execCnt;
    }

    /**
     * @return Last start time.
     */
    long getLastStartTime() {
        return lastStartTime;
    }

    /**
     * @return Last end time.
     */
    long getLastEndTime() {
        return lastEndTime;
    }

    /**
     * @return Execution count.
     */
    int getExecutionCount() {
        return execCnt;
    }

    /**
     * @return {@code True} if currently running.
     */
    boolean isRunning() {
        return running;
    }

    /**
     * Start callback.
     */
    void onStart() {
        long now = U.currentTimeMillis();

        lastStartTime = now;

        long lastEndTime = this.lastEndTime == 0 ? createTime : this.lastEndTime;

        lastIdleTime = now - lastEndTime;

        totalIdleTime += lastIdleTime;

        running = true;
    }

    /**
     * End callback.
     */
    void onEnd() {
        long now = U.currentTimeMillis();

        lastEndTime = now;

        lastExecTime = now - lastStartTime;

        totalExecTime += lastExecTime;

        execCnt++;

        running = false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridScheduleStatistics.class, this);
    }
}