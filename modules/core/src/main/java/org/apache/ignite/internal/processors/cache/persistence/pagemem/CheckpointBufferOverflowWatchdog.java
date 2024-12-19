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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

/**
 * Logic used to determine whether Checkpoint Buffer is in danger zone and writer threads should be throttled.
 */
class CheckpointBufferOverflowWatchdog {
    /** Page memory. */
    private final PageMemoryImpl pageMemory;

    /** Checkpoint buffer pages throttling threshold. */
    private final int cpBufPagesThrottlingThreshold;

    /** Checkpoint buffer pages danger bound. */
    private final int cpBufPagesDangerThreshold;

    /** Checkpoint buffer pages bound to wake up throttled threads. */
    private final int cpBufPagesThreadsWakeupThreshold;

    /**
     * Creates a new instance.
     *
     * @param pageMemory page memory to use
     * @param cpBufThrottlingThreshold Checkpoint buffer fulfill bound to start throttling.
     * @param cpBufDangerThreshold Checkpoint buffer danger fulfill bound.
     * @param cpBufThreadsWakeupThreshold Checkpoint buffer fulfill bound to wake up throttled threads.
     */
    CheckpointBufferOverflowWatchdog(
        PageMemoryImpl pageMemory,
        double cpBufThrottlingThreshold,
        double cpBufDangerThreshold,
        double cpBufThreadsWakeupThreshold
    ) {
        this.pageMemory = pageMemory;
        cpBufPagesThrottlingThreshold = (int)(pageMemory.checkpointBufferPagesSize() * cpBufThrottlingThreshold);
        cpBufPagesDangerThreshold = (int)(pageMemory.checkpointBufferPagesSize() * cpBufDangerThreshold);
        cpBufPagesThreadsWakeupThreshold = (int)(pageMemory.checkpointBufferPagesSize() * cpBufThreadsWakeupThreshold);
    }

    /**
     * Returns true if Checkpoint Buffer is in danger zone (more than danger bound of the buffer is filled)
     * and, hence, checkpoint pages writer should prioritize the pages in checkpoint buffer.
     *
     * @return {@code true} if Checkpoint Buffer is in danger zone
     */
    boolean isInDangerZone() {
        return pageMemory.checkpointBufferPagesCount() > cpBufPagesDangerThreshold;
    }

    /**
     * Returns true if Checkpoint Buffer is in throttling zone (more than throttling bound of the buffer is filled)
     * and, hence, writer threads need to be throttled.
     *
     * @return {@code true} if Checkpoint Buffer is in danger zone
     */
    boolean isInThrottlingZone() {
        return pageMemory.checkpointBufferPagesCount() > cpBufPagesThrottlingThreshold;
    }

    /**
     * @return Checkpoint buffer pages bound to wake up throttled threads.
     */
    int checkpointBufferThrottledThreadsWakeupThreshold() {
        return cpBufPagesThreadsWakeupThreshold;
    }

    /**
     * @return Checkpoint Buffer fill rate.
     */
    double fillRate() {
        return (double)pageMemory.checkpointBufferPagesCount() / pageMemory.checkpointBufferPagesSize();
    }
}
