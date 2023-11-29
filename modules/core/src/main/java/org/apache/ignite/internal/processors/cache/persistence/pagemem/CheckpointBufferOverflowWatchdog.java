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

    /** Checkpoint buffer fullfill bound to start throttling. */
    private final double cpBufFillThreshold;

    /**
     * Creates a new instance.
     *
     * @param pageMemory page memory to use
     */
    CheckpointBufferOverflowWatchdog(PageMemoryImpl pageMemory, double cpBufFillThreshold) {
        this.pageMemory = pageMemory;
        this.cpBufFillThreshold = cpBufFillThreshold;
    }

    /**
     * Returns true if Checkpoint Buffer is in danger zone (more than
     * {@link PagesWriteThrottlePolicy#CP_BUF_FILL_THRESHOLD} of the buffer is filled) and, hence, writer threads need
     * to be throttled.
     *
     * @return {@code true} if Checkpoint Buffer is in danger zone
     */
    boolean isInDangerZone() {
        int checkpointBufLimit = (int)(pageMemory.checkpointBufferPagesSize() * cpBufFillThreshold);

        return pageMemory.checkpointBufferPagesCount() > checkpointBufLimit;
    }

    /**
     * @return Checkpoint Buffer fill rate.
     */
    double fillRate() {
        return (double)pageMemory.checkpointBufferPagesCount() / pageMemory.checkpointBufferPagesSize();
    }
}
