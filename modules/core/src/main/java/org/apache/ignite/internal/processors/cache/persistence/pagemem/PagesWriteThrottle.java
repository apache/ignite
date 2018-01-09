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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;

/**
 * Throttles threads that generate dirty pages during ongoing checkpoint.
 * Designed to avoid zero dropdowns that can happen if checkpoint buffer is overflowed.
 */
public class PagesWriteThrottle {
    /** Page memory. */
    private final PageMemoryImpl pageMemory;

    /** Database manager. */
    private final GridCacheDatabaseSharedManager dbSharedMgr;

    /** Starting throttle time. Limits write speed to 1000 MB/s. */
    private static final long STARTING_THROTTLE_NANOS = 4000;

    /** Backoff ratio. Each next park will be this times longer. */
    private static final double BACKOFF_RATIO = 1.05;

    /** Percent of dirty pages which will not cause throttling. */
    private static final double MIN_RATIO_NO_THROTTLE = 0.1;

    /** Exponential backoff counter. */
    private final AtomicInteger exponentialBackoffCntr = new AtomicInteger(0);

    /** Counter of written pages from checkpoint. Value is saved here for detecting checkpoint start. */
    private final AtomicInteger lastObservedWritten = new AtomicInteger(0);

    /**
     * Dirty pages ratio was observed at checkpoint start (here start is moment when first page was actually saved to
     * store). This ratio is excluded from throttling.
     */
    private volatile double initDirtyRatioAtCpBegin = MIN_RATIO_NO_THROTTLE;

    /**
     * @param pageMemory Page memory.
     * @param dbSharedMgr Database manager.
     */
    public PagesWriteThrottle(PageMemoryImpl pageMemory, GridCacheDatabaseSharedManager dbSharedMgr) {
        this.pageMemory = pageMemory;
        this.dbSharedMgr = dbSharedMgr;
    }

    /**
     * Callback to apply throttling delay.
     * @param isInCheckpoint flag indicating if checkpoint is running.
     */
    public void onMarkDirty(boolean isInCheckpoint) {
        assert dbSharedMgr.checkpointLockIsHeldByThread();

        AtomicInteger writtenPagesCntr = dbSharedMgr.writtenPagesCounter();

        if (writtenPagesCntr == null)
            return; // Don't throttle if checkpoint is not running.

        boolean shouldThrottle = false; //should apply delay (throttling) for current page modification

        if (isInCheckpoint) {
            int checkpointBufLimit = pageMemory.checkpointBufferPagesSize() * 2 / 3;

            shouldThrottle = pageMemory.checkpointBufferPagesCount() > checkpointBufLimit;
        }

        if (!shouldThrottle) {
            int cpWrittenPages = writtenPagesCntr.get();

            int cpTotalPages = dbSharedMgr.currentCheckpointPagesCount();

            if (cpWrittenPages == 0 || cpTotalPages == 0) {
                //probably slow start is running now, drop previous dirty page percent
                initDirtyRatioAtCpBegin = MIN_RATIO_NO_THROTTLE;
                lastObservedWritten.set(cpWrittenPages);
            }
            else if (cpWrittenPages == cpTotalPages) {
                // Checkpoint is already in fsync stage, increasing maximum ratio of dirty pages to 3/4
                shouldThrottle = pageMemory.shouldThrottle(3.0 / 4);
            }
            else {
                double dirtyRatioThreshold = ((double)cpWrittenPages) / cpTotalPages;

                boolean cpStartedToWrite = lastObservedWritten.compareAndSet(0, cpWrittenPages);

                if (cpStartedToWrite) {
                    double newMinRatio = pageMemory.getDirtyPagesRatio();

                    if (newMinRatio < MIN_RATIO_NO_THROTTLE)
                        newMinRatio = MIN_RATIO_NO_THROTTLE;

                    if (newMinRatio > 1)
                        newMinRatio = 1;

                    initDirtyRatioAtCpBegin = newMinRatio;
                }

                double throttleWeight = 1.0 - initDirtyRatioAtCpBegin;

                // Starting with initialDirtyRatioAtCpBegin to avoid throttle right after checkpoint start
                // 7/12 is maximum ratio of dirty pages
                dirtyRatioThreshold = (dirtyRatioThreshold * throttleWeight + initDirtyRatioAtCpBegin) * 7 / 12;

                shouldThrottle = pageMemory.shouldThrottle(dirtyRatioThreshold);
            }
        }


        if (shouldThrottle) {
            int throttleLevel = exponentialBackoffCntr.getAndIncrement();

            LockSupport.parkNanos((long)(STARTING_THROTTLE_NANOS * Math.pow(BACKOFF_RATIO, throttleLevel)));
        }
        else
            exponentialBackoffCntr.set(0);
    }

    /**
     *
     */
    public void onFinishCheckpoint() {
        exponentialBackoffCntr.set(0);
    }
}
