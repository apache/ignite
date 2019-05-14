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
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointWriteProgressSupplier;

/**
 * Throttles threads that generate dirty pages during ongoing checkpoint.
 * Designed to avoid zero dropdowns that can happen if checkpoint buffer is overflowed.
 */
public class PagesWriteThrottle implements PagesWriteThrottlePolicy {
    /** Page memory. */
    private final PageMemoryImpl pageMemory;

    /** Database manager. */
    private final CheckpointWriteProgressSupplier cpProgress;

    /** Checkpoint lock state checker. */
    private CheckpointLockStateChecker stateChecker;

    /** Starting throttle time. Limits write speed to 1000 MB/s. */
    private static final long STARTING_THROTTLE_NANOS = 4000;

    /** Backoff ratio. Each next park will be this times longer. */
    private static final double BACKOFF_RATIO = 1.05;

    /** Exponential backoff counter. */
    private final AtomicInteger inCheckpointCntr = new AtomicInteger(0);

    /** Exponential backoff counter (for pages in checkpoint). */
    private final AtomicInteger notInCheckpointCntr = new AtomicInteger(0);

    /**
     * @param pageMemory Page memory.
     * @param cpProgress Database manager.
     * @param stateChecker checkpoint lock state checker.
     */
    public PagesWriteThrottle(PageMemoryImpl pageMemory,
        CheckpointWriteProgressSupplier cpProgress,
        CheckpointLockStateChecker stateChecker) {
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        this.stateChecker = stateChecker;
    }

    /** {@inheritDoc} */
    @Override public void onMarkDirty(boolean isPageInCheckpoint) {
        assert stateChecker.checkpointLockIsHeldByThread();

        AtomicInteger writtenPagesCntr = cpProgress.writtenPagesCounter();

        if (writtenPagesCntr == null)
            return; // Don't throttle if checkpoint is not running.

        boolean shouldThrottle = false;

        if (isPageInCheckpoint) {
            int checkpointBufLimit = pageMemory.checkpointBufferPagesSize() * 2 / 3;

            shouldThrottle = pageMemory.checkpointBufferPagesCount() > checkpointBufLimit;
        }

        if (!shouldThrottle) {
            int cpWrittenPages = writtenPagesCntr.get();

            int cpTotalPages = cpProgress.currentCheckpointPagesCount();

            if (cpWrittenPages == cpTotalPages) {
                // Checkpoint is already in fsync stage, increasing maximum ratio of dirty pages to 3/4
                shouldThrottle = pageMemory.shouldThrottle(3.0 / 4);
            }
            else {
                double dirtyRatioThreshold = ((double)cpWrittenPages) / cpTotalPages;

                // Starting with 0.05 to avoid throttle right after checkpoint start
                // 7/12 is maximum ratio of dirty pages
                dirtyRatioThreshold = (dirtyRatioThreshold * 0.95 + 0.05) * 7 / 12;

                shouldThrottle = pageMemory.shouldThrottle(dirtyRatioThreshold);
            }
        }

        AtomicInteger cntr = isPageInCheckpoint ? inCheckpointCntr : notInCheckpointCntr;

        if (shouldThrottle) {
            int throttleLevel = cntr.getAndIncrement();

            LockSupport.parkNanos((long)(STARTING_THROTTLE_NANOS * Math.pow(BACKOFF_RATIO, throttleLevel)));
        }
        else
            cntr.set(0);
    }

    /** {@inheritDoc} */
    @Override public void onBeginCheckpoint() {
    }

    /** {@inheritDoc} */
    @Override public void onFinishCheckpoint() {
        inCheckpointCntr.set(0);

        notInCheckpointCntr.set(0);
    }
}
