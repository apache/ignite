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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;

/**
 * Throttles threads that generate dirty pages during ongoing checkpoint.
 * Designed to avoid zero dropdowns that can happen if checkpoint buffer is overflowed.
 */
public class PagesWriteThrottle implements PagesWriteThrottlePolicy {
    /** Page memory. */
    private final PageMemoryImpl pageMemory;

    /** Database manager. */
    private final IgniteOutClosure<CheckpointProgress> cpProgress;

    /** If true, throttle will only protect from checkpoint buffer overflow, not from dirty pages ratio cap excess. */
    private final boolean throttleOnlyPagesInCheckpoint;

    /** Checkpoint lock state checker. */
    private CheckpointLockStateChecker stateChecker;

    /** Starting throttle time. Limits write speed to 1000 MB/s. */
    private static final long STARTING_THROTTLE_NANOS = 4000;

    /** Backoff ratio. Each next park will be this times longer. */
    private static final double BACKOFF_RATIO = 1.05;

    /** Checkpoint buffer fullfill upper bound. */
    private static final float CP_BUF_FILL_THRESHOLD = 2f / 3;

    /** Counter for dirty pages ratio throttling. */
    private final AtomicInteger notInCheckpointBackoffCntr = new AtomicInteger(0);

    /** Counter for checkpoint buffer usage ratio throttling (we need a separate one due to IGNITE-7751). */
    private final AtomicInteger inCheckpointBackoffCntr = new AtomicInteger(0);

    /** Logger. */
    private IgniteLogger log;

    /** Threads that are throttled due to checkpoint buffer overflow. */
    private final ConcurrentHashMap<Long, Thread> cpBufThrottledThreads = new ConcurrentHashMap<>();

    /**
     * @param pageMemory Page memory.
     * @param cpProgress Database manager.
     * @param stateChecker checkpoint lock state checker.
     * @param throttleOnlyPagesInCheckpoint If true, throttle will only protect from checkpoint buffer overflow.
     * @param log Logger.
     */
    public PagesWriteThrottle(PageMemoryImpl pageMemory,
        IgniteOutClosure<CheckpointProgress> cpProgress,
        CheckpointLockStateChecker stateChecker,
        boolean throttleOnlyPagesInCheckpoint,
        IgniteLogger log
    ) {
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        this.stateChecker = stateChecker;
        this.throttleOnlyPagesInCheckpoint = throttleOnlyPagesInCheckpoint;
        this.log = log;

        assert throttleOnlyPagesInCheckpoint || cpProgress != null : "cpProgress must be not null if ratio based throttling mode is used";
    }

    /** {@inheritDoc} */
    @Override public void onMarkDirty(boolean isPageInCheckpoint) {
        assert stateChecker.checkpointLockIsHeldByThread();

        boolean shouldThrottle = false;

        if (isPageInCheckpoint)
            shouldThrottle = shouldThrottle();

        if (!shouldThrottle && !throttleOnlyPagesInCheckpoint) {
            CheckpointProgress progress = cpProgress.apply();

            AtomicInteger writtenPagesCntr = progress == null ? null : cpProgress.apply().writtenPagesCounter();

            if (progress == null || writtenPagesCntr == null)
                return; // Don't throttle if checkpoint is not running.

            int cpWrittenPages = writtenPagesCntr.get();

            int cpTotalPages = progress.currentCheckpointPagesCount();

            if (cpWrittenPages == cpTotalPages) {
                // Checkpoint is already in fsync stage, increasing maximum ratio of dirty pages to 3/4
                shouldThrottle = pageMemory.shouldThrottle(3.0 / 4);
            } else {
                double dirtyRatioThreshold = ((double)cpWrittenPages) / cpTotalPages;

                // Starting with 0.05 to avoid throttle right after checkpoint start
                // 7/12 is maximum ratio of dirty pages
                dirtyRatioThreshold = (dirtyRatioThreshold * 0.95 + 0.05) * 7 / 12;

                shouldThrottle = pageMemory.shouldThrottle(dirtyRatioThreshold);
            }
        }

        AtomicInteger cntr = isPageInCheckpoint ? inCheckpointBackoffCntr : notInCheckpointBackoffCntr;

        if (shouldThrottle) {
            int throttleLevel = cntr.getAndIncrement();

            long throttleParkTimeNs = (long) (STARTING_THROTTLE_NANOS * Math.pow(BACKOFF_RATIO, throttleLevel));

            Thread curThread = Thread.currentThread();

            if (throttleParkTimeNs > LOGGING_THRESHOLD) {
                U.warn(log, "Parking thread=" + curThread.getName()
                    + " for timeout(ms)=" + (throttleParkTimeNs / 1_000_000));
            }

            long startTime = U.currentTimeMillis();

            if (isPageInCheckpoint) {
                cpBufThrottledThreads.put(curThread.getId(), curThread);

                try {
                    LockSupport.parkNanos(throttleParkTimeNs);
                }
                finally {
                    cpBufThrottledThreads.remove(curThread.getId());

                    if (throttleParkTimeNs > LOGGING_THRESHOLD) {
                        U.warn(log, "Unparking thread=" + curThread.getName()
                            + " with park timeout(ms)=" + (throttleParkTimeNs / 1_000_000));
                    }
                }
            }
            else
                LockSupport.parkNanos(throttleParkTimeNs);

            pageMemory.metrics().addThrottlingTime(U.currentTimeMillis() - startTime);
        }
        else {
            int oldCntr = cntr.getAndSet(0);

            if (isPageInCheckpoint && oldCntr != 0)
                cpBufThrottledThreads.values().forEach(LockSupport::unpark);
        }
    }

    /** {@inheritDoc} */
    @Override public void tryWakeupThrottledThreads() {
        if (!shouldThrottle()) {
            inCheckpointBackoffCntr.set(0);

            cpBufThrottledThreads.values().forEach(LockSupport::unpark);
        }
    }

    /** {@inheritDoc} */
    @Override public void onBeginCheckpoint() {
    }

    /** {@inheritDoc} */
    @Override public void onFinishCheckpoint() {
        inCheckpointBackoffCntr.set(0);

        notInCheckpointBackoffCntr.set(0);
    }

    /** {@inheritDoc} */
    @Override public boolean shouldThrottle() {
        int checkpointBufLimit = (int)(pageMemory.checkpointBufferPagesSize() * CP_BUF_FILL_THRESHOLD);

        return pageMemory.checkpointBufferPagesCount() > checkpointBufLimit;
    }
}
