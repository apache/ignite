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
    private final CheckpointLockStateChecker stateChecker;

    /** In-checkpoint protection logic. */
    private final ExponentialBackoffThrottlingStrategy inCheckpointProtection
        = new ExponentialBackoffThrottlingStrategy();

    /** Not-in-checkpoint protection logic. */
    private final ExponentialBackoffThrottlingStrategy notInCheckpointProtection
        = new ExponentialBackoffThrottlingStrategy();

    /** Checkpoint Buffer-related logic used to keep it safe. */
    private final CheckpointBufferOverflowWatchdog cpBufferWatchdog;

    /** Logger. */
    private final IgniteLogger log;

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
        cpBufferWatchdog = new CheckpointBufferOverflowWatchdog(pageMemory);
        this.log = log;

        assert throttleOnlyPagesInCheckpoint || cpProgress != null
                : "cpProgress must be not null if ratio based throttling mode is used";
    }

    /** {@inheritDoc} */
    @Override public void onMarkDirty(boolean isPageInCheckpoint) {
        assert stateChecker.checkpointLockIsHeldByThread();

        boolean shouldThrottle = false;

        if (isPageInCheckpoint)
            shouldThrottle = isCpBufferOverflowThresholdExceeded();

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
            }
            else {
                double dirtyRatioThreshold = ((double)cpWrittenPages) / cpTotalPages;

                // Starting with 0.05 to avoid throttle right after checkpoint start
                // 7/12 is maximum ratio of dirty pages
                dirtyRatioThreshold = (dirtyRatioThreshold * 0.95 + 0.05) * 7 / 12;

                shouldThrottle = pageMemory.shouldThrottle(dirtyRatioThreshold);
            }
        }

        ExponentialBackoffThrottlingStrategy exponentialThrottle = isPageInCheckpoint
                ? inCheckpointProtection : notInCheckpointProtection;

        if (shouldThrottle) {
            long throttleParkTimeNs = exponentialThrottle.protectionParkTime();

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
            boolean backoffWasAlreadyStarted = exponentialThrottle.resetBackoff();

            if (isPageInCheckpoint && backoffWasAlreadyStarted)
                unparkParkedThreads();
        }
    }

    /** {@inheritDoc} */
    @Override public void wakeupThrottledThreads() {
        if (!isCpBufferOverflowThresholdExceeded()) {
            inCheckpointProtection.resetBackoff();

            unparkParkedThreads();
        }
    }

    /**
     * Unparks all the threads that were parked by us.
     */
    private void unparkParkedThreads() {
        cpBufThrottledThreads.values().forEach(LockSupport::unpark);
    }

    /** {@inheritDoc} */
    @Override public void onBeginCheckpoint() {
    }

    /** {@inheritDoc} */
    @Override public void onFinishCheckpoint() {
        inCheckpointProtection.resetBackoff();
        notInCheckpointProtection.resetBackoff();
    }

    /** {@inheritDoc} */
    @Override public boolean isCpBufferOverflowThresholdExceeded() {
        return cpBufferWatchdog.isInDangerZone();
    }
}
