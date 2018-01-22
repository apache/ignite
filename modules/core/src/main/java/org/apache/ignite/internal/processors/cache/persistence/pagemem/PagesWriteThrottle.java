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
import java.util.function.IntBinaryOperator;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;

/**
 * Throttles threads that generate dirty pages during ongoing checkpoint.
 * Designed to avoid zero dropdowns that can happen if checkpoint buffer is overflowed.
 */
public class PagesWriteThrottle {
    /** Page memory. */
    private final PageMemoryImpl pageMemory;

    /** Database manager. */
    private final GridCacheDatabaseSharedManager dbSharedMgr;
    private FileWriteAheadLogManager wal;
    private DataStorageConfiguration dsCfg;

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
     * Debug only field, used for tests by reflection.
     */
    private volatile double lastDirtyRatioThreshold;

    private volatile double pageMemThrottleRatio;

    /**
     * @param pageMemory Page memory.
     * @param dbSharedMgr Database manager.
     * @param wal
     * @param dsCfg
     */
    public PagesWriteThrottle(PageMemoryImpl pageMemory,
        GridCacheDatabaseSharedManager dbSharedMgr,
        FileWriteAheadLogManager wal, DataStorageConfiguration dsCfg) {
        this.pageMemory = pageMemory;
        this.dbSharedMgr = dbSharedMgr;
        this.wal = wal;
        this.dsCfg = dsCfg;
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
                double threshold = 3.0 / 4;

                lastDirtyRatioThreshold = threshold;

                shouldThrottle = pageMemory.shouldThrottle(threshold);
            }
            else {
                double dirtyRatioThreshold = ((double)cpWrittenPages) / cpTotalPages;

                boolean cpStartedToWrite = lastObservedWritten.compareAndSet(0, cpWrittenPages);

                double dirtyPagesRatio = pageMemory.getDirtyPagesRatio();
                if (cpStartedToWrite) {
                    double newMinRatio = dirtyPagesRatio;

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

                lastDirtyRatioThreshold = dirtyRatioThreshold;

                pageMemThrottleRatio = dirtyPagesRatio / dirtyRatioThreshold;

                shouldThrottle = dirtyPagesRatio > dirtyRatioThreshold;
            }
        }

        if(!shouldThrottle) {
            FileWALPointer pointer = wal.currentWritePointer();

            long lastArchIdx = wal.lastAbsArchivedIdx();

            int maxSegments = dsCfg.getWalSegments();
            if (lastArchIdx >= 0 && maxSegments > 1) {
                long segSize = wal.maxWalSegmentSize();
                long curWorkIdx = pointer.index();

                long seqInWork = curWorkIdx - lastArchIdx;
                long segRemained = maxSegments - seqInWork;
                long bytesInCurSeq = segSize - pointer.fileOffset();

                long maxBytesInWorkDir = maxSegments * segSize;
                long bytesRemained = segRemained * segSize + bytesInCurSeq;

                //todo test throttle
                shouldThrottle = 1.0 * bytesRemained < 0.2 * maxBytesInWorkDir;
            }
        }

        if (shouldThrottle) {
            int throttleLevel = exponentialBackoffCntr.getAndIncrement();

            LockSupport.parkNanos((long)(STARTING_THROTTLE_NANOS * Math.pow(BACKOFF_RATIO, throttleLevel)));
        }
        else {
            int newLevel = exponentialBackoffCntr.accumulateAndGet(-1, (left, right) -> {
                int sum = left + right;

                return sum > 0 ? sum : 0;
            });

            if (newLevel > 0)
                LockSupport.parkNanos((long)(STARTING_THROTTLE_NANOS * Math.pow(BACKOFF_RATIO, newLevel)));
            //todo: return or remove: exponentialBackoffCntr.set(0);
        }
    }

    /**
     *
     */
    public void onFinishCheckpoint() {
        exponentialBackoffCntr.set(0);
    }

    /**
     *
     */
    public int throttleLevel() {
        return exponentialBackoffCntr.get();
    }

    public double getPageMemThrottleRatio() {
        return pageMemThrottleRatio;
    }
}
