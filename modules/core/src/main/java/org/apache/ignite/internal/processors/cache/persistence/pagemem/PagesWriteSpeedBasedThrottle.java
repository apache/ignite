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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

/**
 * Throttles threads that generate dirty pages during ongoing checkpoint.
 * Designed to avoid zero dropdowns that can happen if checkpoint buffer is overflowed.
 * Uses average checkpoint write speed and moment speed of marking pages as dirty.
 */
public class PagesWriteSpeedBasedThrottle implements PagesWriteThrottlePolicy {
    /** Maximum dirty pages in region. */
    public static final double MAX_DIRTY_PAGES = 0.75;

    /** Page memory. */
    private final PageMemoryImpl pageMemory;

    /** Database manager. */
    private final GridCacheDatabaseSharedManager dbSharedMgr;

    /** Starting throttle time. Limits write speed to 1000 MB/s. */
    private static final long STARTING_THROTTLE_NANOS = 4000;

    /** Backoff ratio. Each next park will be this times longer. */
    private static final double BACKOFF_RATIO = 1.05;

    /** Percent of dirty pages which will not cause throttling. */
    private static final double MIN_RATIO_NO_THROTTLE = 0.03;

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
     * Target (maximum) dirty pages ratio, after which throttling will start using {@link #lastEstimatedSpeedForMarkAll}.
     */
    private volatile double targetDirtyRatio;

    /**
     * Current dirty pages ratio (percent of dirty pages in most used segment).
     */
    private volatile double currDirtyRatio;

    /**
     * Characteristic function: how close to throttle by size.
     * Shows how close current dirty ratio is to target (max) dirty ratio.
     */
    private volatile double throttleCloseMeasurement;

    /** Speed of marking pages dirty. Value from past 500-750 millis only. Pages/second. */
    private final PagesWriteSpeedTracker speedMarkDirty = new PagesWriteSpeedTracker(250, 2);

    /** Speed average checkpoint write speed. Current and 3 past checkpoints used. Pages/second. */
    private final PagesWriteSpeedTracker speedCpWrite = new PagesWriteSpeedTracker();

    /** Last estimated speed for marking all clear pages as dirty till the end of checkpoint. */
    private volatile long lastEstimatedSpeedForMarkAll;

    /** Threads set. Contains identifiers of all threads which were marking pages for current checkpoint. */
    private final GridConcurrentHashSet<Long> threadIds = new GridConcurrentHashSet<>();

    /**
     * @param pageMemory Page memory.
     * @param dbSharedMgr Database manager.
     */
    public PagesWriteSpeedBasedThrottle(PageMemoryImpl pageMemory,
        GridCacheDatabaseSharedManager dbSharedMgr) {
        this.pageMemory = pageMemory;
        this.dbSharedMgr = dbSharedMgr;
    }

    /** {@inheritDoc} */
    @Override public void onMarkDirty(boolean isPageInCheckpoint) {
        assert dbSharedMgr.checkpointLockIsHeldByThread();

        double dirtyPagesRatio = pageMemory.getDirtyPagesRatio();

        currDirtyRatio = dirtyPagesRatio;

        AtomicInteger writtenPagesCntr = dbSharedMgr.writtenPagesCounter();

        if (writtenPagesCntr == null) {
            lastEstimatedSpeedForMarkAll = 0;
            throttleCloseMeasurement = 0;
            targetDirtyRatio = 1;

            return; // Don't throttle if checkpoint is not running.
        }

        int cpWrittenPages = writtenPagesCntr.get();

        AtomicInteger syncedPagesCntr = dbSharedMgr.syncedPagesCounter();
        int cpSyncedPages = syncedPagesCntr == null ? 0 : syncedPagesCntr.get();

        long fullyCompletedPages = (cpWrittenPages + cpSyncedPages) / 2; // written & sync'ed

        long curMs = System.currentTimeMillis();

        speedCpWrite.setCounter(fullyCompletedPages, curMs);

        long markDirtySpeed = speedMarkDirty.getSpeedPagesPerSec(curMs);

        long curCpWriteSpeed = speedCpWrite.getSpeedPagesPerSec(curMs);

        threadIds.add(Thread.currentThread().getId());

        int nThreads = threadIds.size();

        ThrottleMode level = ThrottleMode.NO; //should apply delay (throttling) for current page modification
        if (isPageInCheckpoint) {
            int checkpointBufLimit = pageMemory.checkpointBufferPagesSize() * 2 / 3;

            if(pageMemory.checkpointBufferPagesCount() > checkpointBufLimit)
                level = ThrottleMode.EXPONENTIAL;
        }

        long speedForMarkAll  = 0;
        long throttleParkTimeNs = 0;
        if (level == ThrottleMode.NO ) {
            int cpTotalPages = dbSharedMgr.currentCheckpointPagesCount();

            if (cpTotalPages == 0) {
                boolean throttleByCpSpeed = curCpWriteSpeed > 0 && markDirtySpeed > curCpWriteSpeed;

                if (throttleByCpSpeed) {
                    level = ThrottleMode.LIMITED;

                    throttleParkTimeNs = calcDelayTime(curCpWriteSpeed, nThreads, 2);
                }
            }
            else {
                detectCpPagesWriteStart(cpWrittenPages, dirtyPagesRatio);

                speedForMarkAll = calcSpeedToMarkAllSpaceTillEndOfCp(dirtyPagesRatio,
                    fullyCompletedPages,
                    curCpWriteSpeed,
                    cpTotalPages);

                double targetDirtyRatio = calcTargetDirtyRatio(fullyCompletedPages, cpTotalPages);

                this.targetDirtyRatio = targetDirtyRatio;

                throttleCloseMeasurement = dirtyPagesRatio / targetDirtyRatio;


                boolean throttleByCpSpeed = curCpWriteSpeed > 0 && markDirtySpeed > (1.05 * curCpWriteSpeed);

                boolean markingTooFast = speedForMarkAll > 0 && markDirtySpeed > speedForMarkAll;
                boolean throttleBySizeAndMarkSpeed = dirtyPagesRatio > targetDirtyRatio && markingTooFast;

                boolean throttleBySizeAndSmallSpace = dirtyPagesRatio > targetDirtyRatio && (dirtyPagesRatio > 0.73);

                if ((throttleBySizeAndMarkSpeed || throttleBySizeAndSmallSpace) && throttleByCpSpeed)
                    level = ThrottleMode.EXPONENTIAL;
                else if (throttleByCpSpeed) {
                    level = ThrottleMode.LIMITED;

                    throttleParkTimeNs = calcDelayTime(curCpWriteSpeed, nThreads, 2);
                }
                else if (throttleBySizeAndMarkSpeed) {
                    level = ThrottleMode.LIMITED;

                    throttleParkTimeNs = calcDelayTime(speedForMarkAll, nThreads, 2);
                }
            }
        }

        lastEstimatedSpeedForMarkAll = speedForMarkAll;

        if (level == ThrottleMode.NO)
            exponentialBackoffCntr.set(0);
        else if (level == ThrottleMode.LIMITED && throttleParkTimeNs > 0)
            LockSupport.parkNanos(throttleParkTimeNs);
        else {
            int exponent = exponentialBackoffCntr.getAndIncrement();

            LockSupport.parkNanos((long)(STARTING_THROTTLE_NANOS * Math.pow(BACKOFF_RATIO, exponent)));
        }

        speedMarkDirty.incrementCounter();
    }

    /**
     * @param dirtyPagesRatio current percent of dirty pages.
     * @param fullyCompletedPages count of written and sync'ed pages
     * @param curCpWriteSpeed pages/second checkpoint write speed. 0 speed means 'no data'.
     * @param cpTotalPages total pages in checkpoint.
     * @return pages/second to mark to mark all clean pages as dirty till the end of checkpoint. 0 speed means 'no
     * data'.
     */
    private long calcSpeedToMarkAllSpaceTillEndOfCp(double dirtyPagesRatio,
        long fullyCompletedPages,
        long curCpWriteSpeed,
        int cpTotalPages) {

        if (curCpWriteSpeed == 0)
            return 0;

        double remainedClear = (MAX_DIRTY_PAGES - dirtyPagesRatio) * pageMemory.totalPages();

        double timeRemainedSeconds = 1.0 * (cpTotalPages - fullyCompletedPages) / curCpWriteSpeed;

        return (long)(remainedClear / timeRemainedSeconds);
    }

    /**
     * @param fullyCompletedPages number of completed.
     * @param cpTotalPages Total amount of pages under checkpoint.
     * @return size-based calculation of target ratio.
     */
    private double calcTargetDirtyRatio(long fullyCompletedPages, int cpTotalPages) {
        double cpProgress = ((double)fullyCompletedPages) / cpTotalPages;

        // Starting with initialDirtyRatioAtCpBegin to avoid throttle right after checkpoint start
        double constStart = initDirtyRatioAtCpBegin;

        double throttleTotalWeight = 1.0 - constStart;

        // .75 is maximum ratio of dirty pages
        return (cpProgress * throttleTotalWeight + constStart) * MAX_DIRTY_PAGES;
    }

    /**
     * @param baseSpeed speed to slow down.
     * @param nThreads operating threads.
     * @param slowdownMultiplier how much it is needed to slowdown base speed.
     * @return sleep time in nanoseconds.
     */
    private long calcDelayTime(long baseSpeed, int nThreads, double slowdownMultiplier) {
        if (slowdownMultiplier < 1.0)
            return 0;

        if (baseSpeed == 0)
            return TimeUnit.SECONDS.toNanos(1) * nThreads;

        long updTimeNsForOnePage = TimeUnit.SECONDS.toNanos(1) * nThreads / (baseSpeed);

        return (long)((slowdownMultiplier - 1.0) * updTimeNsForOnePage);
    }

    /**
     * @param cpWrittenPages current counter of written pages.
     * @param dirtyPagesRatio current percent of dirty pages.
     */
    private void detectCpPagesWriteStart(int cpWrittenPages, double dirtyPagesRatio) {
        if (cpWrittenPages > 0 && lastObservedWritten.compareAndSet(0, cpWrittenPages)) {
            double newMinRatio = dirtyPagesRatio;

            if (newMinRatio < MIN_RATIO_NO_THROTTLE)
                newMinRatio = MIN_RATIO_NO_THROTTLE;

            if (newMinRatio > 1)
                newMinRatio = 1;

            //for slow cp is completed now, drop previous dirty page percent
            initDirtyRatioAtCpBegin = newMinRatio;
        }
    }

    /** {@inheritDoc} */
    @Override public void onBeginCheckpoint() {
        long curMs = System.currentTimeMillis();

        speedCpWrite.setCounter(0L, curMs);

        initDirtyRatioAtCpBegin = MIN_RATIO_NO_THROTTLE;

        lastObservedWritten.set(0);
    }


    /** {@inheritDoc} */
    @Override public void onFinishCheckpoint() {
        exponentialBackoffCntr.set(0);

        speedCpWrite.finishInterval();
        speedMarkDirty.finishInterval();
        threadIds.clear();
    }

    /**
     * @return Exponential backoff counter.
     */
    public int throttleLevel() {
        return exponentialBackoffCntr.get();
    }

    /**
     * @return Target (maximum) dirty pages ratio, after which throttling will start.
     */
    public double getTargetDirtyRatio() {
        return targetDirtyRatio;
    }

    /**
     * @return Current dirty pages ratio.
     */
    public double getCurrDirtyRatio() {
        return currDirtyRatio;
    }

    /**
     * @return Returns {@link #throttleCloseMeasurement}.
     */
    public double getThrottleCloseMeasurement() {
        return throttleCloseMeasurement;
    }

    /**
     * @return  Speed of marking pages dirty. Value from past 500-750 millis only. Pages/second.
     */
    public long getMarkDirtySpeed() {
        return speedMarkDirty.getSpeedPagesPerSecOptional();
    }

    /**
     * @return Speed average checkpoint write speed. Current and 3 past checkpoints used. Pages/second.
     */
    public long getCpWriteSpeed() {
        return speedCpWrite.getSpeedPagesPerSecOptional();
    }

    /**
     * @return Returns {@link #lastEstimatedSpeedForMarkAll}.
     */
    public long getLastEstimatedSpeedForMarkAll() {
        return lastEstimatedSpeedForMarkAll;
    }

    /**
     * Throttling mode for page.
     */
    private enum ThrottleMode {
        /** No delay is applied. */
        NO,

        /** Limited, time is based on target speed. */
        LIMITED,

        /** Exponential. */
        EXPONENTIAL
    }
}
