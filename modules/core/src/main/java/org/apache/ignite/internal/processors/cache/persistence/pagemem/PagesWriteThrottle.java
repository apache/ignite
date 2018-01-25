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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

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
     * Target (maximum) dirty pages ratio, after which throttling will start.
     */
    private volatile double targetDirtyRatio;


    /**
     * Current dirty pages ratio.
     */
    private volatile double currDirtyRatio;

    /**
     * Characteristic function: how close to throttle by size.
     * Shows how close current dirty ratio is to target (max) dirty ratio.
     */
    private volatile double throttleCloseMeasurement;

    //todo remove
    private volatile long lastDumpTime;

    private PagesWriteSpeedTracker speedMarkDirty = new PagesWriteSpeedTracker(100);
    private PagesWriteSpeedTracker speedCpWrite = new PagesWriteSpeedTracker();
    private volatile long lastEstimatedSpeedForMarkAll;

    private GridConcurrentHashSet<Long> threads = new GridConcurrentHashSet<>();

    /**
     * @param pageMemory Page memory.
     * @param dbSharedMgr Database manager.
     */
    public PagesWriteThrottle(PageMemoryImpl pageMemory,
        GridCacheDatabaseSharedManager dbSharedMgr) {
        this.pageMemory = pageMemory;
        this.dbSharedMgr = dbSharedMgr;
    }

    enum ThrottleLevel {
        NO, LIMITED, UNLIMITED
    }


    /**
     * Callback to apply throttling delay.
     * @param isPageInCheckpoint flag indicating if current page is in scope of current checkpoint.
     */
    public void onMarkDirty(boolean isPageInCheckpoint) {
        assert dbSharedMgr.checkpointLockIsHeldByThread();

        currDirtyRatio = pageMemory.getDirtyPagesRatio();

        AtomicInteger writtenPagesCntr = dbSharedMgr.writtenPagesCounter();

        if (writtenPagesCntr == null) {
            lastEstimatedSpeedForMarkAll = 0;
            throttleCloseMeasurement = 0;
            targetDirtyRatio = 1;

            return; // Don't throttle if checkpoint is not running.
        }


        AtomicInteger syncedPagesCounter = dbSharedMgr.syncedPagesCounter();

        int syncedPages = syncedPagesCounter == null ? 0 : syncedPagesCounter.get();
        int cpWrittenPages = writtenPagesCntr.get();

        int completedPages = cpWrittenPages + syncedPages;

        int fullyCompletedPages = completedPages / 2;
        speedCpWrite.setCounter(fullyCompletedPages);

        long currentTimeMillis = System.currentTimeMillis();

        long markDirtySpeed = speedMarkDirty.getSpeedPagesPerSec(currentTimeMillis);
        long curCpWriteSpeed = speedCpWrite.getSpeedPagesPerSec(currentTimeMillis);

        threads.add(Thread.currentThread().getId());
        int nThreads = threads.size();

        long throttleParkTime = 0;
        ThrottleLevel level = ThrottleLevel.NO; //should apply delay (throttling) for current page modification
        if (isPageInCheckpoint) {
            int checkpointBufLimit = pageMemory.checkpointBufferPagesSize() * 2 / 3;

            if(pageMemory.checkpointBufferPagesCount() > checkpointBufLimit)
                level = ThrottleLevel.UNLIMITED;
        }

        double timeRemainedSeconds = 0;
        long speedForMarkAll  = 0;
        if (level == ThrottleLevel.NO ) {

            int cpTotalPages = dbSharedMgr.currentCheckpointPagesCount();

            if (cpTotalPages == 0) {
                //probably slow start is running now, drop previous dirty page percent
                initDirtyRatioAtCpBegin = MIN_RATIO_NO_THROTTLE;
                lastObservedWritten.set(cpWrittenPages);

                boolean throttleByCpSpeed = curCpWriteSpeed > 0 && markDirtySpeed > curCpWriteSpeed;
                if (throttleByCpSpeed) {
                    level = ThrottleLevel.LIMITED;

                    throttleParkTime = calcDelayTime(curCpWriteSpeed, nThreads, 2);
                }
            }
            else {
                double dirtyPagesRatio = currDirtyRatio;
                if (cpWrittenPages > 0)
                    detectCpPagesWriteStart(cpWrittenPages, dirtyPagesRatio);

                double cpProgress = ((double)completedPages) / (2 * cpTotalPages);
                // Starting with initialDirtyRatioAtCpBegin to avoid throttle right after checkpoint start
                // .75 is maximum ratio of dirty pages
                double throttleTotalWeight = 1.0 - initDirtyRatioAtCpBegin;
                double dirtyRatioThreshold = (cpProgress * throttleTotalWeight + initDirtyRatioAtCpBegin) * 0.75;

                double clearPagesThreshold = 0.75 - dirtyPagesRatio;

                double remainedClear = clearPagesThreshold * pageMemory.totalPages();
                if (curCpWriteSpeed == 0) {
                    speedForMarkAll = 0;
                }
                else {
                    timeRemainedSeconds = 1.0 * (cpTotalPages - fullyCompletedPages) / curCpWriteSpeed;

                    speedForMarkAll = (long)(remainedClear / timeRemainedSeconds);

                    if (timeRemainedSeconds < 0.0001)
                        System.err.println(timeRemainedSeconds); //todo remove
                    if (speedForMarkAll > 230000)
                        System.err.println(speedForMarkAll); //todo remove
                }

                targetDirtyRatio = dirtyRatioThreshold;

                throttleCloseMeasurement = dirtyPagesRatio / dirtyRatioThreshold;

                boolean throttleBySize = dirtyPagesRatio > dirtyRatioThreshold
                    && (speedForMarkAll > 0 && markDirtySpeed > speedForMarkAll);
                boolean throttleByCpSpeed =
                    (curCpWriteSpeed > 0 && markDirtySpeed > curCpWriteSpeed);
                if ((throttleBySize || dirtyPagesRatio > 0.73) && throttleByCpSpeed)
                    level = ThrottleLevel.UNLIMITED;
                else if (throttleBySize || throttleByCpSpeed) {
                    level = ThrottleLevel.LIMITED;
                    if (throttleByCpSpeed) {
                        throttleParkTime = calcDelayTime(curCpWriteSpeed, nThreads, 2);
                    }
                    else
                        throttleParkTime = calcDelayTime(speedForMarkAll, nThreads, 2);
                }
            }
        }

        lastEstimatedSpeedForMarkAll = speedForMarkAll;

            //todo remove debug
        if(currentTimeMillis > lastDumpTime + 1000) {
            lastDumpTime = currentTimeMillis;

            System.err.println("CP write speed " + curCpWriteSpeed
                + " mark dirty speed " + markDirtySpeed
                + " exponent " + exponentialBackoffCntr.get()
                + " max sleep time, ns " + throttleParkTime
                + " timeRemainedSeconds=" + String.format("%.2f", timeRemainedSeconds)
                + " speedForMarkAll= " + speedForMarkAll);
        }

        if (level != ThrottleLevel.NO) {
            int exponent;
            if (level == ThrottleLevel.LIMITED) {
                LockSupport.parkNanos(throttleParkTime);
            }
            else {
                exponent = exponentialBackoffCntr.getAndIncrement();

                if (exponent != 0)
                    LockSupport.parkNanos((long)(STARTING_THROTTLE_NANOS * Math.pow(BACKOFF_RATIO, exponent)));
            }
        }
        else
            exponentialBackoffCntr.set(0);

        speedMarkDirty.incrementCounter();
    }

    private long calcDelayTime(long baseSpeed, int nThreads, double slowdownMultiplier) {
        if (slowdownMultiplier < 1.0)
            return 0;

        long maxThrottleTimeNs;
        if (baseSpeed == 0)
            maxThrottleTimeNs = TimeUnit.SECONDS.toNanos(1) * nThreads;
        else {
            long updTimeNsForOnePage = TimeUnit.SECONDS.toNanos(1) * nThreads / (baseSpeed);
            maxThrottleTimeNs = (long)((slowdownMultiplier - 1.0) * updTimeNsForOnePage);
        }
        return maxThrottleTimeNs;
    }

    private void detectCpPagesWriteStart(int cpWrittenPages, double dirtyPagesRatio) {
        boolean cpStartedToWrite = lastObservedWritten.compareAndSet(0, cpWrittenPages);
        if (cpStartedToWrite) {
            double newMinRatio = dirtyPagesRatio;

            if (newMinRatio < MIN_RATIO_NO_THROTTLE)
                newMinRatio = MIN_RATIO_NO_THROTTLE;

            if (newMinRatio > 1)
                newMinRatio = 1;

            initDirtyRatioAtCpBegin = newMinRatio;
        }
    }


    public void onBeginCheckpoint() {
        speedCpWrite.setCounter(0); //will create new measurement interval
    }

    /**
     *
     */
    public void onFinishCheckpoint() {
        exponentialBackoffCntr.set(0);

        speedCpWrite.finishInterval();
        speedMarkDirty.finishInterval();
        threads.clear();
    }

    /**
     *
     */
    public int throttleLevel() {
        return exponentialBackoffCntr.get();
    }

    public double getPageMemTargetDirtyRatio() {
        return targetDirtyRatio;
    }

    public double getCurrDirtyRatio() {
        return currDirtyRatio;
    }

    public double getThrottleCloseMeasurement() {
        return throttleCloseMeasurement;
    }


    public long getMarkDirtySpeed() {
        return speedMarkDirty.getSpeedPagesPerSecOptional();
    }

    public long getCpWriteSpeed() {
        return speedCpWrite.getSpeedPagesPerSecOptional();
    }

    public long getLastEstimatedSpeedForMarkAll() {
        return lastEstimatedSpeedForMarkAll;
    }
}
