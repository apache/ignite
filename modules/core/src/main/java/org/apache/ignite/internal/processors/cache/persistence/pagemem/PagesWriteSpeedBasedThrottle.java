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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;

/**
 * Throttles threads that generate dirty pages during ongoing checkpoint.
 * Designed to avoid zero dropdowns that can happen if checkpoint buffer is overflowed.
 * Uses average checkpoint write speed and moment speed of marking pages as dirty.<br>
 *
 * See also: <a href="https://github.com/apache/ignite/tree/master/modules/core/src/main/java/org/apache/ignite/internal/processors/cache/persistence/pagemem#speed-based-throttling">Speed-based throttling description</a>.
 */
public class PagesWriteSpeedBasedThrottle implements PagesWriteThrottlePolicy {
    /** Maximum dirty pages in region. */
    private static final double MAX_DIRTY_PAGES = 0.75;

    /** Page memory. */
    private final PageMemoryImpl pageMemory;

    /** Database manager. */
    private final IgniteOutClosure<CheckpointProgress> cpProgress;

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
     * Target (maximum) dirty pages ratio, after which throttling will start using
     * {@link #getParkTime(double, long, int, int, long, long)}.
     */
    private volatile double targetDirtyRatio;

    /**
     * Current dirty pages ratio (percent of dirty pages in most used segment), negative value means no cp is running.
     */
    private volatile double currDirtyRatio;

    /** Speed average checkpoint write speed. Current and 3 past checkpoints used. Pages/second. */
    private final IntervalBasedMeasurement speedCpWrite = new IntervalBasedMeasurement();

    /** Last estimated speed for marking all clear pages as dirty till the end of checkpoint. */
    private volatile long speedForMarkAll;

    /** Threads set. Contains identifiers of all threads which were marking pages for current checkpoint. */
    private final GridConcurrentHashSet<Long> threadIds = new GridConcurrentHashSet<>();

    /**
     * Used for calculating speed of marking pages dirty.
     * Value from past 750-1000 millis only.
     * {@link IntervalBasedMeasurement#getSpeedOpsPerSec(long)} returns pages marked/second.
     * {@link IntervalBasedMeasurement#getAverage()} returns average throttle time.
     * */
    private final IntervalBasedMeasurement speedMarkAndAvgParkTime = new IntervalBasedMeasurement(250, 3);

    /** Total pages which is possible to store in page memory. */
    private long totalPages;

    /** Checkpoint lock state provider. */
    private CheckpointLockStateChecker cpLockStateChecker;

    /** Logger. */
    private IgniteLogger log;

    /** Previous warning time, nanos. */
    private AtomicLong prevWarnTime = new AtomicLong();

    /** Warning min delay nanoseconds. */
    private static final long WARN_MIN_DELAY_NS = TimeUnit.SECONDS.toNanos(10);

    /** Warning threshold: minimal level of pressure that causes warning messages to log. */
    static final double WARN_THRESHOLD = 0.2;

    /**
     * @param pageMemory Page memory.
     * @param cpProgress Database manager.
     * @param stateChecker Checkpoint lock state provider.
     * @param log Logger.
     */
    public PagesWriteSpeedBasedThrottle(
            PageMemoryImpl pageMemory,
            IgniteOutClosure<CheckpointProgress> cpProgress,
            CheckpointLockStateChecker stateChecker,
            IgniteLogger log
    ) {
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        totalPages = pageMemory.totalPages();
        this.cpLockStateChecker = stateChecker;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void onMarkDirty(boolean isPageInCheckpoint) {
        assert cpLockStateChecker.checkpointLockIsHeldByThread();

        CheckpointProgress progress = cpProgress.apply();

        AtomicInteger writtenPagesCntr = progress == null ? null : cpProgress.apply().writtenPagesCounter();

        if (writtenPagesCntr == null) {
            speedForMarkAll = 0;
            targetDirtyRatio = -1;
            currDirtyRatio = -1;

            return; // Don't throttle if checkpoint is not running.
        }

        int cpWrittenPages = writtenPagesCntr.get();

        long fullyCompletedPages = (cpWrittenPages + cpSyncedPages()) / 2; // written & sync'ed

        long curNanoTime = System.nanoTime();

        speedCpWrite.setCounter(fullyCompletedPages, curNanoTime);

        long markDirtySpeed = speedMarkAndAvgParkTime.getSpeedOpsPerSec(curNanoTime);

        long curCpWriteSpeed = speedCpWrite.getSpeedOpsPerSec(curNanoTime);

        threadIds.add(Thread.currentThread().getId());

        ThrottleMode level = ThrottleMode.NO; //should apply delay (throttling) for current page modification

        if (isPageInCheckpoint) {
            int checkpointBufLimit = pageMemory.checkpointBufferPagesSize() * 2 / 3;

            if (pageMemory.checkpointBufferPagesCount() > checkpointBufLimit)
                level = ThrottleMode.EXPONENTIAL;
        }

        long throttleParkTimeNs = 0;

        if (level == ThrottleMode.NO) {
            int nThreads = threadIds.size();

            int cpTotalPages = cpTotalPages();

            if (cpTotalPages == 0) {
                boolean throttleByCpSpeed = curCpWriteSpeed > 0 && markDirtySpeed > curCpWriteSpeed;

                if (throttleByCpSpeed) {
                    throttleParkTimeNs = calcDelayTime(curCpWriteSpeed, nThreads, 1);

                    level = ThrottleMode.LIMITED;
                }
            }
            else {
                double dirtyPagesRatio = pageMemory.getDirtyPagesRatio();

                currDirtyRatio = dirtyPagesRatio;

                detectCpPagesWriteStart(cpWrittenPages, dirtyPagesRatio);

                if (dirtyPagesRatio >= MAX_DIRTY_PAGES)
                    level = ThrottleMode.NO; // too late to throttle, will wait on safe to update instead.
                else {
                    int notEvictedPagesTotal = cpTotalPages - cpEvictedPages();

                    throttleParkTimeNs = getParkTime(dirtyPagesRatio,
                        fullyCompletedPages,
                        notEvictedPagesTotal < 0 ? 0 : notEvictedPagesTotal,
                        nThreads,
                        markDirtySpeed,
                        curCpWriteSpeed);

                    level = throttleParkTimeNs == 0 ? ThrottleMode.NO : ThrottleMode.LIMITED;
                }
            }
        }

        if (level == ThrottleMode.EXPONENTIAL) {
            int exponent = exponentialBackoffCntr.getAndIncrement();

            throttleParkTimeNs = (long)(STARTING_THROTTLE_NANOS * Math.pow(BACKOFF_RATIO, exponent));
        }
        else {
            if (isPageInCheckpoint)
                exponentialBackoffCntr.set(0);

            if (level == ThrottleMode.NO)
                throttleParkTimeNs = 0;
        }

        if (throttleParkTimeNs > 0) {
            recurrentLogIfNeed();

            doPark(throttleParkTimeNs);
        }

        pageMemory.metrics().addThrottlingTime(U.nanosToMillis(System.nanoTime() - curNanoTime));

        speedMarkAndAvgParkTime.addMeasurementForAverageCalculation(throttleParkTimeNs);
    }

    /**
     * Disables the current thread for thread scheduling purposes. May be overriden by subclasses for tests
     *
     * @param throttleParkTimeNs the maximum number of nanoseconds to wait
     */
    protected void doPark(long throttleParkTimeNs) {
        if (throttleParkTimeNs > LOGGING_THRESHOLD) {
            U.warn(log, "Parking thread=" + Thread.currentThread().getName()
                + " for timeout(ms)=" + (throttleParkTimeNs / 1_000_000));
        }

        LockSupport.parkNanos(throttleParkTimeNs);
    }

    /**
     * @return number of written pages.
     */
    private int cpWrittenPages() {
        AtomicInteger writtenPagesCntr = cpProgress.apply().writtenPagesCounter();

        return writtenPagesCntr == null ? 0 : writtenPagesCntr.get();
    }

    /**
     * @return Number of pages in current checkpoint.
     */
    private int cpTotalPages() {
        return cpProgress.apply().currentCheckpointPagesCount();
    }

    /**
     * @return  Counter for fsynced checkpoint pages.
     */
    private int cpSyncedPages() {
        AtomicInteger syncedPagesCntr = cpProgress.apply().syncedPagesCounter();

        return syncedPagesCntr == null ? 0 : syncedPagesCntr.get();
    }

    /**
     * @return number of evicted pages.
     */
    private int cpEvictedPages() {
        AtomicInteger evictedPagesCntr = cpProgress.apply().evictedPagesCounter();

        return evictedPagesCntr == null ? 0 : evictedPagesCntr.get();
    }

    /**
     * Prints warning to log if throttling is occurred and requires markable amount of time.
     */
    private void recurrentLogIfNeed() {
        long prevWarningNs = prevWarnTime.get();
        long curNs = System.nanoTime();

        if (prevWarningNs != 0 && (curNs - prevWarningNs) <= WARN_MIN_DELAY_NS)
            return;

        double weight = throttleWeight();
        if (weight <= WARN_THRESHOLD)
            return;

        if (prevWarnTime.compareAndSet(prevWarningNs, curNs) && log.isInfoEnabled()) {
            String msg = String.format("Throttling is applied to page modifications " +
                    "[percentOfPartTime=%.2f, markDirty=%d pages/sec, checkpointWrite=%d pages/sec, " +
                    "estIdealMarkDirty=%d pages/sec, curDirty=%.2f, maxDirty=%.2f, avgParkTime=%d ns, " +
                    "pages: (total=%d, evicted=%d, written=%d, synced=%d, cpBufUsed=%d, cpBufTotal=%d)]",
                weight, getMarkDirtySpeed(), getCpWriteSpeed(),
                getLastEstimatedSpeedForMarkAll(), getCurrDirtyRatio(), getTargetDirtyRatio(), throttleParkTime(),
                cpTotalPages(), cpEvictedPages(), cpWrittenPages(), cpSyncedPages(),
                pageMemory.checkpointBufferPagesCount(), pageMemory.checkpointBufferPagesSize());

            log.info(msg);
        }
    }

    /**
     * @param dirtyPagesRatio actual percent of dirty pages.
     * @param fullyCompletedPages written & fsynced pages count.
     * @param cpTotalPages total checkpoint scope.
     * @param nThreads number of threads providing data during current checkpoint.
     * @param markDirtySpeed registered mark dirty speed, pages/sec.
     * @param curCpWriteSpeed average checkpoint write speed, pages/sec.
     * @return time in nanoseconds to part or 0 if throttling is not required.
     */
    long getParkTime(
        double dirtyPagesRatio,
        long fullyCompletedPages,
        int cpTotalPages,
        int nThreads,
        long markDirtySpeed,
        long curCpWriteSpeed) {

        long speedForMarkAll = calcSpeedToMarkAllSpaceTillEndOfCp(dirtyPagesRatio,
            fullyCompletedPages,
            curCpWriteSpeed,
            cpTotalPages);

        double targetDirtyRatio = calcTargetDirtyRatio(fullyCompletedPages, cpTotalPages);

        this.speedForMarkAll = speedForMarkAll; //publish for metrics
        this.targetDirtyRatio = targetDirtyRatio; //publish for metrics

        boolean lowSpaceLeft = dirtyPagesRatio > targetDirtyRatio && (dirtyPagesRatio + 0.05 > MAX_DIRTY_PAGES);
        int slowdown = lowSpaceLeft ? 3 : 1;

        double multiplierForSpeedForMarkAll = lowSpaceLeft
            ? 0.8
            : 1.0;

        boolean markingTooFast = speedForMarkAll > 0 && markDirtySpeed > multiplierForSpeedForMarkAll * speedForMarkAll;
        boolean throttleBySizeAndMarkSpeed = dirtyPagesRatio > targetDirtyRatio && markingTooFast;

        //for case of speedForMarkAll >> markDirtySpeed, allow write little bit faster than CP average
        double allowWriteFasterThanCp = (speedForMarkAll > 0 && markDirtySpeed > 0 && speedForMarkAll > markDirtySpeed)
            ? (0.1 * speedForMarkAll / markDirtySpeed)
            : (dirtyPagesRatio > targetDirtyRatio ? 0.0 : 0.1);

        double fasterThanCpWriteSpeed = lowSpaceLeft
            ? 1.0
            : 1.0 + allowWriteFasterThanCp;
        boolean throttleByCpSpeed = curCpWriteSpeed > 0 && markDirtySpeed > (fasterThanCpWriteSpeed * curCpWriteSpeed);

        long delayByCpWrite = throttleByCpSpeed ? calcDelayTime(curCpWriteSpeed, nThreads, slowdown) : 0;
        long delayByMarkAllWrite = throttleBySizeAndMarkSpeed ? calcDelayTime(speedForMarkAll, nThreads, slowdown) : 0;
        return Math.max(delayByCpWrite, delayByMarkAllWrite);
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

        if (cpTotalPages <= 0)
            return 0;

        if (dirtyPagesRatio >= MAX_DIRTY_PAGES)
            return 0;

        double remainedClear = (MAX_DIRTY_PAGES - dirtyPagesRatio) * totalPages;

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
     * @param coefficient how much it is needed to slowdown base speed. 1.0 means delay to get exact base speed.
     * @return sleep time in nanoseconds.
     */
    private long calcDelayTime(long baseSpeed, int nThreads, double coefficient) {
        if (coefficient <= 0.0)
            return 0;

        if (baseSpeed <= 0)
            return 0;

        long updTimeNsForOnePage = TimeUnit.SECONDS.toNanos(1) * nThreads / (baseSpeed);

        return (long)(coefficient * updTimeNsForOnePage);
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
        speedCpWrite.setCounter(0L, System.nanoTime());

        initDirtyRatioAtCpBegin = MIN_RATIO_NO_THROTTLE;

        lastObservedWritten.set(0);
    }


    /** {@inheritDoc} */
    @Override public void onFinishCheckpoint() {
        exponentialBackoffCntr.set(0);

        speedCpWrite.finishInterval();
        speedMarkAndAvgParkTime.finishInterval();
        threadIds.clear();
    }

    /**
     * @return Exponential backoff counter.
     */
    public long throttleParkTime() {
        return speedMarkAndAvgParkTime.getAverage();
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
        double ratio = currDirtyRatio;

        if (ratio >= 0)
            return ratio;

        return pageMemory.getDirtyPagesRatio();
    }

    /**
     * @return  Speed of marking pages dirty. Value from past 750-1000 millis only. Pages/second.
     */
    public long getMarkDirtySpeed() {
        return speedMarkAndAvgParkTime.getSpeedOpsPerSec(System.nanoTime());
    }

    /**
     * @return Speed average checkpoint write speed. Current and 3 past checkpoints used. Pages/second.
     */
    public long getCpWriteSpeed() {
        return speedCpWrite.getSpeedOpsPerSecReadOnly();
    }

    /**
     * @return Returns {@link #speedForMarkAll}.
     */
    public long getLastEstimatedSpeedForMarkAll() {
        return speedForMarkAll;
    }

    /**
     * Measurement shows how much throttling time is involved into average marking time.
     * @return metric started from 0.0 and showing how much throttling is involved into current marking process.
     */
    public double throttleWeight() {
        long speed = speedMarkAndAvgParkTime.getSpeedOpsPerSec(System.nanoTime());

        if (speed <= 0)
            return 0;

        long timeForOnePage = calcDelayTime(speed, threadIds.size(), 1);

        if (timeForOnePage == 0)
            return 0;

        return 1.0 * throttleParkTime() / timeForOnePage;
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
