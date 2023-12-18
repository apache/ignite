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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.lang.IgniteOutClosure;
import org.jetbrains.annotations.NotNull;

/**
 * Speed-based throttle used to protect clean pages from exhaustion.
 *
 * The main method is {@link #protectionParkTime(long)} which is used to compute park time, if throttling is needed.
 *
 * @see #protectionParkTime(long)
 */
class SpeedBasedMemoryConsumptionThrottlingStrategy {
    /**
     * Maximum fraction of dirty pages in a region.
     */
    private static final double MAX_DIRTY_PAGES = 0.75;

    /**
     * Percent of dirty pages which will not cause throttling.
     */
    private static final double MIN_RATIO_NO_THROTTLE = 0.03;

    /**
     * Page memory.
     */
    private final PageMemoryImpl pageMemory;

    /**
     * Checkpoint progress provider.
     */
    private final IgniteOutClosure<CheckpointProgress> cpProgress;

    /**
     * Total pages possible to store in page memory.
     */
    private volatile long pageMemTotalPages;

    /**
     * Last estimated speed for marking all clear pages as dirty till the end of checkpoint.
     */
    private volatile long speedForMarkAll;

    /**
     * Target (maximum) dirty pages ratio, after which throttling will start using
     * {@link #getParkTime(double, long, int, int, long, long)}.
     */
    private volatile double targetDirtyRatio;

    /**
     * Current dirty pages ratio (percent of dirty pages in the most used segment), negative value means no cp is running.
     */
    private volatile double currDirtyRatio;

    /**
     * Threads set. Contains identifiers of all threads which were marking pages for the current checkpoint.
     */
    private final Set<Long> threadIds = new GridConcurrentHashSet<>();

    /**
     * Counter of written pages from a checkpoint. Value is saved here for detecting a checkpoint start.
     */
    private final AtomicInteger lastObservedWritten = new AtomicInteger(0);

    /**
     * Dirty pages ratio that was observed at a checkpoint start (here the start is a moment when the first page was actually saved to
     * store). This ratio is excluded from throttling.
     */
    private volatile double initDirtyRatioAtCpBegin = MIN_RATIO_NO_THROTTLE;

    /**
     * Average checkpoint write speed. Only the current value is used. Pages/second.
     */
    private final ProgressSpeedCalculation cpWriteSpeed = new ProgressSpeedCalculation();

    /**
     * Used for calculating speed of marking pages dirty.
     * Value from past 750-1000 millis only.
     * {@link IntervalBasedMeasurement#getSpeedOpsPerSec(long)} returns pages marked/second.
     * {@link IntervalBasedMeasurement#getAverage()} returns average throttle time.
     */
    private final IntervalBasedMeasurement markSpeedAndAvgParkTime;

    /***/
    SpeedBasedMemoryConsumptionThrottlingStrategy(PageMemoryImpl pageMemory,
                                                  IgniteOutClosure<CheckpointProgress> cpProgress,
                                                  IntervalBasedMeasurement markSpeedAndAvgParkTime) {
        this.pageMemory = pageMemory;
        this.cpProgress = cpProgress;
        this.markSpeedAndAvgParkTime = markSpeedAndAvgParkTime;
    }

    /**
     * Computes next duration (in nanos) to throttle a thread.
     * Might return {@link PagesWriteSpeedBasedThrottle#NO_THROTTLING_MARKER} as a marker that no throttling should be applied.
     *
     * @return park time in nanos or #NO_THROTTLING_MARKER if no throttling is needed
     */
    long protectionParkTime(long curNanoTime) {
        CheckpointProgress progress = cpProgress.apply();
        AtomicInteger writtenPagesCounter = progress == null ? null : progress.writtenPagesCounter();

        boolean checkpointProgressIsNotYetReported = writtenPagesCounter == null;
        if (checkpointProgressIsNotYetReported) {
            resetStatistics();

            return PagesWriteSpeedBasedThrottle.NO_THROTTLING_MARKER;
        }

        threadIds.add(Thread.currentThread().getId());

        return computeParkTime(writtenPagesCounter, curNanoTime);
    }

    /***/
    private void resetStatistics() {
        speedForMarkAll = 0;
        targetDirtyRatio = -1;
        currDirtyRatio = -1;
    }

    /***/
    private long computeParkTime(@NotNull AtomicInteger writtenPagesCounter, long curNanoTime) {
        final int cpWrittenPages = writtenPagesCounter.get();
        final long donePages = cpDonePagesEstimation(cpWrittenPages);

        final long instantaneousMarkDirtySpeed = markSpeedAndAvgParkTime.getSpeedOpsPerSec(curNanoTime);
        // NB: we update progress for speed calculation only in this (clean pages protection) scenario, because
        // we only use the computed speed in this same scenario and for reporting in logs (where it's not super
        // important to display an ideally accurate speed), but not in the CP Buffer protection scenario.
        // This is to simplify code.
        // The progress is set to 0 at the beginning of a checkpoint, so we can be sure that the start time remembered
        // in cpWriteSpeed is pretty accurate even without writing to cpWriteSpeed from this method.
        cpWriteSpeed.setProgress(donePages, curNanoTime);
        // TODO: IGNITE-16878 use exponential moving average so that we react to changes faster?
        final long avgCpWriteSpeed = cpWriteSpeed.getOpsPerSecond(curNanoTime);

        final int cpTotalPages = cpTotalPages();

        if (cpTotalPages == 0) {
            // From the current code analysis, we can only get here by accident when
            // CheckpointProgressImpl.clearCounters() is invoked at the end of a checkpoint (by falling through
            // between two volatile assignments). When we get here, we don't have any information about the total
            // number of pages in the current CP, so we calculate park time by only using information we have.
            return parkTimeToThrottleByJustCPSpeed(instantaneousMarkDirtySpeed, avgCpWriteSpeed);
        }
        else {
            return speedBasedParkTime(cpWrittenPages, donePages, cpTotalPages, instantaneousMarkDirtySpeed,
                    avgCpWriteSpeed);
        }
    }

    /**
     * Returns an estimation of the progress made during the current checkpoint. Currently, it is an average of
     * written pages and fully synced pages (probably, to account for both writing (which may be pretty
     * ahead of syncing) and syncing at the same time).
     *
     * @param cpWrittenPages    count of pages written during current checkpoint
     * @return estimation of work done (in pages)
     */
    private int cpDonePagesEstimation(int cpWrittenPages) {
        // TODO: IGNITE-16879 - this only works correctly if time-to-write a page is close to time-to-sync a page.
        // In reality, this does not seem to hold, which produces wrong estimations. We could measure the real times
        // in Checkpointer and make this estimation a lot more precise.
        return (cpWrittenPages + cpSyncedPages()) / 2;
    }

    /**
     * Simplified version of park time calculation used when we don't have information about total CP size (in pages).
     * Such a situation seems to be very rare, but it can happen when finishing a CP.
     *
     * @param markDirtySpeed    speed of page dirtying
     * @param curCpWriteSpeed   speed of CP writing pages
     * @return park time (nanos)
     */
    private long parkTimeToThrottleByJustCPSpeed(long markDirtySpeed, long curCpWriteSpeed) {
        boolean throttleByCpSpeed = curCpWriteSpeed > 0 && markDirtySpeed > curCpWriteSpeed;

        if (throttleByCpSpeed) {
            return nsPerOperation(curCpWriteSpeed);
        }

        return 0;
    }

    /***/
    private long speedBasedParkTime(int cpWrittenPages, long donePages, int cpTotalPages,
                                    long instantaneousMarkDirtySpeed, long avgCpWriteSpeed) {
        final double dirtyPagesRatio = pageMemory.getDirtyPagesRatio();

        currDirtyRatio = dirtyPagesRatio;

        detectCpPagesWriteStart(cpWrittenPages, dirtyPagesRatio);

        if (dirtyPagesRatio >= MAX_DIRTY_PAGES)
            return 0; // too late to throttle, will wait on safe to update instead.
        else {
            return getParkTime(dirtyPagesRatio,
                    donePages,
                    notEvictedPagesTotal(cpTotalPages),
                    threadIdsCount(),
                    instantaneousMarkDirtySpeed,
                    avgCpWriteSpeed);
        }
    }

    /***/
    private int notEvictedPagesTotal(int cpTotalPages) {
        return Math.max(cpTotalPages - cpEvictedPages(), 0);
    }

    /**
     * Computes park time for throttling.
     *
     * @param dirtyPagesRatio     actual percent of dirty pages.
     * @param donePages           roughly, written & fsynced pages count.
     * @param cpTotalPages        total checkpoint scope.
     * @param nThreads            number of threads providing data during current checkpoint.
     * @param instantaneousMarkDirtySpeed registered (during approx last second) mark dirty speed, pages/sec.
     * @param avgCpWriteSpeed     average checkpoint write speed, pages/sec.
     * @return time in nanoseconds to part or 0 if throttling is not required.
     */
    long getParkTime(
            double dirtyPagesRatio,
            long donePages,
            int cpTotalPages,
            int nThreads,
            long instantaneousMarkDirtySpeed,
            long avgCpWriteSpeed) {

        final long targetSpeedToMarkAll = calcSpeedToMarkAllSpaceTillEndOfCp(dirtyPagesRatio, donePages,
            avgCpWriteSpeed, cpTotalPages);
        final double targetCurDirtyRatio = targetCurrentDirtyRatio(donePages, cpTotalPages);

        publishSpeedAndRatioForMetrics(targetSpeedToMarkAll, targetCurDirtyRatio);

        return delayIfMarkingFasterThanTargetSpeedAllows(instantaneousMarkDirtySpeed,
            dirtyPagesRatio, nThreads, targetSpeedToMarkAll, targetCurDirtyRatio);
    }

    /***/
    private long delayIfMarkingFasterThanTargetSpeedAllows(long instantaneousMarkDirtySpeed, double dirtyPagesRatio,
                                                           int nThreads,
                                                           long targetSpeedToMarkAll, double targetCurrentDirtyRatio) {
        final boolean lowSpaceLeft = lowCleanSpaceLeft(dirtyPagesRatio, targetCurrentDirtyRatio);
        final int slowdown = slowdownIfLowSpaceLeft(lowSpaceLeft);

        double multiplierForSpeedToMarkAll = lowSpaceLeft ? 0.8 : 1.0;
        boolean markingTooFastNow = targetSpeedToMarkAll > 0
                && instantaneousMarkDirtySpeed > multiplierForSpeedToMarkAll * targetSpeedToMarkAll;
        boolean markedTooFastSinceCPStart = dirtyPagesRatio > targetCurrentDirtyRatio;
        boolean markingTooFast = markedTooFastSinceCPStart && markingTooFastNow;

        // We must NOT subtract nsPerOperation(instantaneousMarkDirtySpeed, nThreads)! If we do, the actual speed
        // converges to a value that is 1-2 times higher than the target speed.
        return markingTooFast ? nsPerOperation(targetSpeedToMarkAll, nThreads, slowdown) : 0;
    }

    /***/
    private int slowdownIfLowSpaceLeft(boolean lowSpaceLeft) {
        return lowSpaceLeft ? 3 : 1;
    }

    /**
     * Whether too low clean space is left, so more powerful measures are needed (like heavier throttling).
     *
     * @param dirtyPagesRatio  current dirty pages ratio
     * @param targetDirtyRatio target dirty pages ratio
     * @return {@code true} iff clean space left is too low
     */
    private boolean lowCleanSpaceLeft(double dirtyPagesRatio, double targetDirtyRatio) {
        return dirtyPagesRatio > targetDirtyRatio && (dirtyPagesRatio + 0.05 > MAX_DIRTY_PAGES);
    }

    /***/
    private void publishSpeedAndRatioForMetrics(long speedForMarkAll, double targetDirtyRatio) {
        this.speedForMarkAll = speedForMarkAll;
        this.targetDirtyRatio = targetDirtyRatio;
    }

    /**
     * Calculates speed needed to mark dirty all currently clean pages before the current checkpoint ends.
     * May return 0 if the provided parameters do not give enough information to calculate the speed, OR
     * if the current dirty pages ratio is too high (higher than {@link #MAX_DIRTY_PAGES}), in which case
     * we're not going to throttle anyway.
     *
     * @param dirtyPagesRatio     current percent of dirty pages.
     * @param donePages           roughly, count of written and sync'ed pages
     * @param avgCpWriteSpeed     pages/second checkpoint write speed. 0 speed means 'no data'.
     * @param cpTotalPages        total pages in checkpoint.
     * @return pages/second to mark to mark all clean pages as dirty till the end of checkpoint. 0 speed means 'no
     * data', or when we are not going to throttle due to the current dirty pages ratio being too high
     */
    private long calcSpeedToMarkAllSpaceTillEndOfCp(double dirtyPagesRatio, long donePages,
                                                    long avgCpWriteSpeed, int cpTotalPages) {
        if (avgCpWriteSpeed == 0)
            return 0;

        if (cpTotalPages <= 0)
            return 0;

        if (dirtyPagesRatio >= MAX_DIRTY_PAGES)
            return 0;

        // IDEA: here, when calculating the count of clean pages, it includes the pages under checkpoint. It is kinda
        // legal because they can be written (using the Checkpoint Buffer to make a copy of the value to be
        // checkpointed), but the CP Buffer is usually not too big, and if it gets nearly filled, writes become
        // throttled really hard by exponential throttler. Maybe we should subtract the number of not-yet-written-by-CP
        // pages from the count of clean pages? In such a case, we would lessen the risk of CP Buffer-caused throttling.
        double remainedCleanPages = (MAX_DIRTY_PAGES - dirtyPagesRatio) * pageMemTotalPages();

        double secondsTillCPEnd = 1.0 * (cpTotalPages - donePages) / avgCpWriteSpeed;

        return (long)(remainedCleanPages / secondsTillCPEnd);
    }

    /** Returns total number of pages storable in page memory. */
    private long pageMemTotalPages() {
        long curTotalPages = pageMemTotalPages;

        if (curTotalPages == 0) {
            curTotalPages = pageMemory.totalPages();
            pageMemTotalPages = curTotalPages;
        }

        assert curTotalPages > 0 : "PageMemory.totalPages() is still 0";

        return curTotalPages;
    }

    /**
     * @param donePages         number of completed.
     * @param cpTotalPages      Total amount of pages under checkpoint.
     * @return size-based calculation of target ratio.
     */
    private double targetCurrentDirtyRatio(long donePages, int cpTotalPages) {
        double cpProgress = ((double)donePages) / cpTotalPages;

        // Starting with initialDirtyRatioAtCpBegin to avoid throttle right after checkpoint start
        double constStart = initDirtyRatioAtCpBegin;

        double fractionToVaryDirtyRatio = 1.0 - constStart;

        return (cpProgress * fractionToVaryDirtyRatio + constStart) * MAX_DIRTY_PAGES;
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
     * @return Returns {@link #speedForMarkAll}.
     */
    public long getLastEstimatedSpeedForMarkAll() {
        return speedForMarkAll;
    }

    /**
     * @return Speed average checkpoint write speed. Current and 3 past checkpoints used. Pages/second.
     */
    public long getCpWriteSpeed() {
        return cpWriteSpeed.getOpsPerSecondReadOnly();
    }

    /***/
    int threadIdsCount() {
        return threadIds.size();
    }

    /**
     * @return Counter for fsynced checkpoint pages.
     */
    int cpSyncedPages() {
        AtomicInteger syncedPagesCntr = cpProgress.apply().syncedPagesCounter();

        return syncedPagesCntr == null ? 0 : syncedPagesCntr.get();
    }

    /**
     * @return Number of pages in current checkpoint.
     */
    int cpTotalPages() {
        return cpProgress.apply().currentCheckpointPagesCount();
    }

    /**
     * @return number of evicted pages.
     */
    int cpEvictedPages() {
        AtomicInteger evictedPagesCntr = cpProgress.apply().evictedPagesCounter();

        return evictedPagesCntr == null ? 0 : evictedPagesCntr.get();
    }

    /**
     * @param baseSpeed   speed to slow down.
     * @return sleep time in nanoseconds.
     */
    long nsPerOperation(long baseSpeed) {
        return nsPerOperation(baseSpeed, threadIdsCount());
    }

    /**
     * @param speedPagesPerSec   speed to slow down.
     * @param nThreads    operating threads.
     * @return sleep time in nanoseconds.
     */
    private long nsPerOperation(long speedPagesPerSec, int nThreads) {
        return nsPerOperation(speedPagesPerSec, nThreads, 1);
    }

    /**
     * @param speedPagesPerSec   speed to slow down.
     * @param nThreads    operating threads.
     * @param factor      how much it is needed to slowdown base speed. 1 means delay to get exact base speed.
     * @return sleep time in nanoseconds.
     */
    private long nsPerOperation(long speedPagesPerSec, int nThreads, int factor) {
        if (factor <= 0)
            throw new IllegalStateException("Coefficient should be positive");

        if (speedPagesPerSec <= 0)
            return 0;

        long updTimeNsForOnePage = TimeUnit.SECONDS.toNanos(1) * nThreads / (speedPagesPerSec);

        return factor * updTimeNsForOnePage;
    }

    /**
     * @param cpWrittenPages  current counter of written pages.
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

    /**
     * Resets the throttle to its initial state (for example, in the beginning of a checkpoint).
     */
    void reset() {
        cpWriteSpeed.setProgress(0L, System.nanoTime());
        initDirtyRatioAtCpBegin = MIN_RATIO_NO_THROTTLE;
        lastObservedWritten.set(0);
    }

    /**
     * Moves the throttle to its finalized state (for example, when a checkpoint ends).
     */
    void finish() {
        cpWriteSpeed.closeInterval();
        threadIds.clear();
    }
}
