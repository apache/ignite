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
package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.ratemetrics.HitRateMetrics;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.mxbean.DataStorageMetricsMXBean;

/**
 *
 */
public class DataStorageMetricsImpl implements DataStorageMetricsMXBean {
    /** */
    private volatile HitRateMetrics walLoggingRate;

    /** */
    private volatile HitRateMetrics walWritingRate;

    /** */
    private volatile HitRateMetrics walFsyncTimeDuration;

    /** */
    private volatile HitRateMetrics walFsyncTimeNum;

    /** */
    private volatile HitRateMetrics walBuffPollSpinsNum;

    /** */
    private volatile long lastCpLockWaitDuration;

    /** */
    private volatile long lastCpMarkDuration;

    /** */
    private volatile long lastCpPagesWriteDuration;

    /** */
    private volatile long lastCpDuration;

    /** */
    private volatile long lastCpFsyncDuration;

    /** */
    private volatile long lastCpTotalPages;

    /** */
    private volatile long lastCpDataPages;

    /** */
    private volatile long lastCpCowPages;

    /** */
    private volatile long rateTimeInterval;

    /** */
    private volatile int subInts;

    /** */
    private volatile boolean metricsEnabled;

    /** */
    private volatile IgniteWriteAheadLogManager wal;

    /** */
    private volatile IgniteOutClosure<Long> walSizeProvider;

    /** */
    private volatile long lastWalSegmentRollOverTime;

    /** */
    private final AtomicLong totalCheckpointTime = new AtomicLong();

    /** */
    private volatile Collection<DataRegionMetrics> regionMetrics;

    /**
     * @param metricsEnabled Metrics enabled flag.
     * @param rateTimeInterval Rate time interval.
     * @param subInts Number of sub-intervals.
     */
    public DataStorageMetricsImpl(
        boolean metricsEnabled,
        long rateTimeInterval,
        int subInts
    ) {
        this.metricsEnabled = metricsEnabled;
        this.rateTimeInterval = rateTimeInterval;
        this.subInts = subInts;

        resetRates();
    }

    /** {@inheritDoc} */
    @Override public float getWalLoggingRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)walLoggingRate.getRate() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getWalWritingRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)walWritingRate.getRate() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public int getWalArchiveSegments() {
        if (!metricsEnabled)
            return 0;

        return wal.walArchiveSegments();
    }

    /** {@inheritDoc} */
    @Override public float getWalFsyncTimeAverage() {
        if (!metricsEnabled)
            return 0;

        long numRate = walFsyncTimeNum.getRate();

        if (numRate == 0)
            return 0;

        return (float)walFsyncTimeDuration.getRate() / numRate;
    }

    /** {@inheritDoc} */
    @Override public long getWalBuffPollSpinsRate() {
        if (!metricsEnabled)
            return 0;

        return walBuffPollSpinsNum.getRate();
    }


    /** {@inheritDoc} */
    @Override public long getLastCheckpointDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointLockWaitDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpLockWaitDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointMarkDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpMarkDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointPagesWriteDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpPagesWriteDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointFsyncDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpFsyncDuration;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointTotalPagesNumber() {
        if (!metricsEnabled)
            return 0;

        return lastCpTotalPages;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDataPagesNumber() {
        if (!metricsEnabled)
            return 0;

        return lastCpDataPages;
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointCopiedOnWritePagesNumber() {
        if (!metricsEnabled)
            return 0;

        return lastCpCowPages;
    }

    /** {@inheritDoc} */
    @Override public void enableMetrics() {
        metricsEnabled = true;
    }

    /** {@inheritDoc} */
    @Override public void disableMetrics() {
        metricsEnabled = false;
    }

    /** {@inheritDoc} */
    @Override public void rateTimeInterval(long rateTimeInterval) {
        this.rateTimeInterval = rateTimeInterval;

        resetRates();
    }

    /** {@inheritDoc} */
    @Override public void subIntervals(int subInts) {
        this.subInts = subInts;

        resetRates();
    }

    /** {@inheritDoc} */
    @Override public long getWalTotalSize() {
        if (!metricsEnabled)
            return 0;

        IgniteOutClosure<Long> walSize = this.walSizeProvider;

        return walSize != null ? walSize.apply() : 0;
    }

    /** {@inheritDoc} */
    @Override public long getWalLastRollOverTime() {
        if (!metricsEnabled)
            return 0;

        return lastWalSegmentRollOverTime;
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointTotalTime() {
        if (!metricsEnabled)
            return 0;

        return totalCheckpointTime.get();
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long dirtyPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            dirtyPages += rm.getDirtyPages();

        return dirtyPages;
    }

    /** {@inheritDoc} */
    @Override public long getPagesRead() {
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long readPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            readPages += rm.getPagesRead();

        return readPages;
    }

    /** {@inheritDoc} */
    @Override public long getPagesWritten() {
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long writtenPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            writtenPages += rm.getPagesWritten();

        return writtenPages;
    }

    /** {@inheritDoc} */
    @Override public long getPagesReplaced() {
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long replacedPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            replacedPages += rm.getPagesReplaced();

        return replacedPages;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapSize() {
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long offHeapSize = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            offHeapSize += rm.getOffHeapSize();

        return offHeapSize;
    }

    /** {@inheritDoc} */
    @Override public long getOffheapUsedSize() {
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long offHeapUsedSize = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            offHeapUsedSize += rm.getOffheapUsedSize();

        return offHeapUsedSize;
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long totalAllocatedSize = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            totalAllocatedSize += rm.getTotalAllocatedSize();

        return totalAllocatedSize;
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferPages() {
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long usedCheckpointBufferPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            usedCheckpointBufferPages += rm.getUsedCheckpointBufferPages();

        return usedCheckpointBufferPages;
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferSize() {
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long usedCheckpointBufferSize = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            usedCheckpointBufferSize += rm.getUsedCheckpointBufferSize();

        return usedCheckpointBufferSize;
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointBufferSize(){
        if (!metricsEnabled)
            return 0;

        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long checkpointBufferSize = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            checkpointBufferSize += rm.getCheckpointBufferSize();

        return checkpointBufferSize;
    }

    /**
     * @param wal Write-ahead log manager.
     */
    public void wal(IgniteWriteAheadLogManager wal) {
        this.wal = wal;
    }

    /**
     * @param walSizeProvider Wal size provider.
     */
    public void setWalSizeProvider(IgniteOutClosure<Long> walSizeProvider){
        this.walSizeProvider = walSizeProvider;
    }

    /**
     *
     */
    public void onWallRollOver() {
        this.lastWalSegmentRollOverTime = U.currentTimeMillis();
    }

    /**
     *
     */
    public void regionMetrics(Collection<DataRegionMetrics> regionMetrics){
        this.regionMetrics = regionMetrics;
    }

    /**
     * @return Metrics enabled flag.
     */
    public boolean metricsEnabled() {
        return metricsEnabled;
    }

    /**
     * @param lockWaitDuration Lock wait duration.
     * @param markDuration Mark duration.
     * @param pagesWriteDuration Pages write duration.
     * @param fsyncDuration Total checkpoint fsync duration.
     * @param duration Total checkpoint duration.
     * @param totalPages Total number of all pages in checkpoint.
     * @param dataPages Total number of data pages in checkpoint.
     * @param cowPages Total number of COW-ed pages in checkpoint.
     */
    public void onCheckpoint(
        long lockWaitDuration,
        long markDuration,
        long pagesWriteDuration,
        long fsyncDuration,
        long duration,
        long totalPages,
        long dataPages,
        long cowPages
    ) {
        if (metricsEnabled) {
            lastCpLockWaitDuration = lockWaitDuration;
            lastCpMarkDuration = markDuration;
            lastCpPagesWriteDuration = pagesWriteDuration;
            lastCpFsyncDuration = fsyncDuration;
            lastCpDuration = duration;
            lastCpTotalPages = totalPages;
            lastCpDataPages = dataPages;
            lastCpCowPages = cowPages;

            totalCheckpointTime.addAndGet(duration);
        }
    }

    /**
     *
     */
    public void onWalRecordLogged() {
        walLoggingRate.onHit();
    }

    /**
     * @param size Size written.
     */
    public void onWalBytesWritten(int size) {
        walWritingRate.onHits(size);
    }

    /**
     * @param nanoTime Fsync nano time.
     */
    public void onFsync(long nanoTime) {
        long microseconds = nanoTime / 1_000;

        walFsyncTimeDuration.onHits(microseconds);
        walFsyncTimeNum.onHit();
    }

    /**
     * @param num Number.
     */
    public void onBuffPollSpin(int num) {
        walBuffPollSpinsNum.onHits(num);
    }

    /**
     *
     */
    private void resetRates() {
        walLoggingRate = new HitRateMetrics((int)rateTimeInterval, subInts);
        walWritingRate = new HitRateMetrics((int)rateTimeInterval, subInts);
        walBuffPollSpinsNum = new HitRateMetrics((int)rateTimeInterval, subInts);

        walFsyncTimeDuration = new HitRateMetrics((int)rateTimeInterval, subInts);
        walFsyncTimeNum = new HitRateMetrics((int)rateTimeInterval, subInts);
    }
}
