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
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.mxbean.DataStorageMetricsMXBean;
import org.apache.ignite.spi.metric.MetricRegistry;
import org.apache.ignite.spi.metric.counter.HitRateCounter;
import org.apache.ignite.spi.metric.counter.LongCounter;
import org.apache.ignite.spi.metric.gauge.LongGauge;

/**
 *
 */
public class DataStorageMetricsImpl implements DataStorageMetricsMXBean {
    /** */
    private final HitRateCounter walLoggingRate;

    /** */
    private final HitRateCounter walWritingRate;

    /** */
    private final HitRateCounter walFsyncTimeDuration;

    /** */
    private final HitRateCounter walFsyncTimeNum;

    /** */
    private final HitRateCounter walBuffPollSpinsNum;

    /** */
    private final LongGauge lastCpLockWaitDuration;

    /** */
    private final LongGauge lastCpMarkDuration;

    /** */
    private final LongGauge lastCpPagesWriteDuration;

    /** */
    private final LongGauge lastCpDuration;

    /** */
    private final LongGauge lastCpFsyncDuration;

    /** */
    private final LongGauge lastCpTotalPages;

    /** */
    private final LongGauge lastCpDataPages;

    /** */
    private final LongGauge lastCpCowPages;

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
    private final LongGauge lastWalSegmentRollOverTime;

    /** */
    private final LongCounter totalCheckpointTime;

    /** */
    private volatile Collection<DataRegionMetrics> regionMetrics;

    /** */
    private final LongGauge storageSize;

    /** */
    private final LongGauge sparseStorageSize;

    /**
     * @param mreg Metrics registry.
     * @param metricsEnabled Metrics enabled flag.
     * @param rateTimeInterval Rate time interval.
     * @param subInts Number of sub-intervals.
     */
    public DataStorageMetricsImpl(
        MetricRegistry mreg,
        boolean metricsEnabled,
        long rateTimeInterval,
        int subInts
    ) {
        this.metricsEnabled = metricsEnabled;
        this.rateTimeInterval = rateTimeInterval;
        this.subInts = subInts;

        MetricRegistry mset = mreg.withPrefix("io.datastorage");

        walLoggingRate = mset.hitRateCounter("WalLoggingRate",
            "Average number of WAL records per second written during the last time interval.",
            rateTimeInterval,
            subInts);

        walWritingRate = mset.hitRateCounter(
            "WalWritingRate",
            "Average number of bytes per second written during the last time interval.",
            rateTimeInterval,
            subInts);

        walFsyncTimeDuration = mset.hitRateCounter(
            "WalFsyncTimeDuration",
            "Total duration of fsync",
            rateTimeInterval,
            subInts);

        walFsyncTimeNum = mset.hitRateCounter(
            "WalFsyncTimeNum",
            "Total count of fsync",
            rateTimeInterval,
            subInts);

        walBuffPollSpinsNum = mset.hitRateCounter(
            "WalBuffPollSpinsRate",
            "WAL buffer poll spins number over the last time interval.",
            rateTimeInterval,
            subInts);

        lastCpLockWaitDuration = mset.gauge("LastCheckpointLockWaitDuration",
            "Duration of the checkpoint lock wait in milliseconds.");

        lastCpMarkDuration = mset.gauge("LastCheckpointMarkDuration",
            "Duration of the checkpoint lock wait in milliseconds.");

        lastCpPagesWriteDuration = mset.gauge("LastCheckpointPagesWriteDuration",
            "Duration of the checkpoint pages write in milliseconds.");

        lastCpDuration = mset.gauge("LastCheckpointDuration",
            "Duration of the last checkpoint in milliseconds.");

        lastCpFsyncDuration = mset.gauge("LastCheckpointFsyncDuration",
            "Duration of the sync phase of the last checkpoint in milliseconds.");

        lastCpTotalPages = mset.gauge("LastCheckpointTotalPagesNumber",
            "Total number of pages written during the last checkpoint.");

        lastCpDataPages = mset.gauge("LastCheckpointDataPagesNumber",
            "Total number of data pages written during the last checkpoint.");

        lastCpCowPages = mset.gauge("LastCheckpointCopiedOnWritePagesNumber",
            "Number of pages copied to a temporary checkpoint buffer during the last checkpoint.");

        lastWalSegmentRollOverTime = mset.gauge("WalLastRollOverTime",
            "Time of the last WAL segment rollover.");

        totalCheckpointTime = mset.counter("CheckpointTotalTime",
            "Total duration of checkpoint");

        storageSize = mset.gauge("StorageSize",
            "Storage space allocated, in bytes.");

        sparseStorageSize = mset.gauge("SparseStorageSize",
            "Storage space allocated adjusted for possible sparsity, in bytes.");

        mset.register("WalArchiveSegments",
            this::getWalArchiveSegments,
            "Current number of WAL segments in the WAL archive.");

        mset.register("WalTotalSize",
            this::getWalTotalSize,
            "Total size in bytes for storage wal files.");
    }

    /** {@inheritDoc} */
    @Override public float getWalLoggingRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)walLoggingRate.value() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getWalWritingRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)walWritingRate.value() * 1000) / rateTimeInterval;
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

        long numRate = walFsyncTimeNum.value();

        if (numRate == 0)
            return 0;

        return (float)walFsyncTimeDuration.value() / numRate;
    }

    /** {@inheritDoc} */
    @Override public long getWalBuffPollSpinsRate() {
        if (!metricsEnabled)
            return 0;

        return walBuffPollSpinsNum.value();
    }


    /** {@inheritDoc} */
    @Override public long getLastCheckpointDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpDuration.value();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointLockWaitDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpLockWaitDuration.value();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointMarkDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpMarkDuration.value();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointPagesWriteDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpPagesWriteDuration.value();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointFsyncDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpFsyncDuration.value();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointTotalPagesNumber() {
        if (!metricsEnabled)
            return 0;

        return lastCpTotalPages.value();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDataPagesNumber() {
        if (!metricsEnabled)
            return 0;

        return lastCpDataPages.value();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointCopiedOnWritePagesNumber() {
        if (!metricsEnabled)
            return 0;

        return lastCpCowPages.value();
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

        return lastWalSegmentRollOverTime.value();
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
        this.lastWalSegmentRollOverTime.value(U.currentTimeMillis());
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

    /** {@inheritDoc} */
    @Override public long getStorageSize() {
        return storageSize.value();
    }

    /** {@inheritDoc} */
    @Override public long getSparseStorageSize() {
        return sparseStorageSize.value();
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
        long cowPages,
        long storageSize,
        long sparseStorageSize
    ) {
        if (metricsEnabled) {
            lastCpLockWaitDuration.value(lockWaitDuration);
            lastCpMarkDuration.value(markDuration);
            lastCpPagesWriteDuration.value(pagesWriteDuration);
            lastCpFsyncDuration.value(fsyncDuration);
            lastCpDuration.value(duration);
            lastCpTotalPages.value(totalPages);
            lastCpDataPages.value(dataPages);
            lastCpCowPages.value(cowPages);
            this.storageSize.value(storageSize);
            this.sparseStorageSize.value(sparseStorageSize);

            totalCheckpointTime.add(duration);
        }
    }

    /**
     *
     */
    public void onWalRecordLogged() {
        walLoggingRate.increment();
    }

    /**
     * @param size Size written.
     */
    public void onWalBytesWritten(int size) {
        walWritingRate.add(size);
    }

    /**
     * @param nanoTime Fsync nano time.
     */
    public void onFsync(long nanoTime) {
        long microseconds = nanoTime / 1_000;

        walFsyncTimeDuration.add(microseconds);
        walFsyncTimeNum.increment();
    }

    /**
     * @param num Number.
     */
    public void onBuffPollSpin(int num) {
        walBuffPollSpinsNum.add(num);
    }

    /**
     *
     */
    private void resetRates() {
        walLoggingRate.reset(rateTimeInterval, subInts);
        walWritingRate.reset(rateTimeInterval, subInts);
        walBuffPollSpinsNum.reset(rateTimeInterval, subInts);

        walFsyncTimeDuration.reset(rateTimeInterval, subInts);
        walFsyncTimeNum.reset(rateTimeInterval, subInts);
    }
}
