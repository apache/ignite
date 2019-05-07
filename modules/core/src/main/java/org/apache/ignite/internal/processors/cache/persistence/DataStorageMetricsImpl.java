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
import org.apache.ignite.internal.processors.monitoring.GridMonitoringManager;
import org.apache.ignite.internal.processors.monitoring.MonitoringGroup;
import org.apache.ignite.internal.processors.monitoring.sensor.HitRateSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.LongAdderSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.LongSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.mxbean.DataStorageMetricsMXBean;

/**
 * @deprecated Use {@link GridMonitoringManager} instead.
 */
@Deprecated
public class DataStorageMetricsImpl implements DataStorageMetricsMXBean {
    /** */
    private volatile HitRateSensor walLoggingRate;

    /** */
    private volatile HitRateSensor walWritingRate;

    /** */
    private volatile HitRateSensor walFsyncTimeDuration;

    /** */
    private volatile HitRateSensor walFsyncTimeNum;

    /** */
    private volatile HitRateSensor walBuffPollSpinsNum;

    /** */
    private final LongSensor lastCpLockWaitDuration;

    /** */
    private final LongSensor lastCpMarkDuration;

    /** */
    private final LongSensor lastCpPagesWriteDuration;

    /** */
    private final LongSensor lastCpDuration;

    /** */
    private final LongSensor lastCpFsyncDuration;

    /** */
    private final LongSensor lastCpTotalPages;

    /** */
    private final LongSensor lastCpDataPages;

    /** */
    private final LongSensor lastCpCowPages;

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
    private final LongSensor lastWalSegmentRollOverTime;

    /** */
    private final LongAdderSensor totalCheckpointTime;

    /** */
    private volatile Collection<DataRegionMetrics> regionMetrics;

    /** */
    private final LongSensor storageSize;

    /** */
    private final LongSensor sparseStorageSize;

    /**
     * @param metricsEnabled Metrics enabled flag.
     * @param rateTimeInterval Rate time interval.
     * @param subInts Number of sub-intervals.
     */
    public DataStorageMetricsImpl(
        GridMonitoringManager mgr,
        boolean metricsEnabled,
        long rateTimeInterval,
        int subInts
    ) {
        this.metricsEnabled = metricsEnabled;
        this.rateTimeInterval = rateTimeInterval;
        this.subInts = subInts;

        SensorGroup grp = mgr.sensorsGroup(MonitoringGroup.DATA_STORAGE);

        walLoggingRate = grp.hitRateSensor("WalLoggingRate", (int)rateTimeInterval, subInts);
        walWritingRate = grp.hitRateSensor("WalWritingRate", (int)rateTimeInterval, subInts);
        walFsyncTimeDuration = grp.hitRateSensor("WalFsyncTimeDuration", (int)rateTimeInterval, subInts);
        walFsyncTimeNum = grp.hitRateSensor("WalFsyncTimeNum", (int)rateTimeInterval, subInts);
        walBuffPollSpinsNum = grp.hitRateSensor("WalBuffPollSpinsRate", (int)rateTimeInterval, subInts);

        lastCpLockWaitDuration = grp.longSensor("LastCheckpointLockWaitDuration");
        lastCpMarkDuration = grp.longSensor("LastCheckpointMarkDuration");
        lastCpPagesWriteDuration = grp.longSensor("LastCheckpointPagesWriteDuration");
        lastCpDuration = grp.longSensor("LastCheckpointDuration");
        lastCpFsyncDuration = grp.longSensor("LastCheckpointFsyncDuration");
        lastCpTotalPages = grp.longSensor("LastCheckpointTotalPagesNumber");
        lastCpDataPages = grp.longSensor("LastCheckpointDataPagesNumber");
        lastCpCowPages = grp.longSensor("LastCheckpointCopiedOnWritePagesNumber");
        totalCheckpointTime = grp.longAdderSensor("CheckpointTotalTime");
        lastWalSegmentRollOverTime = grp.longSensor("WalLastRollOverTime");
        storageSize = grp.longSensor("StorageSize");
        sparseStorageSize = grp.longSensor("SparseStorageSize");

        grp.sensor("WalArchiveSegments", () -> wal.walArchiveSegments());
        grp.sensor("WalFsyncTimeAverage", this::getWalFsyncTimeAverage);
        grp.sensor("WalTotalSize", this::getWalTotalSize);
        grp.sensor("UsedCheckpointBufferPages", this::getUsedCheckpointBufferPages);
        grp.sensor("UsedCheckpointBufferSize", this::getUsedCheckpointBufferSize);
        grp.sensor("CheckpointBufferSize", this::getCheckpointBufferSize);
        grp.sensor("DirtyPages", this::getDirtyPages);
        grp.sensor("PagesRead", this::getPagesRead);
        grp.sensor("PagesWritten", this::getPagesWritten);
        grp.sensor("PagesReplaced", this::getPagesReplaced);
        grp.sensor("OffHeapSize", this::getOffHeapSize);
        grp.sensor("OffheapUsedSize", this::getOffheapUsedSize);
        grp.sensor("TotalAllocatedSize", this::getTotalAllocatedSize);

        resetRates();
    }

    /** {@inheritDoc} */
    @Override public float getWalLoggingRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)walLoggingRate.getValue() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getWalWritingRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)walWritingRate.getValue() * 1000) / rateTimeInterval;
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

        long numRate = walFsyncTimeNum.getValue();

        if (numRate == 0)
            return 0;

        return (float)walFsyncTimeDuration.getValue() / numRate;
    }

    /** {@inheritDoc} */
    @Override public long getWalBuffPollSpinsRate() {
        if (!metricsEnabled)
            return 0;

        return walBuffPollSpinsNum.getValue();
    }


    /** {@inheritDoc} */
    @Override public long getLastCheckpointDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpDuration.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointLockWaitDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpLockWaitDuration.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointMarkDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpMarkDuration.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointPagesWriteDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpPagesWriteDuration.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointFsyncDuration() {
        if (!metricsEnabled)
            return 0;

        return lastCpFsyncDuration.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointTotalPagesNumber() {
        if (!metricsEnabled)
            return 0;

        return lastCpTotalPages.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDataPagesNumber() {
        if (!metricsEnabled)
            return 0;

        return lastCpDataPages.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointCopiedOnWritePagesNumber() {
        if (!metricsEnabled)
            return 0;

        return lastCpCowPages.getValue();
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

        return lastWalSegmentRollOverTime.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointTotalTime() {
        if (!metricsEnabled)
            return 0;

        return totalCheckpointTime.getValue();
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
        this.lastWalSegmentRollOverTime.set(U.currentTimeMillis());
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
        return storageSize.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getSparseStorageSize() {
        return sparseStorageSize.getValue();
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
            lastCpLockWaitDuration.set(lockWaitDuration);
            lastCpMarkDuration.set(markDuration);
            lastCpPagesWriteDuration.set(pagesWriteDuration);
            lastCpFsyncDuration.set(fsyncDuration);
            lastCpDuration.set(duration);
            lastCpTotalPages.set(totalPages);
            lastCpDataPages.set(dataPages);
            lastCpCowPages.set(cowPages);
            this.storageSize.set(storageSize);
            this.sparseStorageSize.set(sparseStorageSize);

            totalCheckpointTime.add(duration);
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
        walLoggingRate.reset((int)rateTimeInterval, subInts);
        walWritingRate.reset((int)rateTimeInterval, subInts);
        walBuffPollSpinsNum.reset((int)rateTimeInterval, subInts);

        walFsyncTimeDuration.reset((int)rateTimeInterval, subInts);
        walFsyncTimeNum.reset((int)rateTimeInterval, subInts);
    }
}
