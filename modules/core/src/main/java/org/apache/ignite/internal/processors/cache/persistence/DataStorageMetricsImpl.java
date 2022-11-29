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
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DataStorageMetricsImpl {
    /** Prefix for all data storage metrics. */
    public static final String DATASTORAGE_METRIC_PREFIX = "io.datastorage";

    /** */
    private final HitRateMetric walLoggingRate;

    /** */
    private final HitRateMetric walWritingRate;

    /** */
    private final HitRateMetric walFsyncTimeDuration;

    /** */
    private final HitRateMetric walFsyncTimeNum;

    /** */
    private final HitRateMetric walBuffPollSpinsNum;

    /** */
    private final AtomicLongMetric lastCpBeforeLockDuration;

    /** */
    private final AtomicLongMetric lastCpLockWaitDuration;

    /** */
    private final AtomicLongMetric lastCpListenersExecuteDuration;

    /** */
    private final AtomicLongMetric lastCpMarkDuration;

    /** */
    private final AtomicLongMetric lastCpLockHoldDuration;

    /** */
    private final AtomicLongMetric lastCpPagesWriteDuration;

    /** */
    private final AtomicLongMetric lastCpDuration;

    /** */
    private final AtomicLongMetric lastCpStart;

    /** */
    private final AtomicLongMetric lastCpFsyncDuration;

    /** */
    private final AtomicLongMetric lastCpWalRecordFsyncDuration;

    /** */
    private final AtomicLongMetric lastCpWriteEntryDuration;

    /** */
    private final AtomicLongMetric lastCpSplitAndSortPagesDuration;

    /** */
    private final AtomicLongMetric lastCpTotalPages;

    /** */
    private final AtomicLongMetric lastCpDataPages;

    /** */
    private final AtomicLongMetric lastCpCowPages;

    /** */
    private final long rateTimeInterval;

    /** */
    private final int subInts;

    /**
     * @deprecated Will be removed in upcoming releases.
     */
    @Deprecated
    private final boolean metricsEnabled;

    /** WAL manager. */
    @Nullable private volatile IgniteWriteAheadLogManager wal;

    /** */
    private volatile IgniteOutClosure<Long> walSizeProvider;

    /** */
    private final AtomicLongMetric lastWalSegmentRollOverTime;

    /** */
    private final AtomicLongMetric totalCheckpointTime;

    /** */
    private volatile Collection<DataRegionMetrics> regionMetrics;

    /** */
    private final AtomicLongMetric storageSize;

    /** */
    private final AtomicLongMetric sparseStorageSize;

    /** */
    private final HistogramMetricImpl cpBeforeLockHistogram;

    /** */
    private final HistogramMetricImpl cpLockWaitHistogram;

    /** */
    private final HistogramMetricImpl cpListenersExecuteHistogram;

    /** */
    private final HistogramMetricImpl cpMarkHistogram;

    /** */
    private final HistogramMetricImpl cpLockHoldHistogram;

    /** */
    private final HistogramMetricImpl cpPagesWriteHistogram;

    /** */
    private final HistogramMetricImpl cpFsyncHistogram;

    /** */
    private final HistogramMetricImpl cpWalRecordFsyncHistogram;

    /** */
    private final HistogramMetricImpl cpWriteEntryHistogram;

    /** */
    private final HistogramMetricImpl cpSplitAndSortPagesHistogram;

    /** */
    private final HistogramMetricImpl cpHistogram;

    /** Total number of logged bytes into the WAL. */
    private final LongAdderMetric walWrittenBytes;

    /** Total size of the compressed segments in bytes. */
    private final LongAdderMetric walCompressedBytes;

    /**
     * @param mmgr Metrics manager.
     * @param metricsEnabled Metrics enabled flag.
     * @param rateTimeInterval Rate time interval.
     * @param subInts Number of sub-intervals.
     */
    public DataStorageMetricsImpl(
        GridMetricManager mmgr,
        boolean metricsEnabled,
        long rateTimeInterval,
        int subInts
    ) {
        this.metricsEnabled = metricsEnabled;
        this.rateTimeInterval = rateTimeInterval;
        this.subInts = subInts;

        MetricRegistry mreg = mmgr.registry(DATASTORAGE_METRIC_PREFIX);

        walLoggingRate = mreg.hitRateMetric("WalLoggingRate",
            "Average number of WAL records per second written during the last time interval.",
            rateTimeInterval,
            subInts);

        walWritingRate = mreg.hitRateMetric(
            "WalWritingRate",
            "Average number of bytes per second written during the last time interval.",
            rateTimeInterval,
            subInts);

        walFsyncTimeDuration = mreg.hitRateMetric(
            "WalFsyncTimeDuration",
            "Total duration of fsync",
            rateTimeInterval,
            subInts);

        walFsyncTimeNum = mreg.hitRateMetric(
            "WalFsyncTimeNum",
            "Total count of fsync",
            rateTimeInterval,
            subInts);

        walBuffPollSpinsNum = mreg.hitRateMetric(
            "WalBuffPollSpinsRate",
            "WAL buffer poll spins number over the last time interval.",
            rateTimeInterval,
            subInts);

        lastCpBeforeLockDuration = mreg.longMetric("LastCheckpointBeforeLockDuration",
            "Duration of the checkpoint action before taken write lock in milliseconds.");

        lastCpLockWaitDuration = mreg.longMetric("LastCheckpointLockWaitDuration",
            "Duration of the checkpoint lock wait in milliseconds.");

        lastCpListenersExecuteDuration = mreg.longMetric("LastCheckpointListenersExecuteDuration",
            "Duration of the checkpoint execution listeners under write lock in milliseconds.");

        lastCpMarkDuration = mreg.longMetric("LastCheckpointMarkDuration",
            "Duration of the checkpoint mark in milliseconds.");

        lastCpLockHoldDuration = mreg.longMetric("LastCheckpointLockHoldDuration",
            "Duration of the checkpoint lock hold in milliseconds.");

        lastCpPagesWriteDuration = mreg.longMetric("LastCheckpointPagesWriteDuration",
            "Duration of the checkpoint pages write in milliseconds.");

        lastCpDuration = mreg.longMetric("LastCheckpointDuration",
            "Duration of the last checkpoint in milliseconds.");

        lastCpStart = mreg.longMetric("LastCheckpointStart",
            "Start timestamp of the last checkpoint.");

        lastCpFsyncDuration = mreg.longMetric("LastCheckpointFsyncDuration",
            "Duration of the sync phase of the last checkpoint in milliseconds.");

        lastCpWalRecordFsyncDuration = mreg.longMetric("LastCheckpointWalRecordFsyncDuration",
            "Duration of the WAL fsync after logging CheckpointRecord on the start of the last checkpoint " +
                "in milliseconds.");

        lastCpWriteEntryDuration = mreg.longMetric("LastCheckpointWriteEntryDuration",
            "Duration of entry buffer writing to file of the last checkpoint in milliseconds.");

        lastCpSplitAndSortPagesDuration = mreg.longMetric("LastCheckpointSplitAndSortPagesDuration",
            "Duration of splitting and sorting checkpoint pages of the last checkpoint in milliseconds.");

        lastCpTotalPages = mreg.longMetric("LastCheckpointTotalPagesNumber",
            "Total number of pages written during the last checkpoint.");

        lastCpDataPages = mreg.longMetric("LastCheckpointDataPagesNumber",
            "Total number of data pages written during the last checkpoint.");

        lastCpCowPages = mreg.longMetric("LastCheckpointCopiedOnWritePagesNumber",
            "Number of pages copied to a temporary checkpoint buffer during the last checkpoint.");

        lastWalSegmentRollOverTime = mreg.longMetric("WalLastRollOverTime",
            "Time of the last WAL segment rollover.");

        totalCheckpointTime = mreg.longMetric("CheckpointTotalTime",
            "Total duration of checkpoint");

        storageSize = mreg.longMetric("StorageSize",
            "Storage space allocated, in bytes.");

        sparseStorageSize = mreg.longMetric("SparseStorageSize",
            "Storage space allocated adjusted for possible sparsity, in bytes.");

        mreg.register("WalArchiveSegments", () -> {
            IgniteWriteAheadLogManager walMgr = wal;

            return walMgr == null ? 0 : walMgr.walArchiveSegments();
        }, "Current number of WAL segments in the WAL archive.");

        mreg.register("WalTotalSize",
            () -> {
                IgniteOutClosure<Long> walSize = walSizeProvider;

                return walSize != null ? walSize.apply() : 0;
            },
            "Total size in bytes for storage wal files.");

        long[] cpBounds = new long[] {100, 500, 1000, 5000, 30000};

        cpBeforeLockHistogram = mreg.histogram("CheckpointBeforeLockHistogram", cpBounds,
                "Histogram of checkpoint action before taken write lock duration in milliseconds.");

        cpLockWaitHistogram = mreg.histogram("CheckpointLockWaitHistogram", cpBounds,
                "Histogram of checkpoint lock wait duration in milliseconds.");

        cpListenersExecuteHistogram = mreg.histogram("CheckpointListenersExecuteHistogram", cpBounds,
                "Histogram of checkpoint execution listeners under write lock duration in milliseconds.");

        cpMarkHistogram = mreg.histogram("CheckpointMarkHistogram", cpBounds,
                "Histogram of checkpoint mark duration in milliseconds.");

        cpLockHoldHistogram = mreg.histogram("CheckpointLockHoldHistogram", cpBounds,
                "Histogram of checkpoint lock hold duration in milliseconds.");

        cpPagesWriteHistogram = mreg.histogram("CheckpointPagesWriteHistogram", cpBounds,
                "Histogram of checkpoint pages write duration in milliseconds.");

        cpFsyncHistogram = mreg.histogram("CheckpointFsyncHistogram", cpBounds,
                "Histogram of checkpoint fsync duration in milliseconds.");

        cpWalRecordFsyncHistogram = mreg.histogram("CheckpointWalRecordFsyncHistogram", cpBounds,
                "Histogram of the WAL fsync after logging CheckpointRecord on begin of checkpoint duration " +
                    "in milliseconds.");

        cpWriteEntryHistogram = mreg.histogram("CheckpointWriteEntryHistogram", cpBounds,
                "Histogram of entry buffer writing to file duration in milliseconds.");

        cpSplitAndSortPagesHistogram = mreg.histogram("CheckpointSplitAndSortPagesHistogram", cpBounds,
                "Histogram of splitting and sorting checkpoint pages duration in milliseconds.");

        cpHistogram = mreg.histogram("CheckpointHistogram", cpBounds,
                "Histogram of checkpoint duration in milliseconds.");

        walWrittenBytes = mreg.longAdderMetric(
            "WalWrittenBytes",
            "Total number of logged bytes into the WAL."
        );

        walCompressedBytes = mreg.longAdderMetric(
            "WalCompressedBytes",
            "Total size of the compressed segments in bytes."
        );

        mreg.register(
            "walFsyncTimeAverage",
            this::walFsyncTimeAverage,
            "Average WAL fsync duration in microseconds over the last time interval."
        );

        mreg.register("DirtyPages", this::dirtyPages, "Total dirty pages for the next checkpoint.");

        mreg.register("PagesRead", this::pagesRead, "The number of read pages from last restart.");

        mreg.register("PagesWritten", this::pagesWritten, "The number of written pages from last restart.");

        mreg.register("PagesReplaced", this::pagesReplaced, "The number of replaced pages from last restart.");

        mreg.register("OffHeapSize", this::offHeapSize, "Total offheap size in bytes.");

        mreg.register("OffheapUsedSize", this::offheapUsedSize, "Total used offheap size in bytes.");

        mreg.register("TotalAllocatedSize", this::totalAllocatedSize, "Total size of memory allocated in bytes.");

        mreg.register("UsedCheckpointBufferPages",
            this::usedCheckpointBufferPages,
            "Used checkpoint buffer size in pages.");

        mreg.register("UsedCheckpointBufferSize",
            this::usedCheckpointBufferSize,
            "Used checkpoint buffer size in bytes.");

        mreg.register("CheckpointBufferSize", this::checkpointBufferSize, "Checkpoint buffer size in bytes.");
    }

    /**
     * @return The average WAL fsync duration in microseconds over the last time interval.
     */
    private float walFsyncTimeAverage() {
        long numRate = walFsyncTimeNum.value();

        if (numRate == 0)
            return 0;

        return (float)walFsyncTimeDuration.value() / numRate;
    }

    /**
     * Total dirty pages for the next checkpoint.
     *
     * @return  Total dirty pages for the next checkpoint.
     */
    private long dirtyPages() {
        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long dirtyPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            dirtyPages += rm.getDirtyPages();

        return dirtyPages;
    }

    /**
     * The number of read pages from last restart.
     *
     * @return The number of read pages from last restart.
     */
    private long pagesRead() {
        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long readPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            readPages += rm.getPagesRead();

        return readPages;
    }

    /**
     * The number of written pages from last restart.
     *
     * @return The number of written pages from last restart.
     */
    private long pagesWritten() {
        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long writtenPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            writtenPages += rm.getPagesWritten();

        return writtenPages;
    }

    /**
     * The number of replaced pages from last restart.
     *
     * @return The number of replaced pages from last restart.
     */
    private long pagesReplaced() {
        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long replacedPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            replacedPages += rm.getPagesReplaced();

        return replacedPages;
    }

    /**
     * Total offheap size in bytes.
     *
     * @return Total offheap size in bytes.
     */
    private long offHeapSize() {
        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long offHeapSize = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            offHeapSize += rm.getOffHeapSize();

        return offHeapSize;
    }

    /**
     * Total used offheap size in bytes.
     *
     * @return Total used offheap size in bytes.
     */
    private long offheapUsedSize() {
        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long offHeapUsedSize = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            offHeapUsedSize += rm.getOffheapUsedSize();

        return offHeapUsedSize;
    }

    /**
     * Total size of memory allocated in bytes.
     *
     * @return Total size of memory allocated in bytes.
     */
    private long totalAllocatedSize() {
        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long totalAllocatedSize = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            totalAllocatedSize += rm.getTotalAllocatedSize();

        return totalAllocatedSize;
    }

    /**
     * Gets used checkpoint buffer size in pages.
     *
     * @return Checkpoint buffer size in pages.
     */
    private long usedCheckpointBufferPages() {
        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long usedCheckpointBufferPages = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            usedCheckpointBufferPages += rm.getUsedCheckpointBufferPages();

        return usedCheckpointBufferPages;
    }

    /**
     * Gets used checkpoint buffer size in bytes.
     *
     * @return Checkpoint buffer size in bytes.
     */
    private long usedCheckpointBufferSize() {
        Collection<DataRegionMetrics> regionMetrics0 = regionMetrics;

        if (F.isEmpty(regionMetrics0))
            return 0;

        long usedCheckpointBufferSize = 0L;

        for (DataRegionMetrics rm : regionMetrics0)
            usedCheckpointBufferSize += rm.getUsedCheckpointBufferSize();

        return usedCheckpointBufferSize;
    }

    /**
     * Checkpoint buffer size in bytes.
     *
     * @return Checkpoint buffer size in bytes.
     */
    private long checkpointBufferSize() {
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
    public void setWalSizeProvider(IgniteOutClosure<Long> walSizeProvider) {
        this.walSizeProvider = walSizeProvider;
    }

    /**
     *
     */
    public void onWallRollOver() {
        lastWalSegmentRollOverTime.value(U.currentTimeMillis());
    }

    /**
     *
     */
    public void regionMetrics(Collection<DataRegionMetrics> regionMetrics) {
        this.regionMetrics = regionMetrics;
    }

    /**
     * @return Metrics enabled flag.
     * @deprecated Will be removed in upcoming releases.
     */
    @Deprecated
    public boolean metricsEnabled() {
        return metricsEnabled;
    }

    /**
     * @param beforeLockDuration Checkpoint action before taken write lock duration.
     * @param lockWaitDuration Lock wait duration.
     * @param listenersExecuteDuration Execution listeners under write lock duration.
     * @param markDuration Mark duration.
     * @param lockHoldDuration Lock hold duration.
     * @param pagesWriteDuration Pages write duration.
     * @param fsyncDuration Total checkpoint fsync duration.
     * @param walRecordFsyncDuration Duration of WAL fsync after logging {@link CheckpointRecord} on checkpoint begin.
     * @param writeEntryDuration Duration of checkpoint entry buffer writing to file.
     * @param splitAndSortPagesDuration Duration of splitting and sorting checkpoint pages.
     * @param duration Total checkpoint duration.
     * @param start Checkpoint start time.
     * @param totalPages Total number of all pages in checkpoint.
     * @param dataPages Total number of data pages in checkpoint.
     * @param cowPages Total number of COW-ed pages in checkpoint.
     * @param storageSize Storage space allocated, in bytes.
     * @param sparseStorageSize Storage space allocated adjusted for possible sparsity, in bytes.
     */
    public void onCheckpoint(
        long beforeLockDuration,
        long lockWaitDuration,
        long listenersExecuteDuration,
        long markDuration,
        long lockHoldDuration,
        long pagesWriteDuration,
        long fsyncDuration,
        long walRecordFsyncDuration,
        long writeEntryDuration,
        long splitAndSortPagesDuration,
        long duration,
        long start,
        long totalPages,
        long dataPages,
        long cowPages,
        long storageSize,
        long sparseStorageSize
    ) {
        if (!metricsEnabled)
            return;

        lastCpBeforeLockDuration.value(beforeLockDuration);
        lastCpLockWaitDuration.value(lockWaitDuration);
        lastCpListenersExecuteDuration.value(listenersExecuteDuration);
        lastCpMarkDuration.value(markDuration);
        lastCpLockHoldDuration.value(lockHoldDuration);
        lastCpPagesWriteDuration.value(pagesWriteDuration);
        lastCpFsyncDuration.value(fsyncDuration);
        lastCpWalRecordFsyncDuration.value(walRecordFsyncDuration);
        lastCpWriteEntryDuration.value(writeEntryDuration);
        lastCpSplitAndSortPagesDuration.value(splitAndSortPagesDuration);
        lastCpDuration.value(duration);
        lastCpStart.value(start);
        lastCpTotalPages.value(totalPages);
        lastCpDataPages.value(dataPages);
        lastCpCowPages.value(cowPages);
        this.storageSize.value(storageSize);
        this.sparseStorageSize.value(sparseStorageSize);

        totalCheckpointTime.add(duration);

        cpBeforeLockHistogram.value(beforeLockDuration);
        cpLockWaitHistogram.value(lockWaitDuration);
        cpListenersExecuteHistogram.value(listenersExecuteDuration);
        cpMarkHistogram.value(markDuration);
        cpLockHoldHistogram.value(lockHoldDuration);
        cpPagesWriteHistogram.value(pagesWriteDuration);
        cpFsyncHistogram.value(fsyncDuration);
        cpWalRecordFsyncHistogram.value(walRecordFsyncDuration);
        cpWriteEntryHistogram.value(writeEntryDuration);
        cpSplitAndSortPagesHistogram.value(splitAndSortPagesDuration);
        cpHistogram.value(duration);
    }

    /**
     * Callback on logging a record to a WAL.
     *
     * @param size Record size in bytes.
     */
    public void onWalRecordLogged(long size) {
        walLoggingRate.increment();

        walWrittenBytes.add(size);
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

    /** */
    private void resetRates() {
        walLoggingRate.reset(rateTimeInterval, subInts);
        walWritingRate.reset(rateTimeInterval, subInts);
        walBuffPollSpinsNum.reset(rateTimeInterval, subInts);

        walFsyncTimeDuration.reset(rateTimeInterval, subInts);
        walFsyncTimeNum.reset(rateTimeInterval, subInts);
    }

    /**
     * Callback on compression of a WAL segment.
     *
     * @param size Size of the compressed segment in bytes.
     */
    public void onWalSegmentCompressed(long size) {
        walCompressedBytes.add(size);
    }
}
