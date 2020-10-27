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

package org.apache.ignite.internal.processors.metric.sources;

import java.util.Collection;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;

/**
 * Data storage metric source.
 */
public class DataStorageMetricSource extends AbstractMetricSource<DataStorageMetricSource.Holder> {
    /** Prefix for all data storage metrics. */
    public static final String DATASTORAGE_METRIC_PREFIX = "io.datastorage";

    /** WAL manager. */
    private volatile IgniteWriteAheadLogManager wal;

    //TODO: Reconsider approach: replace closure to WAL manager functionality.
    /** Closure that provides info about WAL size on disk. */
    private volatile IgniteOutClosure<Long> walSizeProvider;

    /** */
    //TODO: Reconsider approach.
    private volatile Collection<DataRegionMetricSource> regionMetricSrcs;

    /** */
    private final long rateTimeInterval;

    /** */
    private final int subInts;

    /**
     * Creates data storage metric source.
     *
     * @param ctx Kernal context.
     */
    public DataStorageMetricSource(GridKernalContext ctx) {
        super(DATASTORAGE_METRIC_PREFIX, ctx);

        DataStorageConfiguration dsCfg = ctx.config().getDataStorageConfiguration();

        rateTimeInterval = dsCfg.getMetricsRateTimeInterval();
        subInts = dsCfg.getMetricsSubIntervalCount();
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        wal = ctx().cache().context().wal();

        hldr.walLoggingRate = bldr.hitRateMetric("WalLoggingRate",
                "Average number of WAL records per second written during the last time interval.",
                rateTimeInterval,
                subInts);

        hldr.walWritingRate = bldr.hitRateMetric(
                "WalWritingRate",
                "Average number of bytes per second written during the last time interval.",
                rateTimeInterval,
                subInts);

        hldr.walFsyncTimeDuration = bldr.hitRateMetric(
                "WalFsyncTimeDuration",
                "Total duration of fsync",
                rateTimeInterval,
                subInts);

        hldr.walFsyncTimeNum = bldr.hitRateMetric(
                "WalFsyncTimeNum",
                "Total count of fsync",
                rateTimeInterval,
                subInts);

        hldr.walBuffPollSpinsNum = bldr.hitRateMetric(
                "WalBuffPollSpinsRate",
                "WAL buffer poll spins number over the last time interval.",
                rateTimeInterval,
                subInts);

        hldr.lastCpLockWaitDuration = bldr.longMetric("LastCheckpointLockWaitDuration",
                "Duration of the checkpoint lock wait in milliseconds.");

        hldr.lastCpMarkDuration = bldr.longMetric("LastCheckpointMarkDuration",
                "Duration of the checkpoint lock wait in milliseconds.");

        hldr.lastCpPagesWriteDuration = bldr.longMetric("LastCheckpointPagesWriteDuration",
                "Duration of the checkpoint pages write in milliseconds.");

        hldr.lastCpDuration = bldr.longMetric("LastCheckpointDuration",
                "Duration of the last checkpoint in milliseconds.");

        hldr.lastCpFsyncDuration = bldr.longMetric("LastCheckpointFsyncDuration",
                "Duration of the sync phase of the last checkpoint in milliseconds.");

        hldr.lastCpTotalPages = bldr.longMetric("LastCheckpointTotalPagesNumber",
                "Total number of pages written during the last checkpoint.");

        hldr.lastCpDataPages = bldr.longMetric("LastCheckpointDataPagesNumber",
                "Total number of data pages written during the last checkpoint.");

        hldr.lastCpCowPages = bldr.longMetric("LastCheckpointCopiedOnWritePagesNumber",
                "Number of pages copied to a temporary checkpoint buffer during the last checkpoint.");

        hldr.lastWalSegmentRollOverTime = bldr.longMetric("WalLastRollOverTime",
                "Time of the last WAL segment rollover.");

        hldr.totalCheckpointTime = bldr.longMetric("CheckpointTotalTime",
                "Total duration of checkpoint");

        hldr.storageSize = bldr.longMetric("StorageSize",
                "Storage space allocated, in bytes.");

        hldr.sparseStorageSize = bldr.longMetric("SparseStorageSize",
                "Storage space allocated adjusted for possible sparsity, in bytes.");

        bldr.register("WalArchiveSegments",
                this::walArchiveSegments,
                "Current number of WAL segments in the WAL archive.");

        bldr.register("WalTotalSize",
                this::walTotalSize,
                "Total size in bytes for storage wal files.");
    }

    /**
     * Sets collection of data regions metric sources.
     */
    //TODO: Unsafe and may lead to errors. Reconsider approach if possible.
    public void regionMetrics(Collection<DataRegionMetricSource> regionMetricSrcs) {
        this.regionMetricSrcs = regionMetricSrcs;
    }

    /**
     * Returns the average number of WAL records per second written during the last time interval.
     * <p>
     * The length of time interval is configured via {@link DataStorageConfiguration#setMetricsRateTimeInterval(long)}
     * configuration property.
     * The number of subintervals is configured via {@link DataStorageConfiguration#setMetricsSubIntervalCount(int)}
     * configuration property.
     *
     * @return Average number of WAL records per second written during the last time interval
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public float walLoggingRate() {
        Holder hldr = holder();

        return hldr != null ? ((float)hldr.walLoggingRate.value() * 1000) / rateTimeInterval : Float.NaN;
    }

    /**
     * Returns the average number of bytes per second written during the last time interval.
     * The length of time interval is configured via {@link DataStorageConfiguration#setMetricsRateTimeInterval(long)}
     * configuration property.
     * The number of subintervals is configured via {@link DataStorageConfiguration#setMetricsSubIntervalCount(int)}
     * configuration property.
     *
     * @return Average number of bytes per second written during the last time interval.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public float walWritingRate() {
        Holder hldr = holder();

        return hldr != null ? ((float)hldr.walWritingRate.value() * 1000) / rateTimeInterval : -1;
    }

    /**
     * Returns the current number of WAL segments in the WAL archive.
     *
     * @return Current number of WAL segments in the WAL archive.
     */
    //TODO: Should be private after removing usage from DataStorageMetricsImpl.
    public int walArchiveSegments() {
        Holder hldr = holder();

        if (hldr == null)
            return -1;

        return wal != null ? wal.walArchiveSegments() : -1;
    }

    /**
     * Returns the average WAL fsync duration in microseconds over the last time interval.
     * <p>
     * The length of time interval is configured via {@link DataStorageConfiguration#setMetricsRateTimeInterval(long)}
     * configuration property.
     * The number of subintervals is configured via {@link DataStorageConfiguration#setMetricsSubIntervalCount(int)}
     * configuration property.
     *
     * @return Average WAL fsync duration in microseconds over the last time interval.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed. Could be
     * calculated by external tools.
     */
    @Deprecated
    public float walFsyncTimeAverage() {
        Holder hldr = holder();

        if (hldr == null)
            return -1;

        long numRate = hldr.walFsyncTimeNum.value();

        if (numRate == 0)
            return 0;

        return (float)hldr.walFsyncTimeDuration.value() / numRate;
    }

    /**
     * Returns WAL buffer poll spins number over the last time interval.
     * <p>
     * The length of time interval is configured via {@link DataStorageConfiguration#setMetricsRateTimeInterval(long)}
     * configuration property.
     * The number of subintervals is configured via {@link DataStorageConfiguration#setMetricsSubIntervalCount(int)}
     * configuration property.
     *
     * @return WAL buffer poll spins number over the last time interval.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long walBuffPollSpinsRate() {
        Holder hldr = holder();

        return hldr != null ? hldr.walBuffPollSpinsNum.value() : -1;
    }

    /**
     * Returns the duration of the last checkpoint in milliseconds.
     *
     * @return Total checkpoint duration in milliseconds.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long lastCheckpointDuration() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastCpDuration.value() : -1;
    }

    /**
     * Returns the duration of last checkpoint lock wait in milliseconds.
     *
     * @return Checkpoint lock wait time in milliseconds.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long lastCheckpointLockWaitDuration() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastCpLockWaitDuration.value() : -1;
    }

    /**
     * Returns the duration of last checkpoint mark phase in milliseconds.
     *
     * @return Checkpoint mark duration in milliseconds.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long lastCheckpointMarkDuration() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastCpMarkDuration.value() : -1;
    }

    /**
     * Returns the duration of last checkpoint pages write phase in milliseconds.
     *
     * @return Checkpoint pages write phase in milliseconds.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long lastCheckpointPagesWriteDuration() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastCpPagesWriteDuration.value() : -1;
    }

    /**
     * Returns the duration of the sync phase of the last checkpoint in milliseconds.
     *
     * @return Checkpoint fsync time in milliseconds.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long lastCheckpointFsyncDuration() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastCpFsyncDuration.value() : -1;
    }

    /**
     * Returns the total number of pages written during the last checkpoint.
     *
     * @return Total number of pages written during the last checkpoint.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long lastCheckpointTotalPagesNumber() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastCpTotalPages.value() : -1;
    }

    /**
     * Returns the number of data pages written during the last checkpoint.
     *
     * @return Total number of data pages written during the last checkpoint.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long lastCheckpointDataPagesNumber() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastCpDataPages.value() : -1;
    }

    /**
     * Returns the number of pages copied to a temporary checkpoint buffer during the last checkpoint.
     *
     * @return Total number of pages copied to a temporary checkpoint buffer during the last checkpoint.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long lastCheckpointCopiedOnWritePagesNumber() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastCpCowPages.value() : -1;
    }

    /**
     * Returns total size in bytes for storage wal files.
     *
     * @return Total size in bytes for storage wal files.
     */
    public long walTotalSize() {
        if (!enabled())
            return -1;

        IgniteOutClosure<Long> walSize = walSizeProvider;

        return walSize != null ? walSize.apply() : -1;
    }

    /**
     * Return time of the last WAL segment rollover.
     *
     * @return Time of the last WAL segment rollover.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long walLastRollOverTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.lastWalSegmentRollOverTime.value() : -1;
    }

    /**
     * Return total checkpoint time from last restart.
     *
     * @return Total checkpoint time from last restart.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long checkpointTotalTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.totalCheckpointTime.value() : -1;
    }

    /**
     * @param walSizeProvider Wal size provider.
     */
    //TODO: Replace by direct calls from WAL.
    public void walSizeProvider(IgniteOutClosure<Long> walSizeProvider) {
        this.walSizeProvider = walSizeProvider;
    }

    public void onWallRollOver() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.lastWalSegmentRollOverTime.value(U.currentTimeMillis());
    }

    /**
     * Updates fsync time value.
     *
     * @param nanoTime Fsync nano time.
     */
    public void onFsync(long nanoTime) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.walFsyncTimeDuration.add(nanoTime / 1000);

            hldr.walFsyncTimeNum.increment();
        }
    }

    /**
     * Updates WAL bytes written value.
     *
     * @param size Size written.
     */
    public void onWalBytesWritten(int size) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.walWritingRate.add(size);
    }

    /**
     * @param num Number.
     */
    //TODO: It seems that this value could be removed.
    public void onBuffPollSpin(int num) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.walBuffPollSpinsNum.add(num);
    }

    /**
     * Updates amount of WAL records written.
     */
    public void onWalRecordLogged() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.walLoggingRate.increment();
    }

    /**
     * Updates checkpoint related metrics.
     *
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
        Holder hldr = holder();

        if (hldr != null) {
            hldr.lastCpLockWaitDuration.value(lockWaitDuration);
            hldr.lastCpMarkDuration.value(markDuration);
            hldr.lastCpPagesWriteDuration.value(pagesWriteDuration);
            hldr.lastCpFsyncDuration.value(fsyncDuration);
            hldr.lastCpDuration.value(duration);
            hldr.lastCpTotalPages.value(totalPages);
            hldr.lastCpDataPages.value(dataPages);
            hldr.lastCpCowPages.value(cowPages);
            hldr.storageSize.value(storageSize);
            hldr.sparseStorageSize.value(sparseStorageSize);
            hldr.totalCheckpointTime.add(duration);
        }
    }

    /**
     * Returns storage space allocated in bytes.
     *
     * @return Storage space allocated in bytes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long storageSize() {
        Holder hldr = holder();

        return hldr != null ? hldr.storageSize.value() : -1;
    }

    /**
     * Returns storage space allocated adjusted for possible sparsity in bytes.
     *
     * May produce unstable or even incorrect result on some file systems (e.g. XFS).
     * Known to work correctly on Ext4 and Btrfs.
     *
     * @return Storage space allocated adjusted for possible sparsity in bytes
     *         or negative value is not supported.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long sparseStorageSize() {
        Holder hldr = holder();

        return hldr != null ? hldr.sparseStorageSize.value() : -1;
    }

    /**
     * Returnds Total dirty pages for the next checkpoint.
     *
     * @return Total dirty pages for the next checkpoint.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long dirtyPages() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long dirtyPages = 0L;

        for (DataRegionMetricSource src : srcs0)
            dirtyPages += src.dirtyPages();

        return dirtyPages;
    }

    /**
     * Returns the number of read pages from last restart.
     *
     * @return The number of read pages from last restart.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long pagesRead() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long readPages = 0L;

        for (DataRegionMetricSource src : srcs0)
            readPages += src.pagesRead();

        return readPages;
    }

    /**
     * Returns the number of written pages from last restart.
     *
     * @return The number of written pages from last restart.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long pagesWritten() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long writtenPages = 0L;

        for (DataRegionMetricSource src : srcs0)
            writtenPages += src.pagesWritten();

        return writtenPages;
    }

    /**
     * Returns the number of replaced pages from last restart.
     *
     * @return The number of replaced pages from last restart.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long pagesReplaced() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long replacedPages = 0L;

        for (DataRegionMetricSource src : srcs0)
            replacedPages += src.pagesReplaced();

        return replacedPages;
    }

    /**
     * Return total offheap size in bytes.
     *
     * @return Total offheap size in bytes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long offHeapSize() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long offHeapSize = 0L;

        for (DataRegionMetricSource src : srcs0)
            offHeapSize += src.offHeapSize();

        return offHeapSize;
    }

    /**
     * Returns total used offheap size in bytes.
     *
     * @return Total used offheap size in bytes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long offheapUsedSize() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long offHeapUsedSize = 0L;

        for (DataRegionMetricSource src : srcs0)
            offHeapUsedSize += src.offheapUsedSize();

        return offHeapUsedSize;
    }

    /**
     * Returns total size of memory allocated in bytes.
     *
     * @return Total size of memory allocated in bytes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long totalAllocatedSize() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long totalAllocatedSize = 0L;

        for (DataRegionMetricSource src : srcs0)
            totalAllocatedSize += src.totalAllocatedSize();

        return totalAllocatedSize;
    }

    /**
     * Returns used checkpoint buffer size in pages.
     *
     * @return Checkpoint buffer size in pages.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long usedCheckpointBufferPages() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long usedCheckpointBufPages = 0L;

        for (DataRegionMetricSource src : srcs0)
            usedCheckpointBufPages += src.usedCheckpointBufferPages();

        return usedCheckpointBufPages;
    }

    /**
     * Returns used checkpoint buffer size in bytes.
     *
     * @return Checkpoint buffer size in bytes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long usedCheckpointBufferSize() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long usedCheckpointBufSize = 0L;

        for (DataRegionMetricSource src : srcs0)
            usedCheckpointBufSize += src.usedCheckpointBufferSize();

        return usedCheckpointBufSize;
    }

    /**
     * Returns checkpoint buffer size in bytes.
     *
     * @return Checkpoint buffer size in bytes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     * Could be calculated by external tools (aggregated value).
     */
    @Deprecated
    public long checkpointBufferSize() {
        if (!enabled())
            return -1;

        Collection<DataRegionMetricSource> srcs0 = regionMetricSrcs;

        if (F.isEmpty(srcs0))
            return 0;

        long checkpointBufSize = 0L;

        for (DataRegionMetricSource src : srcs0)
            checkpointBufSize += src.checkpointBufferSize();

        return checkpointBufSize;
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** */
        private HitRateMetric walLoggingRate;

        /** */
        private HitRateMetric walWritingRate;

        /** */
        private HitRateMetric walFsyncTimeDuration;

        /** */
        private HitRateMetric walFsyncTimeNum;

        /** */
        private HitRateMetric walBuffPollSpinsNum;

        /** */
        private AtomicLongMetric lastCpLockWaitDuration;

        /** */
        private AtomicLongMetric lastCpMarkDuration;

        /** */
        private AtomicLongMetric lastCpPagesWriteDuration;

        /** */
        private AtomicLongMetric lastCpDuration;

        /** */
        private AtomicLongMetric lastCpFsyncDuration;

        /** */
        private AtomicLongMetric lastCpTotalPages;

        /** */
        private AtomicLongMetric lastCpDataPages;

        /** */
        private AtomicLongMetric lastCpCowPages;

        /** */
        private AtomicLongMetric lastWalSegmentRollOverTime;

        /** */
        private AtomicLongMetric totalCheckpointTime;

        /** */
        private AtomicLongMetric storageSize;

        /** */
        private AtomicLongMetric sparseStorageSize;
    }
}
