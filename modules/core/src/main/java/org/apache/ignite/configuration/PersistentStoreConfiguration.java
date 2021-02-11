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
package org.apache.ignite.configuration;

import java.io.Serializable;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.MetricsMxBean;

/**
 * Configures Apache Ignite Persistent store.
 * @deprecated Use {@link DataStorageConfiguration} instead.
 */
@Deprecated
public class PersistentStoreConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int DFLT_CHECKPOINTING_FREQ = 180000;

    /** Lock default wait time, 10 sec. */
    public static final int DFLT_LOCK_WAIT_TIME = 10 * 1000;

    /** */
    public static final boolean DFLT_METRICS_ENABLED = false;

    /** Default amount of sub intervals to calculate rate-based metric. */
    public static final int DFLT_SUB_INTERVALS = 5;

    /** Default length of interval over which rate-based metric is calculated. */
    public static final int DFLT_RATE_TIME_INTERVAL_MILLIS = 60_000;

    /** Default number of checkpointing threads. */
    public static final int DFLT_CHECKPOINTING_THREADS = 4;

    /** Default checkpoint write order. */
    public static final CheckpointWriteOrder DFLT_CHECKPOINT_WRITE_ORDER = CheckpointWriteOrder.SEQUENTIAL;

    /** Default number of checkpoints to be kept in WAL after checkpoint is finished */
    public static final int DFLT_WAL_HISTORY_SIZE = 20;

    /** */
    public static final int DFLT_WAL_SEGMENTS = 10;

    /** Default WAL file segment size, 64MBytes */
    public static final int DFLT_WAL_SEGMENT_SIZE = 64 * 1024 * 1024;

    /** Default wal mode. */
    public static final WALMode DFLT_WAL_MODE = WALMode.LOG_ONLY;

    /** Default Wal flush frequency. */
    public static final int DFLT_WAL_FLUSH_FREQ = 2000;

    /** Default wal fsync delay. */
    public static final int DFLT_WAL_FSYNC_DELAY = 1000;

    /** Default wal record iterator buffer size. */
    public static final int DFLT_WAL_RECORD_ITERATOR_BUFFER_SIZE = 64 * 1024 * 1024;

    /** Default wal always write full pages. */
    public static final boolean DFLT_WAL_ALWAYS_WRITE_FULL_PAGES = false;

    /** Default wal directory. */
    public static final String DFLT_WAL_STORE_PATH = "db/wal";

    /** Default wal archive directory. */
    public static final String DFLT_WAL_ARCHIVE_PATH = "db/wal/archive";

    /** Default write throttling enabled. */
    public static final boolean DFLT_WRITE_THROTTLING_ENABLED = false;

    /** */
    private String persistenceStorePath;

    /** Checkpointing frequency. */
    private long checkpointingFreq = DFLT_CHECKPOINTING_FREQ;

    /** Lock wait time, in milliseconds. */
    private long lockWaitTime = DFLT_LOCK_WAIT_TIME;

    /** */
    private long checkpointingPageBufSize;

    /** */
    private int checkpointingThreads = DFLT_CHECKPOINTING_THREADS;

    /** Checkpoint write order. */
    private CheckpointWriteOrder checkpointWriteOrder = DFLT_CHECKPOINT_WRITE_ORDER;

    /** Number of checkpoints to keep */
    private int walHistSize = DFLT_WAL_HISTORY_SIZE;

    /** Number of work WAL segments. */
    private int walSegments = DFLT_WAL_SEGMENTS;

    /** Size of one WAL segment in bytes. 64 Mb is used by default.  Maximum value is 2Gb */
    private int walSegmentSize = DFLT_WAL_SEGMENT_SIZE;

    /** Directory where WAL is stored (work directory) */
    private String walStorePath = DFLT_WAL_STORE_PATH;

    /** WAL archive path. */
    private String walArchivePath = DFLT_WAL_ARCHIVE_PATH;

    /** Metrics enabled flag. */
    private boolean metricsEnabled = DFLT_METRICS_ENABLED;

    /** Wal mode. */
    private WALMode walMode = DFLT_WAL_MODE;

    /** WAl buffer size. */
    private int walBuffSize/* = DFLT_WAL_BUFF_SIZE*/;

    /** Wal flush frequency in milliseconds. */
    private long walFlushFreq = DFLT_WAL_FLUSH_FREQ;

    /** Wal fsync delay. */
    private long walFsyncDelay = DFLT_WAL_FSYNC_DELAY;

    /** Wal record iterator buffer size. */
    private int walRecordIterBuffSize = DFLT_WAL_RECORD_ITERATOR_BUFFER_SIZE;

    /** Always write full pages. */
    private boolean alwaysWriteFullPages = DFLT_WAL_ALWAYS_WRITE_FULL_PAGES;

    /** Factory to provide I/O interface for files */
    private FileIOFactory fileIOFactory =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_USE_ASYNC_FILE_IO_FACTORY, false) ?
            new AsyncFileIOFactory() : new RandomAccessFileIOFactory();

    /**
     * Number of sub-intervals the whole {@link #setRateTimeInterval(long)} will be split into to calculate
     * rate-based metrics.
     * <p>
     * Setting it to a bigger value will result in more precise calculation and smaller drops of
     * rate-based metrics when next sub-interval has to be recycled but introduces bigger
     * calculation overhead.
     */
    private int subIntervals = DFLT_SUB_INTERVALS;

    /** Time interval (in milliseconds) for rate-based metrics. */
    private long rateTimeInterval = DFLT_RATE_TIME_INTERVAL_MILLIS;

    /**
     *  Time interval (in milliseconds) for running auto archiving for incompletely WAL segment
     */
    private long walAutoArchiveAfterInactivity = -1;

    /**
     * If true, threads that generate dirty pages too fast during ongoing checkpoint will be throttled.
     */
    private boolean writeThrottlingEnabled = DFLT_WRITE_THROTTLING_ENABLED;

    /**
     * Returns a path the root directory where the Persistent Store will persist data and indexes.
     */
    public String getPersistentStorePath() {
        return persistenceStorePath;
    }

    /**
     * Sets a path to the root directory where the Persistent Store will persist data and indexes.
     * By default the Persistent Store's files are located under Ignite work directory.
     *
     * @param persistenceStorePath Persistence store path.
     */
    public PersistentStoreConfiguration setPersistentStorePath(String persistenceStorePath) {
        this.persistenceStorePath = persistenceStorePath;

        return this;
    }

    /**
     * Gets checkpointing frequency.
     *
     * @return checkpointing frequency in milliseconds.
     */
    public long getCheckpointingFrequency() {
        return checkpointingFreq <= 0 ? DFLT_CHECKPOINTING_FREQ : checkpointingFreq;
    }

    /**
     * Sets the checkpointing frequency which is a minimal interval when the dirty pages will be written
     * to the Persistent Store. If the rate is high, checkpointing will be triggered more frequently.
     *
     * @param checkpointingFreq checkpointing frequency in milliseconds.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointingFrequency(long checkpointingFreq) {
        this.checkpointingFreq = checkpointingFreq;

        return this;
    }

    /**
     * Gets amount of memory allocated for a checkpointing temporary buffer.
     *
     * @return Checkpointing page buffer size in bytes or {@code 0} for Ignite
     *      to choose the buffer size automatically.
     */
    public long getCheckpointingPageBufferSize() {
        return checkpointingPageBufSize;
    }

    /**
     * Sets amount of memory allocated for the checkpointing temporary buffer. The buffer is used to create temporary
     * copies of pages that are being written to disk and being update in parallel while the checkpointing is in
     * progress.
     *
     * @param checkpointingPageBufSize Checkpointing page buffer size in bytes or {@code 0} for Ignite to
     *      choose the buffer size automatically.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointingPageBufferSize(long checkpointingPageBufSize) {
        this.checkpointingPageBufSize = checkpointingPageBufSize;

        return this;
    }

    /**
     * Gets a number of threads to use for the checkpointing purposes.
     *
     * @return Number of checkpointing threads.
     */
    public int getCheckpointingThreads() {
        return checkpointingThreads;
    }

    /**
     * Sets a number of threads to use for the checkpointing purposes.
     *
     * @param checkpointingThreads Number of checkpointing threads. Four threads are used by default.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setCheckpointingThreads(int checkpointingThreads) {
        this.checkpointingThreads = checkpointingThreads;

        return this;
    }

    /**
     * Time out in milliseonds to wait when acquiring persistence store lock file before failing the
     * local node.
     *
     * @return Lock wait time in milliseconds.
     */
    public long getLockWaitTime() {
        return lockWaitTime;
    }

    /**
     * Time out in milliseconds  to wait when acquiring persistence store lock file before failing the
     * local node.
     *
     * @param lockWaitTime Lock wait time in milliseconds.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setLockWaitTime(long lockWaitTime) {
        this.lockWaitTime = lockWaitTime;

        return this;
    }

    /**
     * Gets a total number of checkpoints to keep in the WAL history.
     *
     * @return Number of checkpoints to keep in WAL after a checkpoint is finished.
     */
    public int getWalHistorySize() {
        return walHistSize <= 0 ? DFLT_WAL_HISTORY_SIZE : walHistSize;
    }

    /**
     * Sets a total number of checkpoints to keep in the WAL history.
     *
     * @param walHistSize Number of checkpoints to keep after a checkpoint is finished.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalHistorySize(int walHistSize) {
        this.walHistSize = walHistSize;

        return this;
    }

    /**
     * Gets a number of WAL segments to work with.
     *
     * @return Number of work WAL segments.
     */
    public int getWalSegments() {
        return walSegments <= 0 ? DFLT_WAL_SEGMENTS : walSegments;
    }

    /**
     * Sets a number of WAL segments to work with. For performance reasons,
     * the whole WAL is split into files of fixed length called segments.
     *
     * @param walSegments Number of WAL segments.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalSegments(int walSegments) {
        this.walSegments = walSegments;

        return this;
    }

    /**
     * Gets size of a WAL segment in bytes.
     *
     * @return WAL segment size.
     */
    public int getWalSegmentSize() {
        return walSegmentSize <= 0 ? DFLT_WAL_SEGMENT_SIZE : walSegmentSize;
    }

    /**
     * Sets size of a WAL segment.
     *
     * @param walSegmentSize WAL segment size. 64 MB is used by default.  Maximum value is 2Gb
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalSegmentSize(int walSegmentSize) {
        this.walSegmentSize = walSegmentSize;

        return this;
    }

    /**
     * Gets a path to the directory where WAL is stored.
     *
     * @return WAL persistence path, absolute or relative to Ignite work directory.
     */
    public String getWalStorePath() {
        return walStorePath;
    }

    /**
     * Sets a path to the directory where WAL is stored . If this path is relative, it will be resolved
     * relatively to Ignite work directory.
     *
     * @param walStorePath WAL persistence path, absolute or relative to Ignite work directory.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalStorePath(String walStorePath) {
        this.walStorePath = walStorePath;

        return this;
    }

    /**
     * Gets a path to the WAL archive directory.
     *
     *  @return WAL archive directory.
     */
    public String getWalArchivePath() {
        return walArchivePath;
    }

    /**
     * Sets a path for the WAL archive directory. Every WAL segment will be fully copied to this directory before
     * it can be reused for WAL purposes.
     *
     * @param walArchivePath WAL archive directory.
     * @return {@code this} for chaining.
     */
    public PersistentStoreConfiguration setWalArchivePath(String walArchivePath) {
        this.walArchivePath = walArchivePath;

        return this;
    }

    /**
     * Gets flag indicating whether persistence metrics collection is enabled.
     * Default value is {@link #DFLT_METRICS_ENABLED}.
     *
     * @return Metrics enabled flag.
     */
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /**
     * Sets flag indicating whether persistence metrics collection is enabled.
     *
     * @param metricsEnabled Metrics enabled flag.
     */
    public PersistentStoreConfiguration setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;

        return this;
    }

    /**
     * Gets flag indicating whether write throttling is enabled.
     */
    public boolean isWriteThrottlingEnabled() {
        return writeThrottlingEnabled;
    }

    /**
     * Sets flag indicating whether write throttling is enabled.
     *
     * @param writeThrottlingEnabled Write throttling enabled flag.
     */
    public PersistentStoreConfiguration setWriteThrottlingEnabled(boolean writeThrottlingEnabled) {
        this.writeThrottlingEnabled = writeThrottlingEnabled;

        return this;
    }

    /**
     * Gets the length of the time interval for rate-based metrics. This interval defines a window over which
     * hits will be tracked. Default value is {@link #DFLT_RATE_TIME_INTERVAL_MILLIS}.
     *
     * @return Time interval in milliseconds.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public long getRateTimeInterval() {
        return rateTimeInterval;
    }

    /**
     * Sets the length of the time interval for rate-based metrics. This interval defines a window over which
     * hits will be tracked.
     *
     * @param rateTimeInterval Time interval in milliseconds.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public PersistentStoreConfiguration setRateTimeInterval(long rateTimeInterval) {
        this.rateTimeInterval = rateTimeInterval;

        return this;
    }

    /**
     * Gets the number of sub-intervals to split the {@link #getRateTimeInterval()} into to track the update history.
     * Default value is {@link #DFLT_SUB_INTERVALS}.
     *
     * @return The number of sub-intervals for history tracking.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public int getSubIntervals() {
        return subIntervals;
    }

    /**
     * Sets the number of sub-intervals to split the {@link #getRateTimeInterval()} into to track the update history.
     *
     * @param subIntervals The number of sub-intervals for history tracking.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public PersistentStoreConfiguration setSubIntervals(int subIntervals) {
        this.subIntervals = subIntervals;

        return this;
    }

    /**
     * Type define behavior wal fsync.
     * Different type provide different guarantees for consistency.
     *
     * @return WAL mode.
     */
    public WALMode getWalMode() {
        return walMode == null ? DFLT_WAL_MODE : walMode;
    }

    /**
     * @param walMode Wal mode.
     */
    public PersistentStoreConfiguration setWalMode(WALMode walMode) {
        if (walMode == WALMode.DEFAULT)
            walMode = WALMode.FSYNC;

        this.walMode = walMode;

        return this;
    }

    /**
     * Property defines size of WAL buffer.
     * Each WAL record will be serialized to this buffer before write in WAL file.
     *
     * @return WAL buffer size.
     * @deprecated Instead {@link #getWalBufferSize()} should be used.
     */
    @Deprecated
    public int getTlbSize() {
        return getWalBufferSize();
    }

    /**
     * @param tlbSize WAL buffer size.
     * @deprecated Instead {@link #setWalBufferSize(int walBuffSize)} should be used.
     */
    @Deprecated
    public PersistentStoreConfiguration setTlbSize(int tlbSize) {
        return setWalBufferSize(tlbSize);
    }

    /**
     * Property defines size of WAL buffer.
     * Each WAL record will be serialized to this buffer before write in WAL file.
     *
     * @return WAL buffer size.
     */
    @Deprecated
    public int getWalBufferSize() {
        return walBuffSize <= 0 ? getWalSegmentSize() / 4 : walBuffSize;
    }

    /**
     * @param walBuffSize WAL buffer size.
     */
    @Deprecated
    public PersistentStoreConfiguration setWalBufferSize(int walBuffSize) {
        this.walBuffSize = walBuffSize;

        return this;
    }

    /**
     *  This property define how often WAL will be fsync-ed in {@code BACKGROUND} mode. Ignored for
     *  all other WAL modes.
     *
     * @return WAL flush frequency, in milliseconds.
     */
    public long getWalFlushFrequency() {
        return walFlushFreq;
    }

    /**
     *  This property define how often WAL will be fsync-ed in {@code BACKGROUND} mode. Ignored for
     *  all other WAL modes.
     *
     * @param walFlushFreq WAL flush frequency, in milliseconds.
     */
    public PersistentStoreConfiguration setWalFlushFrequency(long walFlushFreq) {
        this.walFlushFreq = walFlushFreq;

        return this;
    }

    /**
     * Gets the fsync delay, in nanoseconds.
     */
    public long getWalFsyncDelayNanos() {
        return walFsyncDelay <= 0 ? DFLT_WAL_FSYNC_DELAY : walFsyncDelay;
    }

    /**
     * @param walFsyncDelayNanos Wal fsync delay, in nanoseconds.
     */
    public PersistentStoreConfiguration setWalFsyncDelayNanos(long walFsyncDelayNanos) {
        walFsyncDelay = walFsyncDelayNanos;

        return this;
    }

    /**
     *  Property define how many bytes iterator read from
     *  disk (for one reading), during go ahead wal.
     *
     * @return Record iterator buffer size.
     */
    public int getWalRecordIteratorBufferSize() {
        return walRecordIterBuffSize <= 0 ? DFLT_WAL_RECORD_ITERATOR_BUFFER_SIZE : walRecordIterBuffSize;
    }

    /**
     * @param walRecordIterBuffSize Wal record iterator buffer size.
     */
    public PersistentStoreConfiguration setWalRecordIteratorBufferSize(int walRecordIterBuffSize) {
        this.walRecordIterBuffSize = walRecordIterBuffSize;

        return this;
    }

    /**
     *
     */
    public boolean isAlwaysWriteFullPages() {
        return alwaysWriteFullPages;
    }

    /**
     * @param alwaysWriteFullPages Always write full pages.
     */
    public PersistentStoreConfiguration setAlwaysWriteFullPages(boolean alwaysWriteFullPages) {
        this.alwaysWriteFullPages = alwaysWriteFullPages;

        return this;
    }

    /**
     * Factory to provide implementation of FileIO interface
     * which is used for any file read/write operations
     *
     * @return File I/O factory
     */
    public FileIOFactory getFileIOFactory() {
        return fileIOFactory;
    }

    /**
     * @param fileIOFactory File I/O factory
     */
    public PersistentStoreConfiguration setFileIOFactory(FileIOFactory fileIOFactory) {
        this.fileIOFactory = fileIOFactory;

        return this;
    }

    /**
     * <b>Note:</b> setting this value with {@link WALMode#FSYNC} may generate file size overhead for WAL segments in case
     * grid is used rarely.
     *
     * @param walAutoArchiveAfterInactivity time in millis to run auto archiving segment (even if incomplete) after last
     * record logging. <br> Positive value enables incomplete segment archiving after timeout (inactivity). <br> Zero or
     * negative  value disables auto archiving.
     * @return current configuration instance for chaining
     */
    public PersistentStoreConfiguration setWalAutoArchiveAfterInactivity(long walAutoArchiveAfterInactivity) {
        this.walAutoArchiveAfterInactivity = walAutoArchiveAfterInactivity;

        return this;
    }

    /**
     * @return time in millis to run auto archiving WAL segment (even if incomplete) after last record log
     */
    public long getWalAutoArchiveAfterInactivity() {
        return walAutoArchiveAfterInactivity;
    }

    /**
     * This property defines order of writing pages to disk storage during checkpoint.
     *
     * @return Checkpoint write order.
     */
    public CheckpointWriteOrder getCheckpointWriteOrder() {
        return checkpointWriteOrder;
    }

    /**
     * This property defines order of writing pages to disk storage during checkpoint.
     *
     * @param checkpointWriteOrder Checkpoint write order.
     */
    public PersistentStoreConfiguration setCheckpointWriteOrder(CheckpointWriteOrder checkpointWriteOrder) {
        this.checkpointWriteOrder = checkpointWriteOrder;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PersistentStoreConfiguration.class, this);
    }
}
