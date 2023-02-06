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
import java.util.zip.Deflater;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.mem.MemoryAllocator;
import org.apache.ignite.mxbean.MetricsMxBean;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DATA_STORAGE_PAGE_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_USE_ASYNC_FILE_IO_FACTORY;

/**
 * A durable memory configuration for an Apache Ignite node. The durable memory is a manageable off-heap based memory
 * architecture that divides all expandable data regions into pages of fixed size
 * (see {@link DataStorageConfiguration#getPageSize()}). An individual page can store one or many cache key-value entries
 * that allows reusing the memory in the most efficient way and avoid memory fragmentation issues.
 * <p>
 * By default, the durable memory allocates a single expandable data region with default settings. All the caches that
 * will be configured in an application will be mapped to this data region by default, thus, all the cache data will
 * reside in that data region. Parameters of default data region can be changed by setting
 * {@link DataStorageConfiguration#setDefaultDataRegionConfiguration(DataRegionConfiguration)}.
 * Other data regions (except default) can be configured with
 * {@link DataStorageConfiguration#setDataRegionConfigurations(DataRegionConfiguration...)}.
 * <p>
 * Data region can be used in memory-only mode, or in persistent mode, when memory is used as a caching layer for disk.
 * Persistence for data region can be turned on with {@link DataRegionConfiguration#setPersistenceEnabled(boolean)}
 * flag. To learn more about data regions refer to {@link DataRegionConfiguration} documentation.
 * <p>Sample configuration below shows how to make 5 GB data regions the default one for Apache Ignite:</p>
 * <pre>
 *     {@code
 *
 *     <property name="dataStorageConfiguration">
 *         <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
 *             <property name="systemCacheInitialSize" value="#{100L * 1024 * 1024}"/>
 *
 *             <property name="defaultDataRegionConfiguration">
 *                 <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
 *                     <property name="name" value="default_data_region"/>
 *                     <property name="initialSize" value="#{5L * 1024 * 1024 * 1024}"/>
 *                 </bean>
 *             </property>
 *         </bean>
 *     </property>
 *     }
 * </pre>
 */
public class DataStorageConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Value used for making WAL archive size unlimited. */
    public static final long UNLIMITED_WAL_ARCHIVE = -1;

    /** Default data region start size (256 MB). */
    public static final long DFLT_DATA_REGION_INITIAL_SIZE = 256L * 1024 * 1024;

    /** Fraction of available memory to allocate for default DataRegion. */
    private static final double DFLT_DATA_REGION_FRACTION = 0.2;

    /** Default data region's size is 20% of physical memory available on current machine. */
    public static final long DFLT_DATA_REGION_MAX_SIZE = Math.max(
        (long)(DFLT_DATA_REGION_FRACTION * U.getTotalMemoryAvailable()),
        DFLT_DATA_REGION_INITIAL_SIZE);

    /** Default memory page size. */
    public static final int DFLT_PAGE_SIZE = 4 * 1024;

    /** Max memory page size. */
    public static final int MAX_PAGE_SIZE = 16 * 1024;

    /** Min memory page size. */
    public static final int MIN_PAGE_SIZE = 1024;

    /** This name is assigned to default Dataregion if no user-defined default MemPlc is specified */
    public static final String DFLT_DATA_REG_DEFAULT_NAME = "default";

    /** */
    public static final int DFLT_CHECKPOINT_FREQ = 180000;

    /** Lock default wait time, 10 sec. */
    public static final int DFLT_LOCK_WAIT_TIME = 10 * 1000;

    /** @deprecated Will be removed in upcoming releases. */
    @Deprecated
    public static final boolean DFLT_METRICS_ENABLED = false;

    /** Default amount of sub intervals to calculate rate-based metric. */
    public static final int DFLT_SUB_INTERVALS = 5;

    /** Default length of interval over which rate-based metric is calculated. */
    public static final int DFLT_RATE_TIME_INTERVAL_MILLIS = 60_000;

    /** Default number of checkpoint threads. */
    public static final int DFLT_CHECKPOINT_THREADS = 4;

    /** Default checkpoint write order. */
    public static final CheckpointWriteOrder DFLT_CHECKPOINT_WRITE_ORDER = CheckpointWriteOrder.SEQUENTIAL;

    /** Default number of checkpoints to be kept in WAL after checkpoint is finished */
    public static final int DFLT_WAL_HISTORY_SIZE = 20;

    /** Default max size of WAL archive files, in bytes. */
    public static final long DFLT_WAL_ARCHIVE_MAX_SIZE = 1024 * 1024 * 1024;

    /** */
    public static final int DFLT_WAL_SEGMENTS = 10;

    /** Default WAL file segment size, 64MBytes */
    public static final int DFLT_WAL_SEGMENT_SIZE = 64 * 1024 * 1024;

    /** Default wal mode. */
    public static final WALMode DFLT_WAL_MODE = WALMode.LOG_ONLY;

    /** Default thread local buffer size. */
    public static final int DFLT_TLB_SIZE = 128 * 1024;

    /** Default thread local buffer size. */
    public static final int DFLT_WAL_BUFF_SIZE = DFLT_WAL_SEGMENT_SIZE / 4;

    /** Default Wal flush frequency. */
    public static final int DFLT_WAL_FLUSH_FREQ = 2000;

    /** Default wal fsync delay. */
    public static final int DFLT_WAL_FSYNC_DELAY = 1000;

    /** Default wal record iterator buffer size. */
    public static final int DFLT_WAL_RECORD_ITERATOR_BUFFER_SIZE = 64 * 1024 * 1024;

    /** Default wal always write full pages. */
    public static final boolean DFLT_WAL_ALWAYS_WRITE_FULL_PAGES = false;

    /** Default wal directory. */
    public static final String DFLT_WAL_PATH = "db/wal";

    /** Default wal archive directory. */
    public static final String DFLT_WAL_ARCHIVE_PATH = "db/wal/archive";

    /** Default change data capture directory. */
    public static final String DFLT_WAL_CDC_PATH = "db/wal/cdc";

    /** Default change data capture directory maximum size. */
    public static final long DFLT_CDC_WAL_DIRECTORY_MAX_SIZE = 0;

    /** Default path (relative to working directory) of binary metadata folder */
    public static final String DFLT_BINARY_METADATA_PATH = "db/binary_meta";

    /** Default path (relative to working directory) of marshaller mappings folder */
    public static final String DFLT_MARSHALLER_PATH = "db/marshaller";

    /** Default write throttling enabled. */
    public static final boolean DFLT_WRITE_THROTTLING_ENABLED = false;

    /** Default wal compaction enabled. */
    public static final boolean DFLT_WAL_COMPACTION_ENABLED = false;

    /** Default wal compaction level. */
    public static final int DFLT_WAL_COMPACTION_LEVEL = Deflater.BEST_SPEED;

    /** Default defragmentation thread pool size. */
    public static final int DFLT_DEFRAGMENTATION_THREAD_POOL_SIZE = 4;

    /** Default compression algorithm for WAL page snapshot records. */
    public static final DiskPageCompression DFLT_WAL_PAGE_COMPRESSION = DiskPageCompression.DISABLED;

    /** @see IgniteSystemProperties#IGNITE_USE_ASYNC_FILE_IO_FACTORY */
    public static final boolean DFLT_USE_ASYNC_FILE_IO_FACTORY = true;

    /** Value used to indicate the use of half of the {@link #getMaxWalArchiveSize}. */
    public static final long HALF_MAX_WAL_ARCHIVE_SIZE = -1;

    /** Memory page size. */
    private int pageSize = IgniteSystemProperties.getInteger(
        IGNITE_DEFAULT_DATA_STORAGE_PAGE_SIZE, 0);

    /** Concurrency level. */
    private int concLvl;

    /** Configuration of default data region. */
    private SystemDataRegionConfiguration sysDataRegConf = new SystemDataRegionConfiguration();

    /** Configuration of default data region. */
    private DataRegionConfiguration dfltDataRegConf = new DataRegionConfiguration();

    /** Data regions. */
    @GridToStringInclude
    private DataRegionConfiguration[] dataRegions;

    /** Directory where index and partition files are stored. */
    private String storagePath;

    /** Checkpoint frequency. */
    private long checkpointFreq = DFLT_CHECKPOINT_FREQ;

    /** Lock wait time, in milliseconds. */
    private long lockWaitTime = DFLT_LOCK_WAIT_TIME;

    /** */
    private int checkpointThreads = DFLT_CHECKPOINT_THREADS;

    /** Checkpoint write order. */
    private CheckpointWriteOrder checkpointWriteOrder = DFLT_CHECKPOINT_WRITE_ORDER;

    /** Number of checkpoints to keep */
    private int walHistSize = DFLT_WAL_HISTORY_SIZE;

    /** Maximum size of wal archive folder, in bytes. */
    private long maxWalArchiveSize = DFLT_WAL_ARCHIVE_MAX_SIZE;

    /** Number of work WAL segments. */
    private int walSegments = DFLT_WAL_SEGMENTS;

    /** Size of one WAL segment in bytes. 64 Mb is used by default.  Maximum value is 2Gb */
    private int walSegmentSize = DFLT_WAL_SEGMENT_SIZE;

    /** Directory where WAL is stored (work directory) */
    private String walPath = DFLT_WAL_PATH;

    /** WAL archive path. */
    private String walArchivePath = DFLT_WAL_ARCHIVE_PATH;

    /** Change Data Capture path. */
    @IgniteExperimental
    private String cdcWalPath = DFLT_WAL_CDC_PATH;

    /** Change Data Capture directory size limit. */
    @IgniteExperimental
    private long cdcWalDirMaxSize = DFLT_CDC_WAL_DIRECTORY_MAX_SIZE;

    /**
     * Metrics enabled flag.
     * @deprecated Will be removed in upcoming releases.
     */
    @Deprecated
    private boolean metricsEnabled = DFLT_METRICS_ENABLED;

    /** Wal mode. */
    private WALMode walMode = DFLT_WAL_MODE;

    /** WAl thread local buffer size. */
    private int walTlbSize = DFLT_TLB_SIZE;

    /** WAl buffer size. */
    private int walBuffSize;

    /** Wal flush frequency in milliseconds. */
    private long walFlushFreq = DFLT_WAL_FLUSH_FREQ;

    /** Wal fsync delay. */
    private long walFsyncDelay = DFLT_WAL_FSYNC_DELAY;

    /** Wal record iterator buffer size. */
    private int walRecordIterBuffSize = DFLT_WAL_RECORD_ITERATOR_BUFFER_SIZE;

    /** Always write full pages. */
    private boolean alwaysWriteFullPages = DFLT_WAL_ALWAYS_WRITE_FULL_PAGES;

    /** Factory to provide I/O interface for data storage files */
    private FileIOFactory fileIOFactory =
        IgniteSystemProperties.getBoolean(IGNITE_USE_ASYNC_FILE_IO_FACTORY, DFLT_USE_ASYNC_FILE_IO_FACTORY) ?
            new AsyncFileIOFactory() : new RandomAccessFileIOFactory();

    /**
     * Number of sub-intervals the whole {@link #setMetricsRateTimeInterval(long)} will be split into to calculate
     * rate-based metrics.
     * <p>
     * Setting it to a bigger value will result in more precise calculation and smaller drops of
     * rate-based metrics when next sub-interval has to be recycled but introduces bigger
     * calculation overhead.
     */
    private int metricsSubIntervalCnt = DFLT_SUB_INTERVALS;

    /** Time interval (in milliseconds) for rate-based metrics. */
    private long metricsRateTimeInterval = DFLT_RATE_TIME_INTERVAL_MILLIS;

    /**
     * Time interval (in milliseconds) for running auto archiving for incompletely WAL segment
     */
    private long walAutoArchiveAfterInactivity = -1;

    /** Time interval (in milliseconds) after last log of data change for force archiving of incompletely WAL segment. */
    @IgniteExperimental
    private long walForceArchiveTimeout = -1;

    /** If true, threads that generate dirty pages too fast during ongoing checkpoint will be throttled. */
    private boolean writeThrottlingEnabled = DFLT_WRITE_THROTTLING_ENABLED;

    /**
     * Flag to enable WAL compaction. If true, system filters and compresses WAL archive in background.
     * Compressed WAL archive gets automatically decompressed on demand.
     */
    private boolean walCompactionEnabled = DFLT_WAL_COMPACTION_ENABLED;

    /**
     * ZIP level to WAL compaction.
     *
     * @see java.util.zip.ZipOutputStream#setLevel(int)
     * @see java.util.zip.Deflater#BEST_SPEED
     * @see java.util.zip.Deflater#BEST_COMPRESSION
     */
    private int walCompactionLevel = DFLT_WAL_COMPACTION_LEVEL;

    /** Timeout for checkpoint read lock acquisition. */
    private Long checkpointReadLockTimeout;

    /** Compression algorithm for WAL page snapshot records. */
    private DiskPageCompression walPageCompression = DFLT_WAL_PAGE_COMPRESSION;

    /** Compression level for WAL page snapshot records. */
    private Integer walPageCompressionLevel;

    /** Default warm-up configuration. */
    @Nullable private WarmUpConfiguration dfltWarmUpCfg;

    /** Encryption configuration. */
    private EncryptionConfiguration encCfg = new EncryptionConfiguration();

    /** Maximum number of partitions which can be defragmented at the same time. */
    private int defragmentationThreadPoolSize = DFLT_DEFRAGMENTATION_THREAD_POOL_SIZE;

    /** Minimum size of wal archive folder, in bytes. */
    private long minWalArchiveSize = HALF_MAX_WAL_ARCHIVE_SIZE;

    /** Default memory allocator for all data regions. */
    @Nullable private MemoryAllocator memoryAllocator = null;

    /**
     * Creates valid durable memory configuration with all default values.
     */
    @SuppressWarnings("RedundantNoArgConstructor")
    public DataStorageConfiguration() {
    }

    /**
     * Initial size of a data region reserved for system cache.
     *
     * @return Size in bytes.
     * @deprecated use {@link SystemDataRegionConfiguration#getInitialSize}.
     */
    @Deprecated
    public long getSystemRegionInitialSize() {
        if (sysDataRegConf == null)
            sysDataRegConf = new SystemDataRegionConfiguration();

        return sysDataRegConf.getInitialSize();
    }

    /**
     * Sets initial size of a data region reserved for system cache.
     *
     * @param sysRegionInitSize Size in bytes.
     * @return {@code this} for chaining.
     * @deprecated use {@link SystemDataRegionConfiguration#setInitialSize(long)}.
     */
    @Deprecated
    public DataStorageConfiguration setSystemRegionInitialSize(long sysRegionInitSize) {
        if (sysDataRegConf == null)
            sysDataRegConf = new SystemDataRegionConfiguration();

        sysDataRegConf.setInitialSize(sysRegionInitSize);

        return this;
    }

    /**
     * Maximum data region size reserved for system cache.
     *
     * @return Size in bytes.
     * @deprecated use {@link SystemDataRegionConfiguration#getMaxSize()}.
     */
    @Deprecated
    public long getSystemRegionMaxSize() {
        if (sysDataRegConf == null)
            sysDataRegConf = new SystemDataRegionConfiguration();

        return sysDataRegConf.getMaxSize();
    }

    /**
     * Sets maximum data region size reserved for system cache. The total size should not be less than 10 MB
     * due to internal data structures overhead.
     *
     * @param sysRegionMaxSize Maximum size in bytes for system cache data region.
     * @return {@code this} for chaining.
     * @deprecated use {@link SystemDataRegionConfiguration#setMaxSize(long)}.
     */
    @Deprecated
    public DataStorageConfiguration setSystemRegionMaxSize(long sysRegionMaxSize) {
        if (sysDataRegConf == null)
            sysDataRegConf = new SystemDataRegionConfiguration();

        sysDataRegConf.setMaxSize(sysRegionMaxSize);

        return this;
    }

    /**
     * The page memory consists of one or more expandable data regions defined by {@link DataRegionConfiguration}.
     * Every data region is split on pages of fixed size that store actual cache entries.
     *
     * @return Page size in bytes.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Changes the page size.
     *
     * @param pageSize Page size in bytes. Supported values are: {@code 1024}, {@code 2048}, {@code 4096}, {@code 8192}
     * and {@code 16384}. If value is not set (or zero), {@link #DFLT_PAGE_SIZE} ({@code 4096}) will be used.
     * @return {@code this} for chaining.
     * @see #MIN_PAGE_SIZE
     * @see #MAX_PAGE_SIZE
     */
    public DataStorageConfiguration setPageSize(int pageSize) {
        if (pageSize != 0) {
            A.ensure(pageSize >= MIN_PAGE_SIZE && pageSize <= MAX_PAGE_SIZE,
                "Page size must be between 1kB and 16kB.");
            A.ensure(U.isPow2(pageSize), "Page size must be a power of 2.");
        }

        this.pageSize = pageSize;

        return this;
    }

    /**
     * Gets an array of all data regions configured. Apache Ignite will instantiate a dedicated data region per
     * region. An Apache Ignite cache can be mapped to a specific region with
     * {@link CacheConfiguration#setDataRegionName(String)} method.
     *
     * @return Array of configured data regions.
     */
    public DataRegionConfiguration[] getDataRegionConfigurations() {
        return dataRegions;
    }

    /**
     * Sets data regions configurations.
     *
     * @param dataRegionConfigurations Data regions configurations.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setDataRegionConfigurations(DataRegionConfiguration... dataRegionConfigurations) {
        this.dataRegions = dataRegionConfigurations;

        return this;
    }

    /**
     * Returns the number of concurrent segments in Ignite internal page mapping tables.
     *
     * By default equals to the number of available CPUs.
     *
     * @return Mapping table concurrency level(always greater than 0).
     */
    public int getConcurrencyLevel() {
        return concLvl <= 0 ? Runtime.getRuntime().availableProcessors() : concLvl;
    }

    /**
     * Sets the number of concurrent segments in Ignite internal page mapping tables.
     *
     * If value is not positive, the number of available CPUs will be used.
     *
     * @param concLvl Mapping table concurrency level.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setConcurrencyLevel(int concLvl) {
        this.concLvl = concLvl;

        return this;
    }

    /**
     * @return Configuration of default data region. All cache groups will reside in this data region by default.
     * For assigning a custom data region to cache group, use {@link CacheConfiguration#setDataRegionName(String)}.
     */
    public DataRegionConfiguration getDefaultDataRegionConfiguration() {
        return dfltDataRegConf;
    }

    /**
     * Overrides configuration of default data region which has been created automatically.
     *
     * @param dfltDataRegConf Default data region configuration.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setDefaultDataRegionConfiguration(DataRegionConfiguration dfltDataRegConf) {
        this.dfltDataRegConf = dfltDataRegConf;

        return this;
    }

    /**
     * Configuration of system data region.
     *
     * @return Configuration of system data region.
     */
    public SystemDataRegionConfiguration getSystemDataRegionConfiguration() {
        return sysDataRegConf;
    }

    /**
     * Overrides configuration of system data region which has been created automatically.
     *
     * @param sysDataRegConf System data region configuration.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setSystemDataRegionConfiguration(SystemDataRegionConfiguration sysDataRegConf) {
        this.sysDataRegConf = sysDataRegConf;

        return this;
    }

    /**
     * @return A path the root directory where the Persistent Store will persist data and indexes.
     */
    public String getStoragePath() {
        return storagePath;
    }

    /**
     * Sets a path to the root directory where the Persistent Store will persist data and indexes.
     * By default the Persistent Store's files are located under Ignite work directory.
     *
     * @param persistenceStorePath Persistence store path.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setStoragePath(String persistenceStorePath) {
        this.storagePath = persistenceStorePath;

        return this;
    }

    /**
     * Gets checkpoint frequency.
     *
     * @return Checkpoint frequency in milliseconds.
     */
    public long getCheckpointFrequency() {
        return checkpointFreq <= 0 ? DFLT_CHECKPOINT_FREQ : checkpointFreq;
    }

    /**
     * Sets the checkpoint frequency which is a minimal interval when the dirty pages will be written
     * to the Persistent Store. Checkpoint will be triggered more frequently the lower the value you use.
     *
     * If value is not positive, {@link #DFLT_CHECKPOINT_FREQ} will be used.
     *
     * @param checkpointFreq Checkpoint frequency in milliseconds.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setCheckpointFrequency(long checkpointFreq) {
        this.checkpointFreq = checkpointFreq;

        return this;
    }

    /**
     * Gets a number of threads to use for the checkpoint purposes.
     *
     * @return Number of checkpoint threads.
     */
    public int getCheckpointThreads() {
        return checkpointThreads;
    }

    /**
     * Sets a number of threads to use for the checkpoint purposes.
     *
     * @param checkpointThreads Number of checkpoint threads. Four threads are used by default.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setCheckpointThreads(int checkpointThreads) {
        this.checkpointThreads = checkpointThreads;

        return this;
    }

    /**
     * Timeout in milliseconds to wait when acquiring persistence store lock file before failing the local node.
     *
     * @return Lock wait time in milliseconds.
     */
    public long getLockWaitTime() {
        return lockWaitTime;
    }

    /**
     * Timeout in milliseconds to wait when acquiring persistence store lock file before failing the local node.
     *
     * @param lockWaitTime Lock wait time in milliseconds.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setLockWaitTime(long lockWaitTime) {
        this.lockWaitTime = lockWaitTime;

        return this;
    }

    /**
     * Gets a total number of checkpoints to keep in the WAL history.
     *
     * @return Number of checkpoints to keep in WAL after a checkpoint is finished.
     * @see DataStorageConfiguration#getMaxWalArchiveSize()
     * @deprecated Instead of walHistorySize use maxWalArchiveSize for manage of archive size.
     */
    @Deprecated
    public int getWalHistorySize() {
        return walHistSize <= 0 ? DFLT_WAL_HISTORY_SIZE : walHistSize;
    }

    /**
     * Sets a total number of checkpoints to keep in the WAL history.
     *
     * @param walHistSize Number of checkpoints to keep after a checkpoint is finished.
     * @return {@code this} for chaining.
     * @see DataStorageConfiguration#setMaxWalArchiveSize(long)
     * @deprecated Instead of walHistorySize use maxWalArchiveSize for manage of archive size.
     */
    @Deprecated
    public DataStorageConfiguration setWalHistorySize(int walHistSize) {
        this.walHistSize = walHistSize;

        return this;
    }

    /**
     * If WalHistorySize was set by user will use this parameter for compatibility.
     *
     * @return {@code true} if use WalHistorySize for compatibility.
     */
    public boolean isWalHistorySizeParameterUsed() {
        return getWalHistorySize() != DFLT_WAL_HISTORY_SIZE && getWalHistorySize() != Integer.MAX_VALUE;
    }

    /**
     * Gets a max allowed size(in bytes) of WAL archives.
     *
     * @return max size(in bytes) of WAL archive directory(greater than 0, or {@link #UNLIMITED_WAL_ARCHIVE} if
     * WAL archive size is unlimited).
     */
    public long getMaxWalArchiveSize() {
        return maxWalArchiveSize;
    }

    /**
     * Sets a max allowed size(in bytes) of WAL archives.
     *
     * If value is not positive or {@link #UNLIMITED_WAL_ARCHIVE}, {@link #DFLT_WAL_ARCHIVE_MAX_SIZE} will be used.
     *
     * @param walArchiveMaxSize max size(in bytes) of WAL archive directory.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setMaxWalArchiveSize(long walArchiveMaxSize) {
        if (walArchiveMaxSize != UNLIMITED_WAL_ARCHIVE)
            A.ensure(walArchiveMaxSize > 0, "Max WAL archive size can be only greater than 0 " +
                "or must be equal to " + UNLIMITED_WAL_ARCHIVE + " (to be unlimited)");

        this.maxWalArchiveSize = walArchiveMaxSize;

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
     * @param walSegments Number of WAL segments. Value must be greater than 1.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalSegments(int walSegments) {
        if (walSegments != 0)
            A.ensure(walSegments > 1, "Number of WAL segments must be greater than 1.");

        this.walSegments = walSegments;

        return this;
    }

    /**
     * Gets size(in bytes) of a WAL segment.
     *
     * @return WAL segment size(in bytes).
     */
    public int getWalSegmentSize() {
        return walSegmentSize == 0 ? DFLT_WAL_SEGMENT_SIZE : walSegmentSize;
    }

    /**
     * Sets size(in bytes) of a WAL segment.
     * If value is not set (or zero), {@link #DFLT_WAL_SEGMENT_SIZE} will be used.
     *
     * @param walSegmentSize WAL segment size(in bytes). Value must be between 512Kb and 2Gb.
     * @return {@code This} for chaining.
     */
    public DataStorageConfiguration setWalSegmentSize(int walSegmentSize) {
        if (walSegmentSize != 0)
            A.ensure(walSegmentSize >= 512 * 1024, "WAL segment size must be between 512Kb and 2Gb.");

        this.walSegmentSize = walSegmentSize;

        return this;
    }

    /**
     * Gets a path to the directory where WAL is stored.
     *
     * @return WAL persistence path, absolute or relative to Ignite work directory.
     */
    public String getWalPath() {
        return walPath;
    }

    /**
     * Sets a path to the directory where WAL is stored. If this path is relative, it will be resolved
     * relatively to Ignite work directory.
     *
     * @param walStorePath WAL persistence path, absolute or relative to Ignite work directory.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalPath(String walStorePath) {
        this.walPath = walStorePath;

        return this;
    }

    /**
     * Gets a path to the WAL archive directory.
     *
     * @return WAL archive directory.
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
    public DataStorageConfiguration setWalArchivePath(String walArchivePath) {
        this.walArchivePath = walArchivePath;

        return this;
    }

    /**
     * Gets a path to the CDC directory.
     * If this path is relative, it will be resolved relatively to Ignite work directory.
     *
     * @return CDC directory.
     */
    @IgniteExperimental
    public String getCdcWalPath() {
        return cdcWalPath;
    }

    /**
     * Sets a path for the CDC directory.
     * Hard link to every WAL Archive segment will be created in it for CDC processing purpose.
     *
     * @param cdcWalPath CDC directory.
     * @return {@code this} for chaining.
     */
    @IgniteExperimental
    public DataStorageConfiguration setCdcWalPath(String cdcWalPath) {
        this.cdcWalPath = cdcWalPath;

        return this;
    }

    /**
     * Sets the {@link #getCdcWalPath CDC directory} maximum size in bytes.
     *
     * @return CDC directory maximum size in bytes.
     */
    @IgniteExperimental
    public long getCdcWalDirectoryMaxSize() {
        return cdcWalDirMaxSize;
    }

    /**
     * Sets the CDC directory maximum size in bytes. Zero or negative means no limit. Creation of segment CDC link
     * will be skipped when the total size of CDC files in the {@link #getCdcWalPath directory} exceeds the limit.
     * The CDC application will log an error due to a gap in wal files sequence. Note that cache changes will be lost.
     * Default is no limit.
     *
     * @param cdcWalDirMaxSize CDC directory maximum size in bytes.
     * @return {@code this} for chaining.
     */
    @IgniteExperimental
    public DataStorageConfiguration setCdcWalDirectoryMaxSize(long cdcWalDirMaxSize) {
        this.cdcWalDirMaxSize = cdcWalDirMaxSize;

        return this;
    }

    /**
     * Gets flag indicating whether persistence metrics collection is enabled.
     * Default value is {@link #DFLT_METRICS_ENABLED}.
     *
     * @return Metrics enabled flag.
     * @deprecated Will be removed in upcoming releases.
     */
    @Deprecated
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /**
     * Sets flag indicating whether persistence metrics collection is enabled.
     *
     * @param metricsEnabled Metrics enabled flag.
     * @return {@code this} for chaining.
     * @deprecated Will be removed in upcoming releases.
     */
    @Deprecated
    public DataStorageConfiguration setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;

        return this;
    }

    /**
     * @return  Flag indicating whether write throttling is enabled.
     */
    public boolean isWriteThrottlingEnabled() {
        return writeThrottlingEnabled;
    }

    /**
     * Sets flag indicating whether write throttling is enabled.
     *
     * @param writeThrottlingEnabled Write throttling enabled flag.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWriteThrottlingEnabled(boolean writeThrottlingEnabled) {
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
    public long getMetricsRateTimeInterval() {
        return metricsRateTimeInterval;
    }

    /**
     * Sets the length of the time interval for rate-based metrics. This interval defines a window over which
     * hits will be tracked.
     *
     * @param metricsRateTimeInterval Time interval in milliseconds.
     * @return {@code this} for chaining.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public DataStorageConfiguration setMetricsRateTimeInterval(long metricsRateTimeInterval) {
        this.metricsRateTimeInterval = metricsRateTimeInterval;

        return this;
    }

    /**
     * Gets the number of sub-intervals to split the {@link #getMetricsRateTimeInterval()} into to track the update history.
     * Default value is {@link #DFLT_SUB_INTERVALS}.
     *
     * @return The number of sub-intervals for history tracking.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public int getMetricsSubIntervalCount() {
        return metricsSubIntervalCnt;
    }

    /**
     * Sets the number of sub-intervals to split the {@link #getMetricsRateTimeInterval()} into to track the update history.
     *
     * @param metricsSubIntervalCnt The number of sub-intervals for history tracking.
     * @return {@code this} for chaining.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public DataStorageConfiguration setMetricsSubIntervalCount(int metricsSubIntervalCnt) {
        this.metricsSubIntervalCnt = metricsSubIntervalCnt;

        return this;
    }

    /**
     * Property that defines behavior of wal fsync.
     * Different type provides different guarantees for consistency. See {@link WALMode} for details.
     *
     * @return WAL mode.
     */
    public WALMode getWalMode() {
        return walMode == null ? DFLT_WAL_MODE : walMode;
    }

    /**
     * Sets property that defines behavior of wal fsync.
     * Different type provides different guarantees for consistency. See {@link WALMode} for details.
     *
     * @param walMode Wal mode.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalMode(WALMode walMode) {
        if (walMode == WALMode.DEFAULT)
            walMode = WALMode.FSYNC;

        this.walMode = walMode;

        return this;
    }

    /**
     * Property for size of thread local buffer.
     * Each thread which write to wal have thread local buffer for serialize recode before write in wal.
     *
     * @return Thread local buffer size (in bytes).
     */
    public int getWalThreadLocalBufferSize() {
        return walTlbSize <= 0 ? DFLT_TLB_SIZE : walTlbSize;
    }

    /**
     * Sets size of thread local buffer.
     * Each thread which write to wal have thread local buffer for serialize recode before write in wal.
     *
     * @param walTlbSize Thread local buffer size (in bytes).
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalThreadLocalBufferSize(int walTlbSize) {
        this.walTlbSize = walTlbSize;

        return this;
    }

    /**
     * Property defines size(in bytes) of WAL buffer.
     * Each WAL record will be serialized to this buffer before write in WAL file.
     *
     * @return WAL buffer size(in bytes).
     */
    public int getWalBufferSize() {
        return walBuffSize <= 0 ? getWalSegmentSize() / 4 : walBuffSize;
    }

    /**
     * Property defines size(in bytes) of WAL buffer.
     * If value isn't positive it calculation will be based on {@link #getWalSegmentSize()}.
     *
     * @param walBuffSize WAL buffer size(in bytes).
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalBufferSize(int walBuffSize) {
        this.walBuffSize = walBuffSize;

        return this;
    }

    /**
     * This property define how often WAL will be fsync-ed in {@code BACKGROUND} mode. Ignored for
     * all other WAL modes.
     *
     * @return WAL flush frequency, in milliseconds.
     */
    public long getWalFlushFrequency() {
        return walFlushFreq;
    }

    /**
     * This property define how often WAL will be fsync-ed in {@code BACKGROUND} mode. Ignored for
     * all other WAL modes.
     *
     * @param walFlushFreq WAL flush frequency, in milliseconds.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalFlushFrequency(long walFlushFreq) {
        this.walFlushFreq = walFlushFreq;

        return this;
    }

    /**
     * @return Property that allows to trade latency for throughput in {@link WALMode#FSYNC} mode.
     * It limits minimum time interval between WAL fsyncs. First thread that initiates WAL fsync will wait for
     * this number of nanoseconds, another threads will just wait fsync of first thread (similar to CyclicBarrier).
     * Total throughput should increase under load as total WAL fsync rate will be limited.
     */
    public long getWalFsyncDelayNanos() {
        return walFsyncDelay <= 0 ? DFLT_WAL_FSYNC_DELAY : walFsyncDelay;
    }

    /**
     * Sets property that allows to trade latency for throughput in {@link WALMode#FSYNC} mode.
     * It limits minimum time interval between WAL fsyncs. First thread that initiates WAL fsync will wait for
     * this number of nanoseconds, another threads will just wait fsync of first thread (similar to CyclicBarrier).
     * Total throughput should increase under load as total WAL fsync rate will be limited.
     *
     * @param walFsyncDelayNanos Wal fsync delay, in nanoseconds.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalFsyncDelayNanos(long walFsyncDelayNanos) {
        walFsyncDelay = walFsyncDelayNanos;

        return this;
    }

    /**
     * Property define how many bytes iterator read from
     * disk (for one reading), during go ahead wal.
     *
     * @return Record iterator buffer size.
     */
    public int getWalRecordIteratorBufferSize() {
        return walRecordIterBuffSize <= 0 ? DFLT_WAL_RECORD_ITERATOR_BUFFER_SIZE : walRecordIterBuffSize;
    }

    /**
     * Sets property defining how many bytes iterator read from
     * disk (for one reading), during go ahead wal.
     *
     * @param walRecordIterBuffSize Wal record iterator buffer size.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalRecordIteratorBufferSize(int walRecordIterBuffSize) {
        this.walRecordIterBuffSize = walRecordIterBuffSize;

        return this;
    }

    /**
     * @return  Flag that enforces writing full page to WAL on every change (instead of delta record).
     * Can be used for debugging purposes: every version of page will be present in WAL.
     * Note that WAL will take several times more space in this mode.
     */
    public boolean isAlwaysWriteFullPages() {
        return alwaysWriteFullPages;
    }

    /**
     * Sets flag that enforces writing full page to WAL on every change (instead of delta record).
     * Can be used for debugging purposes: every version of page will be present in WAL.
     * Note that WAL will take several times more space in this mode.
     *
     * @param alwaysWriteFullPages Always write full pages flag.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setAlwaysWriteFullPages(boolean alwaysWriteFullPages) {
        this.alwaysWriteFullPages = alwaysWriteFullPages;

        return this;
    }

    /**
     * Factory to provide implementation of FileIO interface
     * which is used for data storage files read/write operations
     *
     * @return File I/O factory
     */
    public FileIOFactory getFileIOFactory() {
        return fileIOFactory;
    }

    /**
     * Sets factory to provide implementation of FileIO interface
     * which is used for data storage files read/write operations
     *
     * @param fileIOFactory File I/O factory
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setFileIOFactory(FileIOFactory fileIOFactory) {
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
    public DataStorageConfiguration setWalAutoArchiveAfterInactivity(long walAutoArchiveAfterInactivity) {
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
     * @param walForceArchiveTimeout Time in millis after last data change logged to run segment auto archivation
     * (even if incomplete).<br> Positive value enables incomplete segment archivation after timeout.<br>
     * Zero or negative value disables forcefull auto archiving.
     * @return current configuration instance for chaining.
     */
    @IgniteExperimental
    public DataStorageConfiguration setWalForceArchiveTimeout(long walForceArchiveTimeout) {
        this.walForceArchiveTimeout = walForceArchiveTimeout;

        return this;
    }

    /**
     * @return time interval (in milliseconds) after last log of data change
     * for force archiving of incompletely WAL segment.
     */
    @IgniteExperimental
    public long getWalForceArchiveTimeout() {
        return walForceArchiveTimeout;
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
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setCheckpointWriteOrder(CheckpointWriteOrder checkpointWriteOrder) {
        this.checkpointWriteOrder = checkpointWriteOrder;

        return this;
    }

    /**
     * @return Flag indicating whether WAL compaction is enabled.
     */
    public boolean isWalCompactionEnabled() {
        return walCompactionEnabled;
    }

    /**
     * Sets flag indicating whether WAL compaction is enabled.
     *
     * @param walCompactionEnabled Wal compaction enabled flag.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalCompactionEnabled(boolean walCompactionEnabled) {
        this.walCompactionEnabled = walCompactionEnabled;

        return this;
    }

    /**
     * @return ZIP level to WAL compaction.
     */
    public int getWalCompactionLevel() {
        return walCompactionLevel;
    }

    /**
     * @param walCompactionLevel New ZIP level to WAL compaction.
     */
    public void setWalCompactionLevel(int walCompactionLevel) {
        this.walCompactionLevel = walCompactionLevel;
    }

    /**
     * Returns timeout for checkpoint read lock acquisition.
     *
     * @see #setCheckpointReadLockTimeout(long)
     * @return Returns timeout for checkpoint read lock acquisition in milliseconds.
     */
    public Long getCheckpointReadLockTimeout() {
        return checkpointReadLockTimeout;
    }

    /**
     * Sets timeout for checkpoint read lock acquisition.
     * <p>
     * When any thread cannot acquire checkpoint read lock in this time, then critical failure handler is being called.
     *
     * @param checkpointReadLockTimeout Timeout for checkpoint read lock acquisition in milliseconds.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setCheckpointReadLockTimeout(long checkpointReadLockTimeout) {
        this.checkpointReadLockTimeout = checkpointReadLockTimeout;

        return this;
    }

    /**
     * Gets compression algorithm for WAL page snapshot records.
     *
     * @return Page compression algorithm.
     */
    public DiskPageCompression getWalPageCompression() {
        return walPageCompression == null ? DFLT_WAL_PAGE_COMPRESSION : walPageCompression;
    }

    /**
     * Sets compression algorithm for WAL page snapshot records.
     *
     * @param walPageCompression Page compression algorithm.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalPageCompression(DiskPageCompression walPageCompression) {
        this.walPageCompression = walPageCompression;

        return this;
    }

    /**
     * Gets {@link #getWalPageCompression algorithm} specific WAL page compression level.
     *
     * @return WAL page snapshots compression level or {@code null} for default.
     */
    public Integer getWalPageCompressionLevel() {
        return walPageCompressionLevel;
    }

    /**
     * Sets {@link #setWalPageCompression algorithm} specific page compression level.
     *
     * @param walPageCompressionLevel Disk page compression level or {@code null} to use default.
     *      {@link DiskPageCompression#ZSTD Zstd}: from {@code -131072} to {@code 22} (default {@code 3}).
     *      {@link DiskPageCompression#LZ4 LZ4}: from {@code 0} to {@code 17} (default {@code 0}).
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setWalPageCompressionLevel(Integer walPageCompressionLevel) {
        this.walPageCompressionLevel = walPageCompressionLevel;

        return this;
    }

    /**
     * Gets encryyption configuration.
     *
     * @return Encryption configuration.
     */
    public EncryptionConfiguration getEncryptionConfiguration() {
        return encCfg;
    }

    /**
     * Sets encryption configuration.
     *
     * @param encCfg Encryption configuration.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setEncryptionConfiguration(EncryptionConfiguration encCfg) {
        this.encCfg = encCfg;

        return this;
    }

    /**
     * Sets default warm-up configuration.
     *
     * @param dfltWarmUpCfg Default warm-up configuration. To assign a special
     *      warm-up configuration for a data region, use
     *      {@link DataRegionConfiguration#setWarmUpConfiguration}.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setDefaultWarmUpConfiguration(@Nullable WarmUpConfiguration dfltWarmUpCfg) {
        this.dfltWarmUpCfg = dfltWarmUpCfg;

        return this;
    }

    /**
     * Gets default warm-up configuration.
     *
     * @return Default warm-up configuration.
     */
    @Nullable public WarmUpConfiguration getDefaultWarmUpConfiguration() {
        return dfltWarmUpCfg;
    }

    /**
     * Sets maximum number of partitions which can be defragmented at the same time.
     *
     * @param defragmentationThreadPoolSize Maximum number of partitions which can be defragmented at the same time.
     *      Default is {@link DataStorageConfiguration#DFLT_DEFRAGMENTATION_THREAD_POOL_SIZE}.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setDefragmentationThreadPoolSize(int defragmentationThreadPoolSize) {
        A.ensure(defragmentationThreadPoolSize > 1, "Defragmentation thread pool size must be greater or equal to 1.");

        this.defragmentationThreadPoolSize = defragmentationThreadPoolSize;

        return this;
    }

    /**
     * Maximum number of partitions which can be defragmented at the same time.
     *
     * @return Thread pool size for defragmentation.
     */
    public int getDefragmentationThreadPoolSize() {
        return defragmentationThreadPoolSize;
    }

    /**
     * Gets a min allowed size(in bytes) of WAL archives.
     *
     * @return min size(in bytes) of WAL archive directory(greater than 0, or {@link #HALF_MAX_WAL_ARCHIVE_SIZE}).
     */
    public long getMinWalArchiveSize() {
        return minWalArchiveSize;
    }

    /**
     * Sets a min allowed size(in bytes) of WAL archives.
     *
     * If value is not positive, {@link #HALF_MAX_WAL_ARCHIVE_SIZE} will be used.
     *
     * @param walArchiveMinSize min size(in bytes) of WAL archive directory.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setMinWalArchiveSize(long walArchiveMinSize) {
        if (walArchiveMinSize != HALF_MAX_WAL_ARCHIVE_SIZE) {
            A.ensure(walArchiveMinSize > 0, "Min WAL archive size can be only greater than 0 " +
                "or must be equal to " + HALF_MAX_WAL_ARCHIVE_SIZE + " (to be half of max WAL archive size)");
        }

        this.minWalArchiveSize = walArchiveMinSize;

        return this;
    }

    /**
     * @return Memory allocator instance.
     */
    @Nullable public MemoryAllocator getMemoryAllocator() {
        return memoryAllocator;
    }

    /**
     * Sets default memory allocator for all memory regions. If not specified, default, based on {@code Unsafe}
     * allocator will be used. Allocator can be overrided for data region using
     * {@link DataRegionConfiguration#setMemoryAllocator(MemoryAllocator)}
     *
     * @param allocator Memory allocator instance.
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setMemoryAllocator(MemoryAllocator allocator) {
        memoryAllocator = allocator;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStorageConfiguration.class, this);
    }
}
