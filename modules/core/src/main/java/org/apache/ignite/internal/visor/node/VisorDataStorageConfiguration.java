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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for data store configuration.
 */
public class VisorDataStorageConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Size of a memory chunk reserved for system cache initially. */
    private long sysCacheInitSize;

    /** Size of memory for system cache. */
    private long sysCacheMaxSize;

    /** Page size. */
    private int pageSize;

    /** Concurrency level. */
    private int concLvl;

    /** Configuration of default data region. */
    private VisorDataRegionConfiguration dfltDataRegConf;

    /** Memory policies. */
    private List<VisorDataRegionConfiguration> dataRegions;

    /** */
    private String persistenceStorePath;

    /** Checkpointing frequency. */
    private long checkpointingFreq;

    /** Lock wait time. */
    private long lockWaitTime;

    /** */
    private long checkpointingPageBufSize;

    /** */
    private int checkpointingThreads;

    /** Checkpoint write order. */
    private CheckpointWriteOrder checkpointWriteOrder;

    /** */
    private int walHistSize;

    /** Number of work WAL segments. */
    private int walSegments;

    /** Number of WAL segments to keep. */
    private int walSegmentSize;

    /** WAL persistence path. */
    private String walStorePath;

    /** WAL archive path. */
    private String walArchivePath;

    /** Metrics enabled flag. */
    private boolean metricsEnabled;

    /** Wal mode. */
    private WALMode walMode;

    /** WAl thread local buffer size. */
    private int tlbSize;

    /** Wal flush frequency. */
    private long walFlushFreq;

    /** Wal fsync delay in nanoseconds. */
    private long walFsyncDelay;

    /** Wal record iterator buffer size. */
    private int walRecordIterBuffSize;

    /** Always write full pages. */
    private boolean alwaysWriteFullPages;

    /** Factory to provide I/O interface for files */
    private String fileIOFactory;

    /** Number of sub-intervals. */
    private int subIntervals;

    /** Time interval (in milliseconds) for rate-based metrics. */
    private long rateTimeInterval;

    /** Time interval (in milliseconds) for running auto archiving for incompletely WAL segment */
    private long walAutoArchiveAfterInactivity;

    /** If true, threads that generate dirty pages too fast during ongoing checkpoint will be throttled. */
    private boolean writeThrottlingEnabled;

    /**
     * Default constructor.
     */
    public VisorDataStorageConfiguration() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cfg Memory policy configuration.
     */
    public VisorDataStorageConfiguration(DataStorageConfiguration cfg) {
        assert cfg != null;

        sysCacheInitSize = cfg.getSystemCacheInitialSize();
        sysCacheMaxSize = cfg.getSystemCacheMaxSize();
        pageSize = cfg.getPageSize();
        concLvl = cfg.getConcurrencyLevel();

        DataRegionConfiguration dfltRegion = cfg.getDefaultRegionConfiguration();

        if (dfltRegion != null)
            dfltDataRegConf = new VisorDataRegionConfiguration(cfg.getDefaultRegionConfiguration());

        dataRegions = VisorDataRegionConfiguration.from(cfg.getDataRegions());

        persistenceStorePath = cfg.getPersistentStorePath();
        checkpointingFreq = cfg.getCheckpointingFrequency();
        lockWaitTime = cfg.getLockWaitTime();
        checkpointingPageBufSize = cfg.getCheckpointingPageBufferSize();
        checkpointingThreads = cfg.getCheckpointingThreads();
        checkpointWriteOrder = cfg.getCheckpointWriteOrder();
        walHistSize = cfg.getWalHistorySize();
        walSegments = cfg.getWalSegments();
        walSegmentSize = cfg.getWalSegmentSize();
        walStorePath = cfg.getWalStorePath();
        walArchivePath = cfg.getWalArchivePath();
        metricsEnabled = cfg.isMetricsEnabled();
        walMode = cfg.getWalMode();
        tlbSize = cfg.getTlbSize();
        walFlushFreq = cfg.getWalFlushFrequency();
        walFsyncDelay = cfg.getWalFsyncDelayNanos();
        walRecordIterBuffSize = cfg.getWalRecordIteratorBufferSize();
        alwaysWriteFullPages = cfg.isAlwaysWriteFullPages();
        fileIOFactory = compactClass(cfg.getFileIOFactory());
        subIntervals = cfg.getSubIntervals();
        rateTimeInterval = cfg.getRateTimeInterval();
        walAutoArchiveAfterInactivity = cfg.getWalAutoArchiveAfterInactivity();
        writeThrottlingEnabled = cfg.isWriteThrottlingEnabled();
    }

    /**
     * Initial size of a data region reserved for system cache in bytes.
     *
     * @return Size in bytes.
     */
    public long getSystemCacheInitialSize() {
        return sysCacheInitSize;
    }

    /**
     * Maximum data region size reserved for system cache in bytes.
     *
     * @return Size in bytes.
     */
    public long getSystemCacheMaxSize() {
        return sysCacheMaxSize;
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
     * Returns the number of concurrent segments in Ignite internal page mapping tables. By default equals
     * to the number of available CPUs multiplied by 4.
     *
     * @return Mapping table concurrency level.
     */
    public int getConcurrencyLevel() {
        return concLvl;
    }

    /**
     * @return Configuration of default data region. All cache groups will reside in this data region by default.
     * For assigning a custom data region to cache group, use {@link CacheConfiguration#setDataRegionName(String)}.
     */
    public VisorDataRegionConfiguration getDefaultRegionConfiguration() {
        return dfltDataRegConf;
    }

    /**
     * Gets an list of all data regions configured. Apache Ignite will instantiate a dedicated data region per
     * region. An Apache Ignite cache can be mapped to a specific region with
     * {@link CacheConfiguration#setDataRegionName(String)} method.
     *
     * @return Array of configured data regions.
     */
    public List<VisorDataRegionConfiguration> getDataRegions() {
        return dataRegions;
    }

    /**
     * @return Path the root directory where the Persistent Store will persist data and indexes.
     */
    public String getPersistentStorePath() {
        return persistenceStorePath;
    }

    /**
     * @return Checkpointing frequency in milliseconds.
     */
    public long getCheckpointingFrequency() {
        return checkpointingFreq;
    }

    /**
     * @return Checkpointing page buffer size in bytes.
     */
    public long getCheckpointingPageBufferSize() {
        return checkpointingPageBufSize;
    }

    /**
     * @return Number of checkpointing threads.
     */
    public int getCheckpointingThreads() {
        return checkpointingThreads;
    }

    /**
     * @return Checkpoint write order.
     */
    public CheckpointWriteOrder getCheckpointWriteOrder() {
        return checkpointWriteOrder;
    }

    /**
     * @return Time for wait.
     */
    public long getLockWaitTime() {
        return lockWaitTime;
    }

    /**
     * @return Number of WAL segments to keep after a checkpoint is finished.
     */
    public int getWalHistorySize() {
        return walHistSize;
    }

    /**
     * @return Number of work WAL segments.
     */
    public int getWalSegments() {
        return walSegments;
    }

    /**
     * @return WAL segment size.
     */
    public int getWalSegmentSize() {
        return walSegmentSize;
    }

    /**
     * @return WAL persistence path, absolute or relative to Ignite work directory.
     */
    public String getWalStorePath() {
        return walStorePath;
    }

    /**
     *  @return WAL archive directory.
     */
    public String getWalArchivePath() {
        return walArchivePath;
    }

    /**
     * @return Metrics enabled flag.
     */
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /**
     * @return Time interval in milliseconds.
     */
    public long getRateTimeInterval() {
        return rateTimeInterval;
    }

    /**
     * @return The number of sub-intervals for history tracking.
     */
    public int getSubIntervals() {
        return subIntervals;
    }

    /**
     * @return WAL mode.
     */
    public WALMode getWalMode() {
        return walMode;
    }

    /**
     * @return Thread local buffer size.
     */
    public int getTlbSize() {
        return tlbSize;
    }

    /**
     * @return Flush frequency.
     */
    public long getWalFlushFrequency() {
        return walFlushFreq;
    }

    /**
     * Gets the fsync delay, in nanoseconds.
     */
    public long getWalFsyncDelayNanos() {
        return walFsyncDelay;
    }

    /**
     * @return Record iterator buffer size.
     */
    public int getWalRecordIteratorBufferSize() {
        return walRecordIterBuffSize;
    }

    /**
     *
     */
    public boolean isAlwaysWriteFullPages() {
        return alwaysWriteFullPages;
    }

    /**
     * @return File I/O factory class name.
     */
    public String getFileIOFactory() {
        return fileIOFactory;
    }

    /**
     * @return time in millis to run auto archiving WAL segment (even if incomplete) after last record log
     */
    public long getWalAutoArchiveAfterInactivity() {
        return walAutoArchiveAfterInactivity;
    }

    /**
     * Gets flag indicating whether write throttling is enabled.
     */
    public boolean isWriteThrottlingEnabled() {
        return writeThrottlingEnabled;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(sysCacheInitSize);
        out.writeLong(sysCacheMaxSize);
        out.writeInt(pageSize);
        out.writeInt(concLvl);
        U.writeCollection(out, dataRegions);

        U.writeString(out, persistenceStorePath);
        out.writeLong(checkpointingFreq);
        out.writeLong(lockWaitTime);
        out.writeLong(checkpointingPageBufSize);
        out.writeInt(checkpointingThreads);
        U.writeEnum(out, checkpointWriteOrder);
        out.writeInt(walHistSize);
        out.writeInt(walSegments);
        out.writeInt(walSegmentSize);
        U.writeString(out, walStorePath);
        U.writeString(out, walArchivePath);
        out.writeBoolean(metricsEnabled);
        U.writeEnum(out, walMode);
        out.writeInt(tlbSize);
        out.writeLong(walFlushFreq);
        out.writeLong(walFsyncDelay);
        out.writeInt(walRecordIterBuffSize);
        out.writeBoolean(alwaysWriteFullPages);
        U.writeString(out, fileIOFactory);
        out.writeInt(subIntervals);
        out.writeLong(rateTimeInterval);
        out.writeLong(walAutoArchiveAfterInactivity);
        out.writeBoolean(writeThrottlingEnabled);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        sysCacheInitSize = in.readLong();
        sysCacheMaxSize = in.readLong();
        pageSize = in.readInt();
        concLvl = in.readInt();
        dataRegions = U.readList(in);

        persistenceStorePath = U.readString(in);
        checkpointingFreq = in.readLong();
        lockWaitTime = in.readLong();
        checkpointingPageBufSize = in.readLong();
        checkpointingThreads = in.readInt();
        checkpointWriteOrder = CheckpointWriteOrder.fromOrdinal(in.readByte());
        walHistSize = in.readInt();
        walSegments = in.readInt();
        walSegmentSize = in.readInt();
        walStorePath = U.readString(in);
        walArchivePath = U.readString(in);
        metricsEnabled = in.readBoolean();
        walMode = WALMode.fromOrdinal(in.readByte());
        tlbSize = in.readInt();
        walFlushFreq = in.readLong();
        walFsyncDelay = in.readLong();
        walRecordIterBuffSize = in.readInt();
        alwaysWriteFullPages = in.readBoolean();
        fileIOFactory = U.readString(in);
        subIntervals = in.readInt();
        rateTimeInterval = in.readLong();
        walAutoArchiveAfterInactivity = in.readLong();
        writeThrottlingEnabled = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDataStorageConfiguration.class, this);
    }
}
