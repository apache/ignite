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
    private long sysRegionInitSize;

    /** Size of memory for system cache. */
    private long sysRegionMaxSize;

    /** Page size. */
    private int pageSize;

    /** Concurrency level. */
    private int concLvl;

    /** Configuration of default data region. */
    private VisorDataRegionConfiguration dfltDataRegCfg;

    /** Memory policies. */
    private List<VisorDataRegionConfiguration> dataRegCfgs;

    /** */
    private String storagePath;

    /** Checkpointing frequency. */
    private long checkpointFreq;

    /** Lock wait time. */
    private long lockWaitTime;

    /** */
    private int checkpointThreads;

    /** Checkpoint write order. */
    private CheckpointWriteOrder checkpointWriteOrder;

    /** */
    private int walHistSize;

    /** Number of work WAL segments. */
    private int walSegments;

    /** Number of WAL segments to keep. */
    private int walSegmentSize;

    /** WAL persistence path. */
    private String walPath;

    /** WAL archive path. */
    private String walArchivePath;

    /** Metrics enabled flag. */
    private boolean metricsEnabled;

    /** Wal mode. */
    private WALMode walMode;

    /** WAl thread local buffer size. */
    private int walTlbSize;

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
    private int metricsSubIntervalCount;

    /** Time interval (in milliseconds) for rate-based metrics. */
    private long metricsRateTimeInterval;

    /** Time interval (in milliseconds) for running auto archiving for incompletely WAL segment */
    private long walAutoArchiveAfterInactivity;

    /** If true, threads that generate dirty pages too fast during ongoing checkpoint will be throttled. */
    private boolean writeThrottlingEnabled;

    /** Size of WAL buffer. */
    private int walBufSize;

    /** If true, system filters and compresses WAL archive in background. */
    private boolean walCompactionEnabled;

    /**
     * Default constructor.
     */
    public VisorDataStorageConfiguration() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cfg Data storage configuration.
     */
    public VisorDataStorageConfiguration(DataStorageConfiguration cfg) {
        assert cfg != null;

        sysRegionInitSize = cfg.getSystemRegionInitialSize();
        sysRegionMaxSize = cfg.getSystemRegionMaxSize();
        pageSize = cfg.getPageSize();
        concLvl = cfg.getConcurrencyLevel();

        DataRegionConfiguration dfltRegion = cfg.getDefaultDataRegionConfiguration();

        if (dfltRegion != null)
            dfltDataRegCfg = new VisorDataRegionConfiguration(dfltRegion);

        dataRegCfgs = VisorDataRegionConfiguration.from(cfg.getDataRegionConfigurations());

        storagePath = cfg.getStoragePath();
        checkpointFreq = cfg.getCheckpointFrequency();
        lockWaitTime = cfg.getLockWaitTime();
        checkpointThreads = cfg.getCheckpointThreads();
        checkpointWriteOrder = cfg.getCheckpointWriteOrder();
        walHistSize = cfg.getWalHistorySize();
        walSegments = cfg.getWalSegments();
        walSegmentSize = cfg.getWalSegmentSize();
        walPath = cfg.getWalPath();
        walArchivePath = cfg.getWalArchivePath();
        metricsEnabled = cfg.isMetricsEnabled();
        walMode = cfg.getWalMode();
        walTlbSize = cfg.getWalThreadLocalBufferSize();
        walBufSize = cfg.getWalBufferSize();
        walFlushFreq = cfg.getWalFlushFrequency();
        walFsyncDelay = cfg.getWalFsyncDelayNanos();
        walRecordIterBuffSize = cfg.getWalRecordIteratorBufferSize();
        alwaysWriteFullPages = cfg.isAlwaysWriteFullPages();
        fileIOFactory = compactClass(cfg.getFileIOFactory());
        metricsSubIntervalCount = cfg.getMetricsSubIntervalCount();
        metricsRateTimeInterval = cfg.getMetricsRateTimeInterval();
        walAutoArchiveAfterInactivity = cfg.getWalAutoArchiveAfterInactivity();
        writeThrottlingEnabled = cfg.isWriteThrottlingEnabled();
        walCompactionEnabled = cfg.isWalCompactionEnabled();
    }

    /**
     * @return Initial size in bytes.
     */
    public long getSystemRegionInitialSize() {
        return sysRegionInitSize;
    }

    /**
     * @return Maximum in bytes.
     */
    public long getSystemRegionMaxSize() {
        return sysRegionMaxSize;
    }

    /**
     * @return Page size in bytes.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @return Mapping table concurrency level.
     */
    public int getConcurrencyLevel() {
        return concLvl;
    }

    /**
     * @return Configuration of default data region.
     */
    public VisorDataRegionConfiguration getDefaultDataRegionConfiguration() {
        return dfltDataRegCfg;
    }

    /**
     * @return Array of configured data regions.
     */
    public List<VisorDataRegionConfiguration> getDataRegionConfigurations() {
        return dataRegCfgs;
    }

    /**
     * @return Path the root directory where the Persistent Store will persist data and indexes.
     */
    public String getStoragePath() {
        return storagePath;
    }

    /**
     * @return Checkpointing frequency in milliseconds.
     */
    public long getCheckpointFrequency() {
        return checkpointFreq;
    }

    /**
     * @return Number of checkpointing threads.
     */
    public int getCheckpointThreads() {
        return checkpointThreads;
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
    public String getWalPath() {
        return walPath;
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
    public long getMetricsRateTimeInterval() {
        return metricsRateTimeInterval;
    }

    /**
     * @return The number of sub-intervals for history tracking.
     */
    public int getMetricsSubIntervalCount() {
        return metricsSubIntervalCount;
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
    public int getWalThreadLocalBufferSize() {
        return walTlbSize;
    }

    /**
     * @return Flush frequency.
     */
    public long getWalFlushFrequency() {
        return walFlushFreq;
    }

    /**
     * @return Gets the fsync delay, in nanoseconds.
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
     * @return Flag indicating whether full pages should be always written.
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
     * @return Time in millis.
     */
    public long getWalAutoArchiveAfterInactivity() {
        return walAutoArchiveAfterInactivity;
    }

    /**
     * @return Flag indicating whether write throttling is enabled.
     */
    public boolean isWriteThrottlingEnabled() {
        return writeThrottlingEnabled;
    }

    /**
     * @return Size of WAL buffer.
     */
    public int getWalBufferSize() {
        return walBufSize;
    }

    /**
     * @return If true, system filters and compresses WAL archive in background
     */
    public boolean isWalCompactionEnabled() {
        return walCompactionEnabled;
    }

    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeLong(sysRegionInitSize);
        out.writeLong(sysRegionMaxSize);
        out.writeInt(pageSize);
        out.writeInt(concLvl);
        out.writeObject(dfltDataRegCfg);
        U.writeCollection(out, dataRegCfgs);
        U.writeString(out, storagePath);
        out.writeLong(checkpointFreq);
        out.writeLong(lockWaitTime);
        out.writeLong(0);  // Write stub for removed checkpointPageBufSize.
        out.writeInt(checkpointThreads);
        U.writeEnum(out, checkpointWriteOrder);
        out.writeInt(walHistSize);
        out.writeInt(walSegments);
        out.writeInt(walSegmentSize);
        U.writeString(out, walPath);
        U.writeString(out, walArchivePath);
        out.writeBoolean(metricsEnabled);
        U.writeEnum(out, walMode);
        out.writeInt(walTlbSize);
        out.writeLong(walFlushFreq);
        out.writeLong(walFsyncDelay);
        out.writeInt(walRecordIterBuffSize);
        out.writeBoolean(alwaysWriteFullPages);
        U.writeString(out, fileIOFactory);
        out.writeInt(metricsSubIntervalCount);
        out.writeLong(metricsRateTimeInterval);
        out.writeLong(walAutoArchiveAfterInactivity);
        out.writeBoolean(writeThrottlingEnabled);
        out.writeInt(walBufSize);
        out.writeBoolean(walCompactionEnabled);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        sysRegionInitSize = in.readLong();
        sysRegionMaxSize = in.readLong();
        pageSize = in.readInt();
        concLvl = in.readInt();
        dfltDataRegCfg = (VisorDataRegionConfiguration)in.readObject();
        dataRegCfgs = U.readList(in);
        storagePath = U.readString(in);
        checkpointFreq = in.readLong();
        lockWaitTime = in.readLong();
        in.readLong(); // Read stub for removed checkpointPageBufSize.
        checkpointThreads = in.readInt();
        checkpointWriteOrder = CheckpointWriteOrder.fromOrdinal(in.readByte());
        walHistSize = in.readInt();
        walSegments = in.readInt();
        walSegmentSize = in.readInt();
        walPath = U.readString(in);
        walArchivePath = U.readString(in);
        metricsEnabled = in.readBoolean();
        walMode = WALMode.fromOrdinal(in.readByte());
        walTlbSize = in.readInt();
        walFlushFreq = in.readLong();
        walFsyncDelay = in.readLong();
        walRecordIterBuffSize = in.readInt();
        alwaysWriteFullPages = in.readBoolean();
        fileIOFactory = U.readString(in);
        metricsSubIntervalCount = in.readInt();
        metricsRateTimeInterval = in.readLong();
        walAutoArchiveAfterInactivity = in.readLong();
        writeThrottlingEnabled = in.readBoolean();

        if (protoVer > V1) {
            walBufSize = in.readInt();
            walCompactionEnabled = in.readBoolean();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDataStorageConfiguration.class, this);
    }
}
