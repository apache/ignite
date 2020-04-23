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
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * DTO object for {@link DataStorageConfiguration}.
 */
public class VisorPersistentStoreConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

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

    /** Number of sub-intervals. */
    private int subIntervals;

    /** Time interval (in milliseconds) for rate-based metrics. */
    private long rateTimeInterval;

    /**
     * Default constructor.
     */
    public VisorPersistentStoreConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Persistent store configuration.
     */
    public VisorPersistentStoreConfiguration(DataStorageConfiguration cfg) {
        persistenceStorePath = cfg.getStoragePath();
        checkpointingFreq = cfg.getCheckpointFrequency();
        lockWaitTime = cfg.getLockWaitTime();
        checkpointingThreads = cfg.getCheckpointThreads();
        walHistSize = cfg.getWalHistorySize();
        walSegments = cfg.getWalSegments();
        walSegmentSize = cfg.getWalSegmentSize();
        walStorePath = cfg.getWalPath();
        walArchivePath = cfg.getWalArchivePath();
        metricsEnabled = cfg.isMetricsEnabled();
        walMode = cfg.getWalMode();
        tlbSize = cfg.getWalBufferSize();
        walFlushFreq = cfg.getWalFlushFrequency();
        walFsyncDelay = cfg.getWalFsyncDelayNanos();
        walRecordIterBuffSize = cfg.getWalRecordIteratorBufferSize();
        alwaysWriteFullPages = cfg.isAlwaysWriteFullPages();
        subIntervals = cfg.getMetricsSubIntervalCount();
        rateTimeInterval = cfg.getMetricsRateTimeInterval();
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

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, persistenceStorePath);
        out.writeLong(checkpointingFreq);
        out.writeLong(lockWaitTime);
        out.writeLong(checkpointingPageBufSize);
        out.writeInt(checkpointingThreads);
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
        out.writeInt(subIntervals);
        out.writeLong(rateTimeInterval);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        persistenceStorePath = U.readString(in);
        checkpointingFreq = in.readLong();
        lockWaitTime = in.readLong();
        checkpointingPageBufSize = in.readLong();
        checkpointingThreads = in.readInt();
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
        subIntervals = in.readInt();
        rateTimeInterval = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPersistentStoreConfiguration.class, this);
    }
}
