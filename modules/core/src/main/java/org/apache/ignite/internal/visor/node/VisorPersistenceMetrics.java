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
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * DTO object for {@link DataStorageMetrics}.
 */
public class VisorPersistenceMetrics extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private float walLoggingRate;

    /** */
    private float walWritingRate;

    /** */
    private int walArchiveSegments;

    /** */
    private float walFsyncTimeAvg;

    /** */
    private long walBufPollSpinRate;

    /** */
    private long walSz;

    /** */
    private long walLastRollOverTm;

    /** */
    private long cpTotalTm;

    /** */
    private long lastCpDuration;

    /** */
    private long lastCpLockWaitDuration;

    /** */
    private long lastCpMmarkDuration;

    /** */
    private long lastCpPagesWriteDuration;

    /** */
    private long lastCpFsyncDuration;

    /** */
    private long lastCpTotalPages;

    /** */
    private long lastCpDataPages;

    /** */
    private long lastCpCowPages;

    /** */
    private long dirtyPages;

    /** */
    private long pagesRead;

    /** */
    private long pagesWritten;

    /** */
    private long pagesReplaced;

    /** */
    private long offHeapSz;

    /** */
    private long offheapUsedSz;

    /** */
    private long usedCpBufPages;

    /** */
    private long usedCpBufSz;

    /** */
    private long cpBufSz;

    /** */
    private long totalSz;

    /**
     * Default constructor.
     */
    public VisorPersistenceMetrics() {
        // No-op.
    }

    /**
     * @param m Persistence metrics.
     */
    public VisorPersistenceMetrics(DataStorageMetrics m) {
        walLoggingRate = m.getWalLoggingRate();
        walWritingRate = m.getWalWritingRate();
        walArchiveSegments = m.getWalArchiveSegments();
        walFsyncTimeAvg = m.getWalFsyncTimeAverage();
        walBufPollSpinRate = m.getWalBuffPollSpinsRate();
        walSz = m.getWalTotalSize();
        walLastRollOverTm = m.getWalLastRollOverTime();

        cpTotalTm = m.getCheckpointTotalTime();

        lastCpDuration = m.getLastCheckpointDuration();
        lastCpLockWaitDuration = m.getLastCheckpointLockWaitDuration();
        lastCpMmarkDuration = m.getLastCheckpointMarkDuration();
        lastCpPagesWriteDuration = m.getLastCheckpointPagesWriteDuration();
        lastCpFsyncDuration = m.getLastCheckpointFsyncDuration();
        lastCpTotalPages = m.getLastCheckpointTotalPagesNumber();
        lastCpDataPages = m.getLastCheckpointDataPagesNumber();
        lastCpCowPages = m.getLastCheckpointCopiedOnWritePagesNumber();

        dirtyPages = m.getDirtyPages();
        pagesRead = m.getPagesRead();
        pagesWritten = m.getPagesWritten();
        pagesReplaced = m.getPagesReplaced();

        offHeapSz = m.getOffHeapSize();
        offheapUsedSz = m.getOffheapUsedSize();

        usedCpBufPages = m.getUsedCheckpointBufferPages();
        usedCpBufSz = m.getUsedCheckpointBufferSize();
        cpBufSz = m.getCheckpointBufferSize();

        totalSz = m.getTotalAllocatedSize();
    }

    /**
     * @return Average number of WAL records per second written during the last time interval.
     */
    public float getWalLoggingRate() {
        return walLoggingRate;
    }

    /**
     * @return Average number of bytes per second written during the last time interval.
     */
    public float getWalWritingRate() {
        return walWritingRate;
    }

    /**
     * @return Current number of WAL segments in the WAL archive.
     */
    public int getWalArchiveSegments() {
        return walArchiveSegments;
    }

    /**
     * @return Average WAL fsync duration in microseconds over the last time interval.
     */
    public float getWalFsyncTimeAverage() {
        return walFsyncTimeAvg;
    }

    /**
     * @return WAL buffer poll spins number over the last time interval.
     */
    public long getWalBuffPollSpinsRate() {
        return walBufPollSpinRate;
    }

    /**
     * @return Total size in bytes for storage WAL files.
     */
    public long getWalTotalSize() {
        return walSz;
    }

    /**
     * @return Time of the last WAL segment rollover.
     */
    public long getWalLastRollOverTime() {
        return walLastRollOverTm;
    }

    /**
     * @return Total checkpoint time from last restart.
     */
    public long getCheckpointTotalTime() {
        return cpTotalTm;
    }

    /**
     * @return Total checkpoint duration in milliseconds.
     */
    public long getLastCheckpointingDuration() {
        return lastCpDuration;
    }

    /**
     * @return Checkpoint lock wait time in milliseconds.
     */
    public long getLastCheckpointLockWaitDuration() {
        return lastCpLockWaitDuration;
    }

    /**
     * @return Checkpoint mark duration in milliseconds.
     */
    public long getLastCheckpointMarkDuration() {
        return lastCpMmarkDuration;
    }

    /**
     * @return Checkpoint pages write phase in milliseconds.
     */
    public long getLastCheckpointPagesWriteDuration() {
        return lastCpPagesWriteDuration;
    }

    /**
     * @return Checkpoint fsync time in milliseconds.
     */
    public long getLastCheckpointFsyncDuration() {
        return lastCpFsyncDuration;
    }

    /**
     * @return Total number of pages written during the last checkpoint.
     */
    public long getLastCheckpointTotalPagesNumber() {
        return lastCpTotalPages;
    }

    /**
     * @return Total number of data pages written during the last checkpoint.
     */
    public long getLastCheckpointDataPagesNumber() {
        return lastCpDataPages;
    }

    /**
     * @return Total number of pages copied to a temporary checkpoint buffer during the last checkpoint.
     */
    public long getLastCheckpointCopiedOnWritePagesNumber() {
        return lastCpCowPages;
    }

    /**
     * @return Total dirty pages for the next checkpoint.
     */
    public long getDirtyPages() {
        return dirtyPages;
    }

    /**
     * @return The number of read pages from last restart.
     */
    public long getPagesRead() {
        return pagesRead;
    }

    /**
     * @return The number of written pages from last restart.
     */
    public long getPagesWritten() {
        return pagesWritten;
    }

    /**
     * @return The number of replaced pages from last restart.
     */
    public long getPagesReplaced() {
        return pagesReplaced;
    }

    /**
     * @return Total offheap size in bytes.
     */
    public long getOffHeapSize() {
        return offHeapSz;
    }

    /**
     * @return Total used offheap size in bytes.
     */
    public long getOffheapUsedSize() {
        return offheapUsedSz;
    }

    /**
     * @return Checkpoint buffer size in pages.
     */
    public long getUsedCheckpointBufferPages() {
        return usedCpBufPages;
    }

    /**
     * @return Checkpoint buffer size in bytes.
     */
    public long getUsedCheckpointBufferSize() {
        return usedCpBufSz;
    }

    /**
     * @return Checkpoint buffer size in bytes.
     */
    public long getCheckpointBufferSize() {
        return cpBufSz;
    }

    /**
     * @return Total size of memory allocated in bytes.
     */
    public long getTotalAllocatedSize() {
        return totalSz;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeFloat(walLoggingRate);
        out.writeFloat(walWritingRate);
        out.writeInt(walArchiveSegments);
        out.writeFloat(walFsyncTimeAvg);
        out.writeLong(lastCpDuration);
        out.writeLong(lastCpLockWaitDuration);
        out.writeLong(lastCpMmarkDuration);
        out.writeLong(lastCpPagesWriteDuration);
        out.writeLong(lastCpFsyncDuration);
        out.writeLong(lastCpTotalPages);
        out.writeLong(lastCpDataPages);
        out.writeLong(lastCpCowPages);

        // V2
        out.writeLong(walBufPollSpinRate);
        out.writeLong(walSz);
        out.writeLong(walLastRollOverTm);
        out.writeLong(cpTotalTm);
        out.writeLong(dirtyPages);
        out.writeLong(pagesRead);
        out.writeLong(pagesWritten);
        out.writeLong(pagesReplaced);
        out.writeLong(offHeapSz);
        out.writeLong(offheapUsedSz);
        out.writeLong(usedCpBufPages);
        out.writeLong(usedCpBufSz);
        out.writeLong(cpBufSz);
        out.writeLong(totalSz);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        walLoggingRate = in.readFloat();
        walWritingRate = in.readFloat();
        walArchiveSegments = in.readInt();
        walFsyncTimeAvg = in.readFloat();
        lastCpDuration = in.readLong();
        lastCpLockWaitDuration = in.readLong();
        lastCpMmarkDuration = in.readLong();
        lastCpPagesWriteDuration = in.readLong();
        lastCpFsyncDuration = in.readLong();
        lastCpTotalPages = in.readLong();
        lastCpDataPages = in.readLong();
        lastCpCowPages = in.readLong();

        if (protoVer > V1) {
            walBufPollSpinRate = in.readLong();
            walSz = in.readLong();
            walLastRollOverTm = in.readLong();
            cpTotalTm = in.readLong();
            dirtyPages = in.readLong();
            pagesRead = in.readLong();
            pagesWritten = in.readLong();
            pagesReplaced = in.readLong();
            offHeapSz = in.readLong();
            offheapUsedSz = in.readLong();
            usedCpBufPages = in.readLong();
            usedCpBufSz = in.readLong();
            cpBufSz = in.readLong();
            totalSz = in.readLong();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPersistenceMetrics.class, this);
    }
}
