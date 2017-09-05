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
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * DTO object for {@link PersistenceMetrics}.
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

    /**
     * Default constructor.
     */
    public VisorPersistenceMetrics() {
        // No-op.
    }

    /**
     * @param metrics Persistence metrics.
     */
    public VisorPersistenceMetrics(PersistenceMetrics metrics) {
        walLoggingRate = metrics.getWalLoggingRate();
        walWritingRate = metrics.getWalWritingRate();
        walArchiveSegments = metrics.getWalArchiveSegments();
        walFsyncTimeAvg = metrics.getWalFsyncTimeAverage();
        lastCpDuration = metrics.getLastCheckpointingDuration();
        lastCpLockWaitDuration = metrics.getLastCheckpointLockWaitDuration();
        lastCpMmarkDuration = metrics.getLastCheckpointMarkDuration();
        lastCpPagesWriteDuration = metrics.getLastCheckpointPagesWriteDuration();
        lastCpFsyncDuration = metrics.getLastCheckpointFsyncDuration();
        lastCpTotalPages = metrics.getLastCheckpointTotalPagesNumber();
        lastCpDataPages = metrics.getLastCheckpointDataPagesNumber();
        lastCpCowPages = metrics.getLastCheckpointCopiedOnWritePagesNumber();
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
    public float getWalWritingRate(){
        return walWritingRate;
    }

    /**
     * @return Current number of WAL segments in the WAL archive.
     */
    public int getWalArchiveSegments(){
        return walArchiveSegments;
    }

    /**
     * @return Average WAL fsync duration in microseconds over the last time interval.
     */
    public float getWalFsyncTimeAverage(){
        return walFsyncTimeAvg;
    }

    /**
     * @return Total checkpoint duration in milliseconds.
     */
    public long getLastCheckpointingDuration(){
        return lastCpDuration;
    }

    /**
     * @return Checkpoint lock wait time in milliseconds.
     */
    public long getLastCheckpointLockWaitDuration(){
        return lastCpLockWaitDuration;
    }

    /**
     * @return Checkpoint mark duration in milliseconds.
     */
    public long getLastCheckpointMarkDuration(){
        return lastCpMmarkDuration;
    }

    /**
     * @return Checkpoint pages write phase in milliseconds.
     */
    public long getLastCheckpointPagesWriteDuration(){
        return lastCpPagesWriteDuration;
    }

    /**
     * @return Checkpoint fsync time in milliseconds.
     */
    public long getLastCheckpointFsyncDuration(){
        return lastCpFsyncDuration;
    }

    /**
     * @return Total number of pages written during the last checkpoint.
     */
    public long getLastCheckpointTotalPagesNumber(){
        return lastCpTotalPages;
    }

    /**
     * @return Total number of data pages written during the last checkpoint.
     */
    public long getLastCheckpointDataPagesNumber(){
        return lastCpDataPages;
    }

    /**
     * @return Total number of pages copied to a temporary checkpoint buffer during the last checkpoint.
     */
    public long getLastCheckpointCopiedOnWritePagesNumber(){
        return lastCpCowPages;
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
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorPersistenceMetrics.class, this);
    }
}
