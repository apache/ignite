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

package org.apache.ignite.internal.visor.igfs;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Visor IGFS profiler information about one file.
 */
@Deprecated
public class VisorIgfsProfilerEntry extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Timestamp comparator. */
    public static final Comparator<VisorIgfsProfilerEntry> ENTRY_TIMESTAMP_COMPARATOR =
        new Comparator<VisorIgfsProfilerEntry>() {
            @Override public int compare(VisorIgfsProfilerEntry a, VisorIgfsProfilerEntry b) {
                return Long.compare(a.ts, b.ts);
            }
        };

    /** Path to file. */
    private String path;

    /** Timestamp of last file operation. */
    private long ts;

    /** IGFS mode. */
    private VisorIgfsMode mode;

    /** File size. */
    private long size;

    /** How many bytes were read. */
    private long bytesRead;

    /** How long read take. */
    private long readTime;

    /** User read time. */
    private long userReadTime;

    /** How many bytes were written. */
    private long bytesWritten;

    /** How long write take. */
    private long writeTime;

    /** User write read time. */
    private long userWriteTime;

    /** Calculated uniformity. */
    private double uniformity = -1;

    /** Counters for uniformity calculation. */
    private VisorIgfsProfilerUniformityCounters counters;

    /** Read speed in bytes per second or {@code -1} if speed not available. */
    private long readSpeed;

    /** Write speed in bytes per second or {@code -1} if speed not available. */
    private long writeSpeed;

    /**
     * Default constructor.
     */
    public VisorIgfsProfilerEntry() {
        // No-op.
    }

    /** Create data transfer object with given parameters. */
    public VisorIgfsProfilerEntry(
        String path,
        long ts,
        VisorIgfsMode mode,
        long size,
        long bytesRead,
        long readTime,
        long userReadTime,
        long bytesWritten,
        long writeTime,
        long userWriteTime,
        VisorIgfsProfilerUniformityCounters counters
    ) {
        assert counters != null;

        this.path = path;
        this.ts = ts;
        this.mode = mode;
        this.size = size;
        this.bytesRead = bytesRead;
        this.readTime = readTime;
        this.userReadTime = userReadTime;
        this.bytesWritten = bytesWritten;
        this.writeTime = writeTime;
        this.userWriteTime = userWriteTime;
        this.counters = counters;

        readSpeed = speed(bytesRead, readTime);
        writeSpeed = speed(bytesWritten, writeTime);
    }

    /**
     * Calculate speed of bytes processing.
     *
     * @param bytes How many bytes were processed.
     * @param time How long processing take (in nanoseconds).
     * @return Speed of processing in bytes per second or {@code -1} if speed not available.
     */
    private static long speed(long bytes, long time) {
        if (time > 0) {
            double bytesScaled = bytes * 100000d;
            double timeScaled = time / 10000d;

            return (long)(bytesScaled / timeScaled);
        }
        else
            return -1L;
    }

    /**
     * @return Path to file.
     */
    public String getPath() {
        return path;
    }

    /**
     * @return Timestamp of last file operation.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @return IGFS mode.
     */
    public VisorIgfsMode getMode() {
        return mode;
    }

    /**
     * @return File size.
     */
    public long getSize() {
        return size;
    }

    /**
     * @return How many bytes were read.
     */
    public long getBytesRead() {
        return bytesRead;
    }

    /**
     * @return How long read take.
     */
    public long getReadTime() {
        return readTime;
    }

    /**
     * @return User read time.
     */
    public long getUserReadTime() {
        return userReadTime;
    }

    /**
     * @return How many bytes were written.
     */
    public long getBytesWritten() {
        return bytesWritten;
    }

    /**
     * @return How long write take.
     */
    public long getWriteTime() {
        return writeTime;
    }

    /**
     * @return User write read time.
     */
    public long getUserWriteTime() {
        return userWriteTime;
    }

    /**
     * @return Calculated uniformity.
     */
    public double getUniformity() {
        if (uniformity < 0)
            uniformity = counters.calc();

        return uniformity;
    }

    /**
     * @return Counters for uniformity calculation.
     */
    public VisorIgfsProfilerUniformityCounters getCounters() {
        return counters;
    }

    /**
     * @return Read speed in bytes per second or {@code -1} if speed not available.
     */
    public long getReadSpeed() {
        return readSpeed;
    }

    /**
     * @return Write speed in bytes per second or {@code -1} if speed not available.
     */
    public long getWriteSpeed() {
        return writeSpeed;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, path);
        out.writeLong(ts);
        U.writeEnum(out, mode);
        out.writeLong(size);
        out.writeLong(bytesRead);
        out.writeLong(readTime);
        out.writeLong(userReadTime);
        out.writeLong(bytesWritten);
        out.writeLong(writeTime);
        out.writeLong(userWriteTime);
        out.writeDouble(uniformity);
        out.writeObject(counters);
        out.writeLong(readSpeed);
        out.writeLong(writeSpeed);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        path = U.readString(in);
        ts = in.readLong();
        mode = VisorIgfsMode.fromOrdinal(in.readByte());
        size = in.readLong();
        bytesRead = in.readLong();
        readTime = in.readLong();
        userReadTime = in.readLong();
        bytesWritten = in.readLong();
        writeTime = in.readLong();
        userWriteTime = in.readLong();
        uniformity = in.readDouble();
        counters = (VisorIgfsProfilerUniformityCounters)in.readObject();
        readSpeed = in.readLong();
        writeSpeed = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfsProfilerEntry.class, this);
    }
}
