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

import java.io.Serializable;
import java.util.Comparator;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Visor IGFS profiler information about one file.
 */
public class VisorIgfsProfilerEntry implements Serializable {
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
    private final String path;

    /** Timestamp of last file operation. */
    private final long ts;

    /** IGFS mode. */
    private final IgfsMode mode;

    /** File size. */
    private final long size;

    /** How many bytes were read. */
    private final long bytesRead;

    /** How long read take. */
    private final long readTime;

    /** User read time. */
    private final long userReadTime;

    /** How many bytes were written. */
    private final long bytesWritten;

    /** How long write take. */
    private final long writeTime;

    /** User write read time. */
    private final long userWriteTime;

    /** Calculated uniformity. */
    private double uniformity = -1;

    /** Counters for uniformity calculation. */
    private final VisorIgfsProfilerUniformityCounters counters;

    /** Read speed in bytes per second or {@code -1} if speed not available. */
    private final long readSpeed;

    /** Write speed in bytes per second or {@code -1} if speed not available. */
    private final long writeSpeed;

    /** Create data transfer object with given parameters. */
    public VisorIgfsProfilerEntry(
        String path,
        long ts,
        IgfsMode mode,
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
    public String path() {
        return path;
    }

    /**
     * @return Timestamp of last file operation.
     */
    public long timestamp() {
        return ts;
    }

    /**
     * @return IGFS mode.
     */
    public IgfsMode mode() {
        return mode;
    }

    /**
     * @return File size.
     */
    public long size() {
        return size;
    }

    /**
     * @return How many bytes were read.
     */
    public long bytesRead() {
        return bytesRead;
    }

    /**
     * @return How long read take.
     */
    public long readTime() {
        return readTime;
    }

    /**
     * @return User read time.
     */
    public long userReadTime() {
        return userReadTime;
    }

    /**
     * @return How many bytes were written.
     */
    public long bytesWritten() {
        return bytesWritten;
    }

    /**
     * @return How long write take.
     */
    public long writeTime() {
        return writeTime;
    }

    /**
     * @return User write read time.
     */
    public long userWriteTime() {
        return userWriteTime;
    }

    /**
     * @return Calculated uniformity.
     */
    public double uniformity() {
        if (uniformity < 0)
            uniformity = counters.calc();

        return uniformity;
    }

    /**
     * @return Counters for uniformity calculation.
     */
    public VisorIgfsProfilerUniformityCounters counters() {
        return counters;
    }

    /**
     * @return Read speed in bytes per second or {@code -1} if speed not available.
     */
    public long readSpeed() {
        return readSpeed;
    }

    /**
     * @return Write speed in bytes per second or {@code -1} if speed not available.
     */
    public long writeSpeed() {
        return writeSpeed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfsProfilerEntry.class, this);
    }
}