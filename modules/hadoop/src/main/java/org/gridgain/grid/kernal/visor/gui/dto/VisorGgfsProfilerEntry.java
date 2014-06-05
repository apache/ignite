/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Visor GGFS profiler information about one file.
 */
public class VisorGgfsProfilerEntry implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Timestamp comparator. */
    public static final Comparator<VisorGgfsProfilerEntry> ENTRY_TIMESTAMP_COMPARATOR =
        new Comparator<VisorGgfsProfilerEntry>() {
            @Override public int compare(VisorGgfsProfilerEntry a, VisorGgfsProfilerEntry b) {
                return Long.compare(a.timestamp, b.timestamp);
        }
    };

    /** Path to file. */
    private final String path;

    /** Timestamp of last file operation. */
    private final long timestamp;

    /** GGFS mode. */
    private final GridGgfsMode mode;

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

    /** Counters for uniformity calculation.  */
    private final VisorGgfsProfilerUniformityCounters counters;

    /** Read speed in bytes per second or {@code -1} if speed not available. */
    private final long readSpeed;

    /** Write speed in bytes per second or {@code -1} if speed not available. */
    private final long writeSpeed;

    /** Create data transfer object with given parameters. */
    public VisorGgfsProfilerEntry(
        String path,
        long timestamp,
        GridGgfsMode mode,
        long size,
        long bytesRead,
        long readTime,
        long userReadTime,
        long bytesWritten,
        long writeTime,
        long userWriteTime,
        VisorGgfsProfilerUniformityCounters counters
    ) {
        assert counters != null;

        this.path = path;
        this.timestamp = timestamp;
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
        return timestamp;
    }

    /**
     * @return GGFS mode.
     */
    public GridGgfsMode mode() {
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
    public VisorGgfsProfilerUniformityCounters counters() {
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
        return S.toString(VisorGgfsProfilerEntry.class, this);
    }
}
