/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.ggfs.*;

import java.io.*;

/**
 * Visor GGFS profiler information about one file.
 */
public class VisorGgfsProfilerEntry implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

//    /** Path to file. */
//    private final String path;
//
//    /** Timestamp of last file operation. */
//    private final long timestamp;
//
//    /** GGFS mode. */
//    private final GridGgfsMode mode;
//
//    /** File size. */
//    private final long size;
//
//    /** How many bytes were read. */
//    private final long bytesRead;
//
//    /** How long read take. */
//    private final long readTime;
//
//    /** User read time. */
//    private final long userReadTime;
//
//    /** How many bytes were written. */
//    private final long bytesWritten;
//
//    /** How long write take. */
//    private final long writeTime;
//
//    /** User write read time. */
//    private final long userWriteTime;
//
//    /** Calculated uniformity. */
//    private final double uniformity;
//
//    /** Counters for uniformity calculation.  */
//    private final VisorGgfsProfilerUniformityCounters counters;
//
//    /**
//     * Calculate speed of bytes processing.
//     *
//     * @param bytes How many bytes were processed.
//     * @param time How long processing take (in nanoseconds).
//     * @return Speed of processing in bytes per second or `-1` if speed not available.
//     */
//    private private final speed(bytes: Long, time: Long): Long = {
//        if (time > 0) {
//            val bytesScaled = bytes * 100000d
//            val timeScaled = time / 10000d
//
//            (bytesScaled / timeScaled).toLong
//        }
//
//        else
//            -1
//    }
//
//    /**
//     * Calculate read speed.
//     *
//     * @return Read speed in bytes per second or `-1` if speed not available..
//     */
//    private final readSpeed = speed(bytesRead, readTime)
//
//    /**
//     * Calculate write speed.
//     *
//     * @return Write speed in bytes per second or `-1` if speed not available..
//     */
//    private final writeSpeed = speed(bytesWritten, writeTime)
}
