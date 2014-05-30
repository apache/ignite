/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.ggfs.GridGgfsMode;

import java.util.List;

/**
 * Various global constants for GGFS profiler.
 */
public class VisorGgfsProfiler {
    public static final int UNIFORMITY_DFLT_BLOCK_SIZE = 4096;
    public static final int UNIFORMITY_BLOCKS = 100;

    /**
     * Aggregate GGFS profiler entries.
     *
     * @param lines Lines to sum.
     * @return Single aggregated entry.
     */
    public static VisorGgfsProfilerEntry aggregateGgfsProfilerEntries(List<VisorGgfsProfilerEntry> lines) {
        assert lines != null;
        assert !lines.isEmpty();

        if (lines.size() == 1) {
            return lines.get(0); // No need to aggregate.
        }
        else {
            String path = lines.get(0).path();

            long timestamp = 0;
            long size = 0;
            long bytesRead = 0;
            long readTime = 0;
            long userReadTime = 0;
            long bytesWritten = 0;
            long writeTime = 0;
            long userWriteTime = 0;
            GridGgfsMode mode = null;
            VisorGgfsProfilerUniformityCounters counters = new VisorGgfsProfilerUniformityCounters();

            // TODO
//            lines.toSeq.sortBy(_.timestamp).foreach(line => {
//                // Take last timestamp.
//                timestamp = line.timestamp
//
//                // Take last size.
//                size = line.size
//
//                // Take last size.
//                mode = line.mode
//
//                // Aggregate metrics.
//                bytesRead += line.bytesRead
//                readTime += line.readTime
//                userReadTime += line.userReadTime
//                bytesWritten += line.bytesWritten
//                writeTime += line.writeTime
//                userWriteTime += line.userWriteTime
//
//                counters.aggregate(line.counters)
//            })

            return new VisorGgfsProfilerEntry(path, timestamp, mode, size, bytesRead, readTime, userReadTime,
                bytesWritten, writeTime, userWriteTime, counters);
        }
    }

}
