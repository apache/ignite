/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.ggfs;

import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

/**
 * Various global constants for GGFS profiler.
 */
public class VisorGgfsProfiler {
    /** Default file block size to calculate uniformity. */
    public static final int UNIFORMITY_DFLT_BLOCK_SIZE = 4096;

    /** Default number of blocks to split file for uniformity calculations. */
    public static final int UNIFORMITY_BLOCKS = 100;

    /**
     * Aggregate GGFS profiler entries.
     *
     * @param entries Entries to sum.
     * @return Single aggregated entry.
     */
    public static VisorGgfsProfilerEntry aggregateGgfsProfilerEntries(List<VisorGgfsProfilerEntry> entries) {
        assert !F.isEmpty(entries);

        if (entries.size() == 1)
            return entries.get(0); // No need to aggregate.
        else {
            String path = entries.get(0).path();

            Collections.sort(entries, VisorGgfsProfilerEntry.ENTRY_TIMESTAMP_COMPARATOR);

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

            for (VisorGgfsProfilerEntry entry : entries) {
                // Take last timestamp.
                timestamp = entry.timestamp();

                // Take last size.
                size = entry.size();

                // Take last mode.
                mode = entry.mode();

                // Aggregate metrics.
                bytesRead += entry.bytesRead();
                readTime += entry.readTime();
                userReadTime += entry.userReadTime();
                bytesWritten += entry.bytesWritten();
                writeTime += entry.writeTime();
                userWriteTime += entry.userWriteTime();

                counters.aggregate(entry.counters());
            }

            return new VisorGgfsProfilerEntry(path, timestamp, mode, size, bytesRead, readTime, userReadTime,
                bytesWritten, writeTime, userWriteTime, counters);
        }
    }

}
