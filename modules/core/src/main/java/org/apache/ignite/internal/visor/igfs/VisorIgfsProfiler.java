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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Various global constants for IGFS profiler.
 */
public class VisorIgfsProfiler {
    /** Default file block size to calculate uniformity. */
    public static final int UNIFORMITY_DFLT_BLOCK_SIZE = 4096;

    /** Default number of blocks to split file for uniformity calculations. */
    public static final int UNIFORMITY_BLOCKS = 100;

    /**
     * Aggregate IGFS profiler entries.
     *
     * @param entries Entries to sum.
     * @return Single aggregated entry.
     */
    public static VisorIgfsProfilerEntry aggregateIgfsProfilerEntries(List<VisorIgfsProfilerEntry> entries) {
        assert !F.isEmpty(entries);

        if (entries.size() == 1)
            return entries.get(0); // No need to aggregate.
        else {
            String path = entries.get(0).path();

            Collections.sort(entries, VisorIgfsProfilerEntry.ENTRY_TIMESTAMP_COMPARATOR);

            long ts = 0;
            long size = 0;
            long bytesRead = 0;
            long readTime = 0;
            long userReadTime = 0;
            long bytesWritten = 0;
            long writeTime = 0;
            long userWriteTime = 0;
            IgfsMode mode = null;
            VisorIgfsProfilerUniformityCounters counters = new VisorIgfsProfilerUniformityCounters();

            for (VisorIgfsProfilerEntry entry : entries) {
                // Take last timestamp.
                ts = entry.timestamp();

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

            return new VisorIgfsProfilerEntry(path, ts, mode, size, bytesRead, readTime, userReadTime,
                bytesWritten, writeTime, userWriteTime, counters);
        }
    }

}