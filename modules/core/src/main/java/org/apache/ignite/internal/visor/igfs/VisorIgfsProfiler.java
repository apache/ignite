/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
            String path = entries.get(0).getPath();

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
                ts = entry.getTimestamp();

                // Take last size.
                size = entry.getSize();

                // Take last mode.
                mode = entry.getMode();

                // Aggregate metrics.
                bytesRead += entry.getBytesRead();
                readTime += entry.getReadTime();
                userReadTime += entry.getUserReadTime();
                bytesWritten += entry.getBytesWritten();
                writeTime += entry.getWriteTime();
                userWriteTime += entry.getUserWriteTime();

                counters.aggregate(entry.getCounters());
            }

            return new VisorIgfsProfilerEntry(path, ts, mode, size, bytesRead, readTime, userReadTime,
                bytesWritten, writeTime, userWriteTime, counters);
        }
    }

}
