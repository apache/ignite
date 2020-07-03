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

package org.apache.ignite.internal.performancestatistics;

import java.io.File;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsReader;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsHandler;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERFORMANCE_STAT_DIR;

/**
 * Test performance statistics file reader.
 */
public class TestFilePerformanceStatisticsReader {
    /**
     * Reads statistics to a log.
     *
     * @param log Log to write operations to.
     */
    public static void readToLog(IgniteLogger log) throws Exception {
        File dir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERFORMANCE_STAT_DIR, false);

        FilePerformanceStatisticsReader.read(Collections.singletonList(dir), new LogMessageHandler(log));
    }

    /** The handler that writes handled operations to the log. */
    private static class LogMessageHandler implements PerformanceStatisticsHandler {
        /** Log to write operations to. */
        private final IgniteLogger log;

        /** @param log Log to write operations to. */
        private LogMessageHandler(IgniteLogger log) {
            this.log = log;
        }

        /** {@inheritDoc} */
        @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime,
            long duration) {
            log("cacheOperation", "nodeId", nodeId, "type", type, "cacheId", cacheId,
                "startTime", startTime, "duration", duration);
        }

        /** {@inheritDoc} */
        @Override public void transaction(UUID nodeId, GridIntList cacheIds, long startTime, long duration,
            boolean commited) {
            log("transaction", "nodeId", nodeId, "cacheIds", cacheIds,
                "startTime", startTime, "duration", duration, "commited", commited);
        }

        /** {@inheritDoc} */
        @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
            long duration, boolean success) {
            log("query", "nodeId", nodeId, "type", type, "text", text, "id", id,
                "startTime", startTime, "duration", duration, "success", success);
        }

        /** {@inheritDoc} */
        @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
            long logicalReads, long physicalReads) {
            log("queryReads", "nodeId", nodeId, "type", type, "queryNodeId", queryNodeId, "id", id,
                "logicalReads", logicalReads, "physicalReads", physicalReads);
        }

        /** {@inheritDoc} */
        @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
            int affPartId) {
            log("task", "nodeId", nodeId, "sesId", sesId, "taskName", taskName,
                "startTime", startTime, "duration", duration, "affPartId", affPartId);
        }

        /** {@inheritDoc} */
        @Override public void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long startTime, long duration,
            boolean timedOut) {
            log("job", "nodeId", nodeId, "sesId", sesId, "queuedTime", queuedTime,
                "startTime", startTime, "duration", duration, "timedOut", timedOut);
        }

        /**
         * Logs operations.
         *
         * @param category Profile category.
         * @param tuples   Tuples to log (key, value).
         */
        private void log(String category, Object... tuples) {
            assert tuples.length % 2 == 0;

            StringBuilder sb = new StringBuilder();

            sb.append(category).append(" [");

            for (int i = 0; i < tuples.length; i += 2) {
                sb.append(tuples[i]).append("=").append(tuples[i + 1]);

                if (i < tuples.length - 2)
                    sb.append(", ");
            }

            sb.append(']');

            log.info(sb.toString());
        }
    }
}
