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

package org.apache.ignite.internal.performancestatistics.handlers;

import java.util.Map;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsHandler;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;

/**
 * The interface represents performance statistics operations handler to build JSON for UI views.
 */
public interface IgnitePerformanceStatisticsHandler extends PerformanceStatisticsHandler {
    /**
     * Map of named JSON results.
     *
     * @return Result map.
     */
    Map<String, JsonNode> results();

    /** {@inheritDoc} */
    @Override default void cacheStart(UUID nodeId, int cacheId, String name) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override default void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime, long duration) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override default void transaction(UUID nodeId, GridIntList cacheIds, long startTime, long duration,
        boolean commited) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override default void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
        long duration, boolean success) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override default void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
        long logicalReads, long physicalReads) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override default void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
        int affPartId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override default void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long startTime, long duration,
        boolean timedOut) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override default void checkpoint(UUID nodeId, long beforeLockDuration, long lockWaitDuration,
        long listenersExecDuration, long markDuration, long lockHoldDuration, long pagesWriteDuration,
        long fsyncDuration, long walCpRecordFsyncDuration, long writeCpEntryDuration, long splitAndSortCpPagesDuration,
        long totalDuration, long cpStartTime, int pagesSize, int dataPagesWritten, int cowPagesWritten) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override default void pagesWriteThrottle(UUID nodeId, long endTime, long duration) {
        // No-op.
    }
}
