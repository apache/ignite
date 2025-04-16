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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;

/**
 * The interface represents performance statistics operations collection for purposes of troubleshooting and
 * performance analysis.
 */
public interface PerformanceStatisticsHandler {
    /**
     * @param nodeId Node id.
     * @param cacheId Cache id.
     * @param name Cache name.
     */
    void cacheStart(UUID nodeId, int cacheId, String name);

    /**
     * @param nodeId Node id.
     * @param type Operation type.
     * @param cacheId Cache id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     */
    void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime, long duration);

    /**
     * @param nodeId Node id.
     * @param cacheIds Cache IDs.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param commited {@code True} if commited.
     */
    void transaction(UUID nodeId, GridIntList cacheIds, long startTime, long duration, boolean commited);

    /**
     * @param nodeId Node id.
     * @param type Cache query type.
     * @param text Query text in case of SQL query. Cache name in case of SCAN query.
     * @param id Query id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param success Success flag.
     */
    void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime, long duration,
        boolean success);

    /**
     * @param nodeId Node id.
     * @param type Cache query type.
     * @param queryNodeId Originating node id.
     * @param id Query id.
     * @param logicalReads Number of logical reads.
     * @param physicalReads Number of physical reads.
     */
    void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads,
        long physicalReads);

    /**
     * Count of rows processed by query.
     *
     * @param nodeId Node id.
     * @param type Cache query type.
     * @param qryNodeId Originating node id.
     * @param id Query id.
     * @param action Action with rows.
     * @param rows Number of rows processed.
     */
    void queryRows(UUID nodeId, GridCacheQueryType type, UUID qryNodeId, long id, String action, long rows);

    /**
     * Custom query property.
     *
     * @param nodeId Node id.
     * @param type Cache query type.
     * @param qryNodeId Originating node id.
     * @param id Query id.
     * @param name Query property name.
     * @param val Query property value.
     */
    void queryProperty(UUID nodeId, GridCacheQueryType type, UUID qryNodeId, long id, String name, String val);

    /**
     * @param nodeId Node id.
     * @param sesId Session id.
     * @param taskName Task name.
     * @param startTime Start time in milliseconds.
     * @param duration Duration.
     * @param affPartId Affinity partition id.
     */
    void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId);

    /**
     * @param nodeId Node id.
     * @param sesId Session id.
     * @param queuedTime Time job spent on waiting queue.
     * @param startTime Start time in milliseconds.
     * @param duration Job execution time.
     * @param timedOut {@code True} if job is timed out.
     */
    void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut);

    /**
     * @param nodeId Node id.
     * @param beforeLockDuration Before lock duration.
     * @param lockWaitDuration Lock wait duration.
     * @param listenersExecDuration Listeners execute duration.
     * @param markDuration Mark duration.
     * @param lockHoldDuration Lock hold duration.
     * @param pagesWriteDuration Pages write duration.
     * @param fsyncDuration Fsync duration.
     * @param walCpRecordFsyncDuration Wal cp record fsync duration.
     * @param writeCpEntryDuration Write checkpoint entry duration.
     * @param splitAndSortCpPagesDuration Split and sort cp pages duration.
     * @param recoveryDataWriteDuration Recovery data write duration.
     * @param totalDuration Total duration in milliseconds.
     * @param cpStartTime Checkpoint start time in milliseconds.
     * @param pagesSize Pages size.
     * @param dataPagesWritten Data pages written.
     * @param cowPagesWritten Cow pages written.
     */
    void checkpoint(
        UUID nodeId,
        long beforeLockDuration,
        long lockWaitDuration,
        long listenersExecDuration,
        long markDuration,
        long lockHoldDuration,
        long pagesWriteDuration,
        long fsyncDuration,
        long walCpRecordFsyncDuration,
        long writeCpEntryDuration,
        long splitAndSortCpPagesDuration,
        long recoveryDataWriteDuration,
        long totalDuration,
        long cpStartTime,
        int pagesSize,
        int dataPagesWritten,
        int cowPagesWritten
    );

    /**
     * @param nodeId Node id.
     * @param endTime End time in milliseconds.
     * @param duration Duration in milliseconds.
     */
    void pagesWriteThrottle(UUID nodeId, long endTime, long duration);

    /**
     * @param id    Node id.
     * @param name  Name of system view.
     * @param schema Attributes of system view.
     * @param row System view row.
     */
    void systemView(UUID id, String name, List<String> schema, List<Object> row);
}
