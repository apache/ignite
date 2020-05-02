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

package org.apache.ignite.internal.profiling;

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/** Profiles operations. */
public interface IgniteProfiling {
    /**
     * @param type Operation type.
     * @param cacheId Cache id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     */
    void cacheOperation(CacheOperationType type, int cacheId, long startTime, long duration);

    /**
     * @param cacheIds Cache IDs.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param commit {@code True} if commited.
     */
    void transaction(GridIntList cacheIds, long startTime, long duration, boolean commit);

    /**
     * @param type Cache query type.
     * @param text Query text in case of SQL query. Cache name in case of SCAN query.
     * @param queryNodeId Originating node id.
     * @param id Query id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param success Success flag.
     */
    void query(GridCacheQueryType type, String text, UUID queryNodeId, long id, long startTime, long duration,
        boolean success);

    /**
     * @param type Cache query type.
     * @param queryNodeId Originating node id.
     * @param id Query id.
     * @param logicalReads Number of logical reads.
     * @param physicalReads Number of physical reads.
     */
    void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads, long physicalReads);

    /**
     * @param sesId Session id.
     * @param taskName Task name.
     * @param startTime Start time in milliseconds.
     * @param duration Duration.
     * @param affPartId Affinity partition id.
     */
    void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId);

    /**
     * @param sesId Session id.
     * @param queuedTime Time job spent on waiting queue.
     * @param startTime Start time in milliseconds.
     * @param duration Job execution time.
     * @param timedOut {@code True} if job is timed out.
     */
    void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut);

    /**
     * @param cacheId Cache id.
     * @param startTime Start time in milliseconds.
     * @param cacheName Cache name.
     * @param groupName Group name.
     * @param userCache User cache flag.
     */
    void cacheStart(int cacheId, long startTime, String cacheName, @Nullable String groupName, boolean userCache);

    /**
     * @param nodeId Node id.
     * @param igniteInstanceName Ignite instance name.
     * @param igniteVersion Ignite version.
     * @param startTime Profiling start time.
     */
    void profilingStart(UUID nodeId, String igniteInstanceName, String igniteVersion, long startTime);

    /** Cache operation type. */
    public enum CacheOperationType {
        /** */
        GET,

        /** */
        PUT,

        /** */
        REMOVE,

        /** */
        GET_AND_PUT,

        /** */
        GET_AND_REMOVE,

        /** */
        INVOKE,

        /** */
        LOCK,

        /** */
        GET_ALL,

        /** */
        PUT_ALL,

        /** */
        REMOVE_ALL,

        /** */
        INVOKE_ALL;

        /** */
        private static final CacheOperationType[] VALS = values();

        /** */
        public static CacheOperationType fromOrdinal(byte idx) {
            return idx < 0 || idx >= VALS.length ? null : VALS[idx];
        }
    }
}
