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

package org.apache.ignite.internal.profiling.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.profiling.ProfilingLogParser;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.profiling.ProfilingLogParser.currentPhase;
import static org.apache.ignite.internal.profiling.util.Utils.MAPPER;

/**
 * Builds JSON with aggregated query statistics.
 *
 * Example:
 * <pre>
 * {
 *      $textOrCacheName : {
 *          "count" : $executionsCount,
 *          "duration" : $duration,
 *          "logicalReads" : $logicalReads,
 *          "physicalReads" : $physicalReads,
 *          "failures" : $failures
 *      }
 * }
 * </pre>
 * Example of slowest queries:
 * <pre>
 * [
 *  {
 *      "text" : $textOrCacheName,
 *      "startTime" : $startTime,
 *      "duration" : $duration,
 *      "nodeId" : $nodeId,
 *      "logicalReads" : $logicalReads,
 *      "physicalReads" : $physicalReads,
 *      "success" : $success
 *  }
 * ]
 * </pre>
 *
 * @see TopSlowQueryHelper
 */
public class QueryParser implements IgniteLogParser {
    /** SQL queries results: queryText -> aggregatedInfo. */
    private final Map<String, AggregatedQueryInfo> sqlRes = new HashMap<>();

    /** SCAN queries results: cacheName -> aggregatedInfo. */
    private final Map<String, AggregatedQueryInfo> scanRes = new HashMap<>();

    /** Parsed reads that have not mapped to queries yet: queryNodeId -> queryId -> reads. */
    private final Map<UUID, Map<Long, long[]>> unmergedIds = new HashMap<>();

    /** Top of slowest queries. */
    private final TopSlowQueryHelper slowQueries = new TopSlowQueryHelper();

    /** {@inheritDoc} */
    @Override public void query(GridCacheQueryType type, String text, UUID queryNodeId, long id, long startTime,
        long duration, boolean success) {
        if (currentPhase() == ProfilingLogParser.ParsePhase.REDUCE) {
            slowQueries.mergeQuery(type, text, queryNodeId, id, startTime, duration, success);

            return;
        }

        slowQueries.value(type, duration, queryNodeId, id);

        Map<String, AggregatedQueryInfo> res;

        if (type == GridCacheQueryType.SQL_FIELDS)
            res = sqlRes;
        else if (type == GridCacheQueryType.SCAN)
            res = scanRes;
        else
            return;

        AggregatedQueryInfo info = res.computeIfAbsent(text, k -> new AggregatedQueryInfo());

        info.merge(queryNodeId, id, duration, success);

        // Try merge unmerged ids.
        Map<Long, long[]> nodeIds = unmergedIds.get(queryNodeId);

        if (nodeIds != null) {
            long[] reads = nodeIds.get(id);

            if (reads != null) {
                info.addReads(reads[0], reads[1]);

                nodeIds.remove(id);

                if (nodeIds.isEmpty())
                    unmergedIds.remove(queryNodeId);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads,
        long physicalReads) {
        if (currentPhase() == ProfilingLogParser.ParsePhase.REDUCE) {
            slowQueries.mergeReads(type, queryNodeId, id, logicalReads, physicalReads);

            return;
        }

        Map<String, AggregatedQueryInfo> res;

        if (type == GridCacheQueryType.SQL_FIELDS)
            res = sqlRes;
        else if (type == GridCacheQueryType.SCAN)
            res = scanRes;
        else
            return;

        for (AggregatedQueryInfo info : res.values()) {
            Set<Long> ids = info.ids.get(queryNodeId);

            if (ids != null && ids.contains(id)) {
                info.addReads(logicalReads, physicalReads);

                // Merged.
                return;
            }
        }

        Map<Long, long[]> ids = unmergedIds.computeIfAbsent(queryNodeId, k -> new HashMap<>());

        long[] readsArr = ids.computeIfAbsent(id, k -> new long[] {0, 0});

        readsArr[0] += logicalReads;
        readsArr[1] += physicalReads;
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode sqlRes = MAPPER.createObjectNode();
        ObjectNode scanRes = MAPPER.createObjectNode();

        buildResult(this.sqlRes, sqlRes);
        buildResult(this.scanRes, scanRes);

        Map<String, JsonNode> res = U.map("sql", sqlRes, "scan", scanRes);

        res.putAll(slowQueries.results());

        return res;
    }

    /** Builds JSON. */
    private void buildResult(Map<String, AggregatedQueryInfo> res, ObjectNode jsonRes) {
        res.forEach((text, info) -> {
            ObjectNode sql = (ObjectNode)jsonRes.get(text);

            if (sql == null) {
                sql = MAPPER.createObjectNode();

                sql.put("count", info.count);
                sql.put("duration", info.totalDuration);
                sql.put("logicalReads", info.logicalReads);
                sql.put("physicalReads", info.physicalReads);
                sql.put("failures", info.failures);

                jsonRes.set(text, sql);
            }
        });
    }

    /** Aggregated query info. */
    private static class AggregatedQueryInfo {
        /** Executions count. */
        int count;

        /** Total duration. */
        long totalDuration;

        /** Number of logical reads. */
        long logicalReads;

        /** Number of physical reads. */
        long physicalReads;

        /** Failures count. */
        int failures;

        /** Query ids. Parsed from global query id: NodeId -> queryIds */
        final Map<UUID, Set<Long>> ids = new HashMap<>();

        /** */
        public void addReads(long logicalReads, long physicalReads) {
            this.logicalReads += logicalReads;
            this.physicalReads += physicalReads;
        }

        /** */
        public void merge(UUID queryNodeId, long id, long duration, boolean success) {
            count += 1;
            totalDuration += duration;

            if (!success)
                failures += 1;

            ids.computeIfAbsent(queryNodeId, k -> new HashSet<>())
                .add(id);
        }
    }
}
