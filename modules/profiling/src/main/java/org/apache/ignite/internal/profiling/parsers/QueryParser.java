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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.profiling.util.OrderedFixedSizeStructure;
import org.apache.ignite.internal.util.typedef.F;

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
 */
public class QueryParser implements IgniteLogParser {
    /**  Queries results: queryType -> queryText -> aggregatedInfo. */
    private final Map<GridCacheQueryType, Map<String, AggregatedQueryInfo>> aggrQuery =
        new EnumMap<>(GridCacheQueryType.class);

    /** Parsed reads: queryType -> queryNodeId -> queryId -> reads. */
    private final Map<GridCacheQueryType, Map<UUID, Map<Long, long[]>>> readsById =
        new EnumMap<>(GridCacheQueryType.class);

    /** Structure to store top of slow SQL queries: queryType -> duration -> query. */
    private final Map<GridCacheQueryType, OrderedFixedSizeStructure<Long, Query>> topSlow =
        new EnumMap<>(GridCacheQueryType.class);

    /** {@inheritDoc} */
    @Override public void query(GridCacheQueryType type, String text, UUID queryNodeId, long id, long startTime,
        long duration, boolean success) {
        Query query = new Query(type, text, queryNodeId, id, startTime, duration, success);

        OrderedFixedSizeStructure<Long, Query> tree = topSlow.computeIfAbsent(type,
            t -> new OrderedFixedSizeStructure<>());

        tree.put(duration, query);

        AggregatedQueryInfo info = aggrQuery.computeIfAbsent(type, t -> new HashMap<>())
            .computeIfAbsent(text, k -> new AggregatedQueryInfo());

        info.merge(queryNodeId, id, duration, success);
    }

    /** {@inheritDoc} */
    @Override public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads,
        long physicalReads) {

        Map<Long, long[]> ids = readsById.computeIfAbsent(type, t -> new HashMap<>())
            .computeIfAbsent(queryNodeId, k -> new HashMap<>());

        long[] readsArr = ids.computeIfAbsent(id, k -> new long[] {0, 0});

        readsArr[0] += logicalReads;
        readsArr[1] += physicalReads;
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode sqlRes = MAPPER.createObjectNode();
        ObjectNode scanRes = MAPPER.createObjectNode();

        buildResult(GridCacheQueryType.SQL_FIELDS, sqlRes);
        buildResult(GridCacheQueryType.SCAN, scanRes);

        ArrayNode topSlowSql = MAPPER.createArrayNode();
        ArrayNode topSlowScan = MAPPER.createArrayNode();

        buildTopSlowResult(GridCacheQueryType.SQL_FIELDS, topSlowSql);
        buildTopSlowResult(GridCacheQueryType.SCAN, topSlowScan);

        return F.asMap("sql", sqlRes, "scan", scanRes, "topSlowSql", topSlowSql, "topSlowScan", topSlowScan);
    }

    /** Builds JSON. */
    private void buildResult(GridCacheQueryType type, ObjectNode jsonRes) {
        if (!aggrQuery.containsKey(type))
            return;

        Map<String, AggregatedQueryInfo> res = aggrQuery.get(type);

        res.forEach((text, info) -> {
            info.ids.forEach((uuid, ids) -> {
                if (!readsById.containsKey(type) || !readsById.get(type).containsKey(uuid))
                    return;

                Map<Long, long[]> reads = readsById.get(type).get(uuid);

                ids.forEach(id -> {
                    long[] readsArr = reads.get(id);

                    if (readsArr == null)
                        return;

                    info.logicalReads += readsArr[0];
                    info.physicalReads += readsArr[1];
                });
            });

            ObjectNode sql = (ObjectNode)jsonRes.get(text);

            if (sql == null) {
                sql = MAPPER.createObjectNode();

                sql.put("count", info.count);
                sql.put("duration", TimeUnit.NANOSECONDS.toMillis(info.totalDuration));
                sql.put("logicalReads", info.logicalReads);
                sql.put("physicalReads", info.physicalReads);
                sql.put("failures", info.failures);

                jsonRes.set(text, sql);
            }
        });
    }

    /** Builds JSON. */
    private void buildTopSlowResult(GridCacheQueryType type, ArrayNode jsonRes) {
        if (!topSlow.containsKey(type))
            return;

        OrderedFixedSizeStructure<Long, Query> tree = topSlow.get(type);

        tree.values().forEach(query -> {
            ObjectNode json = MAPPER.createObjectNode();

            json.put("text", query.text);
            json.put("startTime", query.startTime);
            json.put("duration", TimeUnit.NANOSECONDS.toMillis(query.duration));
            json.put("nodeId", String.valueOf(query.queryNodeId));
            json.put("success", query.success);
            json.put("logicalReads", 0);
            json.put("physicalReads", 0);

            jsonRes.add(json);

            if (!readsById.containsKey(type) || !readsById.get(type).containsKey(query.queryNodeId))
                return;

            long[] readsArr = readsById.get(type).get(query.queryNodeId).get(query.id);

            if (readsArr == null)
                return;

            json.put("logicalReads", readsArr[0]);
            json.put("physicalReads", readsArr[1]);
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

    /** Query. */
    public class Query {
        /** Cache query type. */
        final GridCacheQueryType type;

        /** Query text in case of SQL query. Cache name in case of SCAN query. */
        final String text;

        /** Originating node id (as part of global query id). */
        final UUID queryNodeId;

        /** Query id. */
        final long id;

        /** Start time. */
        final long startTime;

        /** Duration. */
        final long duration;

        /** Success flag. */
        final boolean success;

        /**
         * @param type Cache query type.
         * @param text Query text in case of SQL query. Cache name in case of SCAN query.
         * @param queryNodeId Originating node id.
         * @param id Query id.
         * @param startTime Start time.
         * @param duration Duration.
         * @param success Success flag.
         */
        public Query(GridCacheQueryType type, String text, UUID queryNodeId, long id, long startTime, long duration,
            boolean success) {
            this.type = type;
            this.text = text;
            this.queryNodeId = queryNodeId;
            this.id = id;
            this.startTime = startTime;
            this.duration = duration;
            this.success = success;
        }
    }
}
