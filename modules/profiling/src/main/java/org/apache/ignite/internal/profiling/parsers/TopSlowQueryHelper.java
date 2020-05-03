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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.profiling.util.OrderedFixedSizeStructure;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.profiling.ProfilingLogParser.currentNodeId;
import static org.apache.ignite.internal.profiling.util.Utils.MAPPER;

/**
 * Build JSON of slowest queries.
 *
 * @see QueryParser
 */
public class TopSlowQueryHelper {
    /** Tree to store top of slow SQL queries: duration -> globalQueryId. */
    private final OrderedFixedSizeStructure<Long, T2<UUID, Long>> topSql = new OrderedFixedSizeStructure<>();

    /** Tree to store top of slow SCAN queries: duration -> globalQueryId. */
    private final OrderedFixedSizeStructure<Long, T2<UUID, Long>> topScan = new OrderedFixedSizeStructure<>();

    /** Result map with aggregated reads: queryNodeId -> queryId -> query & reads. */
    private final Map<UUID, Map<Long, ObjectNode>> sqlRes = new HashMap<>();

    /** Result map with aggregated reads: queryNodeId -> queryId -> query & reads. */
    private final Map<UUID, Map<Long, ObjectNode>> scanRes = new HashMap<>();

    /**
     * Put value.
     */
    public void value(GridCacheQueryType type, long duration, UUID queryNodeId, long id) {
        OrderedFixedSizeStructure<Long, T2<UUID, Long>> map;

        if (type == GridCacheQueryType.SQL_FIELDS)
            map = topSql;
        else if (type == GridCacheQueryType.SCAN)
            map = topScan;
        else
            return;

        map.put(duration, new T2<>(queryNodeId, id));
    }

    /** Callback on all logs parsed at first iteration. */
    public void onFirstPhaseEnd() {
        prepareResult(topSql, sqlRes);
        prepareResult(topScan, scanRes);
    }

    /** */
    public void mergeQuery(GridCacheQueryType type, String text, UUID queryNodeId, long id, long startTime,
        long duration, boolean success) {
        Map<UUID, Map<Long, ObjectNode>> map;

        if (type == GridCacheQueryType.SQL_FIELDS)
            map = sqlRes;
        else if (type == GridCacheQueryType.SCAN)
            map = scanRes;
        else
            return;

        Map<Long, ObjectNode> ids = map.get(queryNodeId);

        if (ids == null)
            return;

        ids.computeIfPresent(id, (k, json) -> {
            json.put("text", text);
            json.put("startTime", startTime);
            json.put("duration", duration);
            json.put("nodeId", currentNodeId().toString());
            json.put("success", success);

            return json;
        });
    }

    /**
     * Adds reads to slowest queries.
     */
    public void mergeReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads, long physicalReads) {
        Map<UUID, Map<Long, ObjectNode>> map;

        if (type == GridCacheQueryType.SQL_FIELDS)
            map = sqlRes;
        else if (type == GridCacheQueryType.SCAN)
            map = scanRes;
        else
            return;

        Map<Long, ObjectNode> ids = map.get(queryNodeId);

        if (ids == null)
            return;

        ids.computeIfPresent(id, (k, json) -> {
            json.put("logicalReads", json.get("logicalReads").asLong() + logicalReads);
            json.put("physicalReads", json.get("physicalReads").asLong() + physicalReads);

            return json;
        });
    }

    /**
     * Map of named JSON results.
     *
     * @return Result map.
     */
    public Map<String, JsonNode> results() {
        ArrayNode sqlRes = MAPPER.createArrayNode();
        ArrayNode scanRes = MAPPER.createArrayNode();

        this.sqlRes.values().forEach(map -> map.values().forEach(sqlRes::add));
        this.scanRes.values().forEach(map -> map.values().forEach(scanRes::add));

        return U.map("topSlowSql", sqlRes, "topSlowScan", scanRes);
    }

    /** Prepares results to start to find reads. */
    private void prepareResult(OrderedFixedSizeStructure<Long, T2<UUID, Long>> top,
        Map<UUID, Map<Long, ObjectNode>> res) {
        top.values().forEach(t2 -> {
            res.computeIfAbsent(t2.getKey(), k -> new HashMap<>())
                .computeIfAbsent(t2.getValue(), k -> {
                    ObjectNode node = MAPPER.createObjectNode();

                    node.put("logicalReads", 0);
                    node.put("physicalReads", 0);

                    return node;
                });
        });
    }
}
