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
import org.apache.ignite.internal.profiling.parsers.QueryParser.Query;
import org.apache.ignite.internal.profiling.parsers.QueryParser.QueryReads;
import org.apache.ignite.internal.profiling.util.OrderedFixedSizeStructure;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.profiling.util.Utils.MAPPER;

/**
 * Build JSON of slowest queries.
 *
 * @see QueryParser
 */
public class TopSlowQueryHelper {
    /** Tree to store top of slow SQL queries: duration -> query. */
    private final OrderedFixedSizeStructure<Long, Query> topSql = new OrderedFixedSizeStructure<>();

    /** Tree to store top of slow SCAN queries: duration -> query. */
    private final OrderedFixedSizeStructure<Long, Query> topScan = new OrderedFixedSizeStructure<>();

    /** Result map with aggregated reads: queryNodeId -> queryId -> query & reads. */
    private final Map<String, Map<Integer, T2<Query, long[]>>> sqlRes = new HashMap<>();

    /** Result map with aggregated reads: queryNodeId -> queryId -> query & reads. */
    private final Map<String, Map<Integer, T2<Query, long[]>>> scanRes = new HashMap<>();

    /**
     * Put value.
     *
     * @param nodeId Node id.
     * @param query Query.
     */
    public void value(String nodeId, Query query) {
        OrderedFixedSizeStructure<Long, Query> map = query.isSql ? topSql : topScan;

        map.put(query.duration, query);
    }

    /** Callback on all logs parsed at first iteration. */
    public void onFirstPhaseEnd() {
        prepareResult(topSql, sqlRes);
        prepareResult(topScan, scanRes);
    }

    /**
     * Adds reads to slowest queries.
     *
     * @param reads Query reads.
     */
    public void mergeReads(QueryReads reads) {
        Map<String, Map<Integer, T2<Query, long[]>>> map = reads.isSql ? sqlRes : scanRes;

        Map<Integer, T2<Query, long[]>> ids = map.get(reads.nodeId);

        if (ids == null)
            return;

        ids.computeIfPresent(reads.id, (k, t2) -> {
            long[] queryReads = t2.get2();

            queryReads[0] += reads.logicalReads;
            queryReads[1] += reads.physicalReads;

            return t2;
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

        buildResultJson(this.sqlRes, sqlRes);
        buildResultJson(this.scanRes, scanRes);

        return U.map("topSlowSql", sqlRes, "topSlowScan", scanRes);
    }

    /** Prepares results to start to find reads. */
    private void prepareResult(OrderedFixedSizeStructure<Long, Query> top,
        Map<String, Map<Integer, T2<Query, long[]>>> res) {
        top.values().forEach(query -> {
            res.computeIfAbsent(query.queryNodeId, k -> new HashMap<>())
                .computeIfAbsent(query.id, k -> new T2<>(query, new long[] {0, 0}));
        });
    }

    /** Builds JSON. */
    private void buildResultJson(Map<String, Map<Integer, T2<Query, long[]>>> res, ArrayNode json) {
        res.forEach((originNodeId, qrs) -> {
            for (T2<Query, long[]> t3 : qrs.values()) {
                Query query = t3.get1();
                long[] reads = t3.get2();

                ObjectNode node = MAPPER.createObjectNode();

                node.put("text", query.text);
                node.put("startTime", query.startTime);
                node.put("duration", query.duration);
                node.put("nodeId", query.nodeId);
                node.put("logicalReads", reads[0]);
                node.put("physicalReads", reads[1]);
                node.put("success", query.success);

                json.add(node);
            }
        });
    }
}
