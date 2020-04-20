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
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class QueryParser implements IgniteLogParser {
    /** SQL queries results: queryText -> aggregatedInfo. */
    private final Map<String, AggregatedQueryInfo> sqlRes = new HashMap<>();

    /** SCAN queries results: cacheName -> aggregatedInfo. */
    private final Map<String, AggregatedQueryInfo> scanRes = new HashMap<>();

    /** Parsed reads that have not mapped to queries yet: queryNodeId -> queryId -> reads. */
    private final Map<String, Map<Integer, long[]>> unmergedIds = new HashMap<>();

    /** */
    private final TopSlowQueryHelper slowQueries = new TopSlowQueryHelper();

    /** {@inheritDoc} */
    @Override public void parse(String nodeId, String str) {
        if (str.startsWith("query "))
            parseQuery(nodeId, str);
        else if (str.startsWith("queryStat"))
            parseReads(str);
    }

    /** */
    private void parseQuery(String nodeId, String str) {
        Query query = new Query(nodeId, str);

        slowQueries.value(nodeId, query);

        Map<String, AggregatedQueryInfo> res = query.isSql ? sqlRes : scanRes;

        AggregatedQueryInfo info = res.computeIfAbsent(query.text, k -> new AggregatedQueryInfo());

        info.merge(query);

        // Try merge unmerged ids.
        Map<Integer, long[]> nodeIds = unmergedIds.get(query.queryNodeId);

        if (nodeIds != null) {
            long[] reads = nodeIds.get(query.id);

            if (reads != null) {
                info.addReads(reads[0], reads[1]);

                nodeIds.remove(query.id);

                if (nodeIds.isEmpty())
                    unmergedIds.remove(query.queryNodeId);
            }
        }
    }

    /** */
    private void parseReads(String str) {
        QueryReads reads = new QueryReads(str);

        Map<String, AggregatedQueryInfo> res = reads.isSql ? sqlRes : scanRes;

        for (AggregatedQueryInfo info : res.values()) {
            Set<Integer> ids = info.ids.get(reads.nodeId);

            if (ids != null && ids.contains(reads.id)) {
                info.addReads(reads.logicalReads, reads.physicalReads);

                // Merged.
                return;
            }
        }

        Map<Integer, long[]> ids = unmergedIds.computeIfAbsent(reads.nodeId, k -> new HashMap<>());

        long[] readsArr = ids.computeIfAbsent(reads.id, k -> new long[] {0, 0});

        readsArr[0] += reads.logicalReads;
        readsArr[1] += reads.physicalReads;
    }

    /** {@inheritDoc} */
    @Override public void onFirstPhaseEnd() {
        slowQueries.onFirstPhaseEnd();
    }

    /** {@inheritDoc} */
    @Override public void parsePhase2(String nodeId, String str) {
        if (str.startsWith("queryStat")) {
            QueryReads reads = new QueryReads(str);

            slowQueries.mergeReads(reads);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode sqlRes = mapper.createObjectNode();
        ObjectNode scanRes = mapper.createObjectNode();

        buildResult(this.sqlRes, sqlRes);
        buildResult(this.scanRes, scanRes);

        Map<String, JsonNode> res = U.map("sql", sqlRes, "scan", scanRes);

        res.putAll(slowQueries.results());

        return res;
    }

    /** */
    private void buildResult(Map<String, AggregatedQueryInfo> res, ObjectNode jsonRes) {
        res.forEach((text, info) -> {
            ObjectNode sql = (ObjectNode)jsonRes.get(text);

            if (sql == null) {
                sql = mapper.createObjectNode();

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
        final Map<String, Set<Integer>> ids = new HashMap<>();

        /** */
        public void addReads(long logicalReads, long physicalReads) {
            this.logicalReads += logicalReads;
            this.physicalReads += physicalReads;
        }

        /** @param query Query to merge with. */
        public void merge(Query query) {
            count += 1;
            totalDuration += query.duration;

            if (!query.success)
                failures += 1;

            ids.computeIfAbsent(query.queryNodeId, k -> new HashSet<>())
                .add(query.id);
        }
    }

    /** */
    static class Query {
        /** Node id. */
        final String nodeId;

        /** {@code True} if query type is SQL, otherwise SCAN. */
        boolean isSql;

        /** Query text in case of SQL query. Cache name in case of SCAN query. */
        String text;

        /** Originating node id (as part of global query id). */
        String queryNodeId;

        /** Query id. */
        int id;

        /** Start time. */
        long startTime;

        /** Duration. */
        long duration;

        /** Success flag. */
        boolean success;

        /**
         * @param nodeId Node id.
         * @param str String to parse from.
         */
        Query (String nodeId, String str) {
            this.nodeId = nodeId;

            int idx = str.indexOf('=');
            int idx2 = str.indexOf('=', idx + 1);
            int textIdx = idx2 + 1;

            isSql = str.charAt(13) == 'Q';
            success = str.charAt(str.length() - 3) == 'u';

            idx = str.lastIndexOf(',');
            idx2 = str.lastIndexOf('=', idx);
            duration = U.nanosToMillis(Long.parseLong(str.substring(idx2 + 1, idx)));

            idx = str.lastIndexOf(',', idx2);
            idx2 = str.lastIndexOf('=', idx);
            startTime = Long.parseLong(str.substring(idx2 + 1, idx));

            idx = str.lastIndexOf(',', idx2);
            idx2 = str.lastIndexOf('_', idx);
            id = Integer.parseInt(str.substring(idx2 + 1, idx));

            idx = str.lastIndexOf('_', idx2);
            idx2 = str.lastIndexOf('=', idx);
            queryNodeId = str.substring(idx2 + 1, idx);

            idx = str.lastIndexOf(',', idx2);
            text = str.substring(textIdx, idx);
        }
    }

    /** Query reads. */
    static class QueryReads {
        /** {@code True} if query type is SQL, otherwise SCAN. */
        boolean isSql;

        /** Originating node id. */
        String nodeId;

        /** Query id. */
        int id;

        /** Number of logical reads. */
        int logicalReads;

        /** Number of physical reads. */
        int physicalReads;

        /** @param str String to parse from. */
        QueryReads(String str) {
            isSql = str.charAt(17) == 'Q';

            int idx = str.indexOf('=');

            idx = str.indexOf('=', idx + 1);
            int idx2 = str.indexOf('_', idx);
            nodeId = str.substring(idx + 1, idx2);

            idx = str.indexOf('_', idx2);
            idx2 = str.indexOf(',', idx);
            id = Integer.parseInt(str.substring(idx + 1, idx2));

            idx = str.indexOf('=', idx2);
            idx2 = str.indexOf(',', idx);
            logicalReads = Integer.parseInt(str.substring(idx + 1, idx2));

            idx = str.indexOf('=', idx2);
            idx2 = str.indexOf(']', idx);
            physicalReads = Integer.parseInt(str.substring(idx + 1, idx2));
        }
    }
}
