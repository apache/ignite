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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.profiling.ProfilingLogParser.currentNodeId;
import static org.apache.ignite.internal.profiling.util.Utils.MAPPER;
import static org.apache.ignite.internal.profiling.util.Utils.createArrayIfAbsent;
import static org.apache.ignite.internal.profiling.util.Utils.createObjectIfAbsent;

/**
 * Builds JSON with aggregated transaction statistics.
 *
 * Example:
 * <pre>
 * {
 *    $nodeId : {
 *       $cacheId : {
 *          $txState : [ [ $startTime, $count] ]
 *       }
 *    }
 * }
 * </pre>
 */
public class TransactionsParser implements IgniteLogParser {
    /** Field name of aggregated by caches/nodes values. */
    private static final String TOTAL = "total";

    /** Transaction durations histogram buckets in milliseconds. */
    public static final long[] HISTOGRAM_BUCKETS = new long[] {1, 10, 100, 250, 1000};

    /** Aggregated results: nodeId -> cacheId -> opType -> aggregatedResults. */
    private final Map<UUID, Map<Integer, Map<TransactionState, Map<Long, Integer>>>> res = new HashMap<>();

    /** Transaction durations histogram data: nodeId -> cacheId -> histogram. */
    private final Map<UUID, Map<Integer, HistogramMetricImpl>> histogram = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commit) {
        ArrayList<Integer> cacheIdsArr = new ArrayList<>(cacheIds.size() + 1);

        // cacheId=0 means aggregate by all caches.
        cacheIdsArr.add(0);

        GridIntIterator iter = cacheIds.iterator();

        while (iter.hasNext())
            cacheIdsArr.add(iter.next());

        // nodeId=null means aggregate by all nodes.
        UUID[] nodesId = new UUID[] {null, currentNodeId()};

        long aggrTime = startTime / 1000 * 1000;

        for (UUID nodeId : nodesId) {
            for (Integer cacheId : cacheIdsArr) {
                res.computeIfAbsent(nodeId, s -> new HashMap<>())
                    .computeIfAbsent(cacheId, s -> new EnumMap<>(TransactionState.class))
                    .computeIfAbsent(commit ? TransactionState.COMMIT : TransactionState.ROLLBACK, s -> new HashMap<>())
                    .compute(aggrTime, (time, count) -> count == null ? 1 : count + 1);

                histogram.computeIfAbsent(nodeId, s -> new HashMap<>())
                    .computeIfAbsent(cacheId, s -> new HistogramMetricImpl("", null, HISTOGRAM_BUCKETS))
                    .value(U.nanosToMillis(duration));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode jsonRes = resultsToJson();
        ObjectNode histogram = histogramToJson();

        ArrayNode buckets = MAPPER.createArrayNode();

        Arrays.stream(HISTOGRAM_BUCKETS).forEach(buckets::add);

        return U.map("tx", jsonRes, "txHistogram", histogram, "txHistogramBuckets", buckets);
    }

    /** Builds JSON. */
    private ObjectNode resultsToJson() {
        ObjectNode json = MAPPER.createObjectNode();

        res.forEach((nodeId, cachesMap) -> {
            ObjectNode nodesInfo = createObjectIfAbsent(nodeId == null ? TOTAL : String.valueOf(nodeId), json);

            cachesMap.forEach((cacheId, opsMap) -> {
                ObjectNode cachesInfo = createObjectIfAbsent(cacheId == 0 ? TOTAL : String.valueOf(cacheId), nodesInfo);

                opsMap.forEach((opType, timingMap) -> {
                    ArrayNode op = createArrayIfAbsent(opType.name().toLowerCase(), cachesInfo);

                    timingMap.forEach((time, count) -> {
                        ArrayNode arr = MAPPER.createArrayNode();

                        arr.add(time);
                        arr.add(count);

                        op.add(arr);
                    });
                });
            });
        });

        return json;
    }

    /** Builds JSON. */
    private ObjectNode histogramToJson() {
        ObjectNode json = MAPPER.createObjectNode();

        histogram.forEach((nodeId, map) -> {
            ObjectNode nodesInfo = createObjectIfAbsent(nodeId == null ? TOTAL : String.valueOf(nodeId), json);

            map.forEach((cacheId, metric) -> {
                ArrayNode values = createArrayIfAbsent(cacheId == 0 ? TOTAL : String.valueOf(cacheId), nodesInfo);

                Arrays.stream(metric.value()).forEach(values::add);
            });
        });

        return json;
    }

    /** */
    private enum TransactionState {
        /** */
        COMMIT,

        /** */
        ROLLBACK
    }
}
