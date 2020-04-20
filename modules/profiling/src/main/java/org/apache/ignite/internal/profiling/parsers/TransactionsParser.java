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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.profiling.util.Utils.createArrayIfAbsent;
import static org.apache.ignite.internal.profiling.util.Utils.createObjectIfAbsent;

/** */
public class TransactionsParser implements IgniteLogParser {
    /** Histogram buckets for duration get, put, remove, commit, rollback operations in milliseconds. */
    public static final long[] HISTOGRAM_BUCKETS = new long[] {1, 10, 100, 250,  1000};

    /** nodeId->cacheId->opType->aggregatedResults */
    private final Map<String, Map<String, Map<String, Map<Long, Integer>>>> res = new HashMap<>();

    /** Transaction durations histogram data. nodeId->cacheId->histogram */
    private final Map<String, Map<String, HistogramMetricImpl>> histogram = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void parse(String nodeId, String str) {
        if (!str.startsWith("tx"))
            return;

        Transaction tx = Transaction.fromString(str);

        String op = tx.commit ? "commit" : "rollback";

        String[] cacheIds = tx.cacheIds.split(",");
        // Aggragate total by cache and by nodes by cache.
        cacheIds = Arrays.copyOf(cacheIds, cacheIds.length + 1);
        cacheIds[cacheIds.length - 1] = "";

        String[] nodes = new String[] {"", nodeId};

        long aggrTime = tx.startTime / 1000 * 1000;

        for (String node : nodes) {
            for (String cache : cacheIds) {
                res.computeIfAbsent(node, s -> new HashMap<>())
                    .computeIfAbsent(cache, s -> new HashMap<>())
                    .computeIfAbsent(op, s -> new HashMap<>())
                    .compute(aggrTime, (time, count) -> count == null ? 1 : count + 1);

                histogram.computeIfAbsent(node, s -> new HashMap<>())
                    .computeIfAbsent(cache, s -> new HistogramMetricImpl("", null, HISTOGRAM_BUCKETS))
                    .value(U.nanosToMillis(tx.duration));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode jsonRes = resultsToJson();
        ObjectNode histogram = histogramToJson();

        ArrayNode buckets = mapper.createArrayNode();

        Arrays.stream(HISTOGRAM_BUCKETS).forEach(buckets::add);

        return U.map("tx", jsonRes, "txHistogram", histogram, "txHistogramBuckets", buckets);
    }

    /** */
    private ObjectNode resultsToJson() {
        ObjectNode json = mapper.createObjectNode();

        res.forEach((nodeId, cachesMap) -> {
            ObjectNode nodesInfo = createObjectIfAbsent(nodeId, json);

            cachesMap.forEach((cacheId, opsMap) -> {
                ObjectNode cachesInfo = createObjectIfAbsent(cacheId, nodesInfo);

                opsMap.forEach((opType, timingMap) -> {
                    ArrayNode op = createArrayIfAbsent(opType, cachesInfo);

                    timingMap.forEach((time, count) -> {
                        ArrayNode arr = mapper.createArrayNode();

                        arr.add(time);
                        arr.add(count);

                        op.add(arr);
                    });
                });
            });
        });

        return json;
    }

    /** */
    private ObjectNode histogramToJson() {
        ObjectNode json = mapper.createObjectNode();

        histogram.forEach((nodeId, map) -> {
            ObjectNode nodesInfo = createObjectIfAbsent(nodeId, json);

            map.forEach((cacheId, metric) -> {
                ArrayNode values = createArrayIfAbsent(cacheId, nodesInfo);

                Arrays.stream(metric.value()).forEach(values::add);
            });
        });

        return json;
    }

    /** */
    static class Transaction {
        String cacheIds;
        long startTime;
        long duration;
        boolean commit;

        static Transaction fromString(String str) {
            /*tx [cacheIds=[-531344302], startTime=1586860023059, duration=169901176, commit=true]*/
            Transaction res = new Transaction();

            res.commit = str.charAt(str.length() - 3) == 'u';

            int idx = str.indexOf('=');
            int idx2 = str.indexOf(']', idx);
            res.cacheIds = str.substring(idx + 2, idx2);

            idx = str.indexOf('=', idx2);
            idx2 = str.indexOf(',', idx);
            res.startTime = Long.parseLong(str.substring(idx + 1, idx2));

            idx = str.indexOf('=', idx2);
            idx2 = str.indexOf(',', idx);
            res.duration = Long.parseLong(str.substring(idx + 1, idx2));

            return res;
        }
    }
}
