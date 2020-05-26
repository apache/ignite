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

package org.apache.ignite.internal.profiling.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.profiling.ProfilingFilesParser.currentNodeId;
import static org.apache.ignite.internal.profiling.util.Utils.MAPPER;
import static org.apache.ignite.internal.profiling.util.Utils.createArrayIfAbsent;
import static org.apache.ignite.internal.profiling.util.Utils.createObjectIfAbsent;

/**
 * Builds JSON with aggregated cache operations statistics.
 *
 * Example:
 * <pre>
 * {
 *    $nodeId : {
 *       $cacheId : {
 *          $opType : [ [ $startTime, $count] ]
 *       }
 *    }
 * }
 * </pre>
 */
public class CacheOperationsHandler implements IgniteProfilingHandler {
    /** Field name of aggregated by caches/nodes values. */
    private static final String TOTAL = "total";

    /** Cache operations statistics: nodeId->cacheId->opType->aggregatedResults. */
    private final Map<UUID, Map<Integer, Map<String, Map<Long, Integer>>>> res = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void cacheOperation(CacheOperationType type, int cacheId, long startTime, long duration) {
        // nodeId=null means aggregate by all nodes.
        UUID[] nodesId = new UUID[] {null, currentNodeId()};

        // cacheId=0 means aggregate by all caches.
        int[] cacheIds = new int[] {0, cacheId};

        // Aggregate by seconds.
        long aggrTime = startTime / 1000 * 1000;

        for (UUID nodeId : nodesId) {
            for (int cache : cacheIds) {
                res.computeIfAbsent(nodeId, k -> new HashMap<>())
                    .computeIfAbsent(cache, k -> new HashMap<>())
                    .computeIfAbsent(type.name(), k -> new HashMap<>())
                    .compute(aggrTime, (time, count) -> count == null ? 1 : count + 1);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ObjectNode jsonRes = MAPPER.createObjectNode();

        res.forEach((nodeId, cachesMap) -> {
            ObjectNode node = createObjectIfAbsent(nodeId == null ? TOTAL : String.valueOf(nodeId), jsonRes);

            cachesMap.forEach((cacheId, opsMap) -> {
                ObjectNode cache = createObjectIfAbsent(cacheId == 0 ? TOTAL : String.valueOf(cacheId), node);

                opsMap.forEach((opType, timingMap) -> {
                    ArrayNode op = createArrayIfAbsent(opType, cache);

                    timingMap.forEach((time, count) -> {
                        ArrayNode arr = MAPPER.createArrayNode();

                        arr.add(time);
                        arr.add(count);

                        op.add(arr);
                    });
                });
            });
        });

        return U.map("ops", jsonRes);
    }
}
