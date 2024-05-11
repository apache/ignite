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

package org.apache.ignite.internal.performancestatistics.handlers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.performancestatistics.util.Utils.MAPPER;

/**
 * Builds JSON with nodes and started caches information.
 *
 * Example:
 * <pre>
 * {
 *      "nodes" : [
 *          {
 *              "id" : $nodeId
 *          }
 *      ],
 *      "caches": [
 *          {
 *              "id" : $cacheId
 *              "name" : $name
 *          }
 *      ]
 * }
 * </pre>
 */
public class ClusterInfoHandler implements IgnitePerformanceStatisticsHandler {
    /** Result JSON. */
    private final ObjectNode res = MAPPER.createObjectNode();

    /** */
    private final Set<UUID> nodesIds = new HashSet<>();

    /** */
    private final Map<Integer, String> cachesIds = new HashMap<>();

    /** {@inheritDoc} */
    @Override public void cacheStart(UUID nodeId, int cacheId, String name) {
        nodesIds.add(nodeId);

        cachesIds.put(cacheId, name);
    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long startTime, long duration) {
        nodesIds.add(nodeId);

        cachesIds.putIfAbsent(cacheId, null);
    }

    /** {@inheritDoc} */
    @Override public void transaction(UUID nodeId, GridIntList cacheIds, long startTime, long duration,
        boolean commited) {
        nodesIds.add(nodeId);

        GridIntIterator iter = cacheIds.iterator();

        while (iter.hasNext())
            cachesIds.putIfAbsent(iter.next(), null);
    }

    /** {@inheritDoc} */
    @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long startTime,
        long duration, boolean success) {
        nodesIds.add(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
        long logicalReads, long physicalReads) {
        nodesIds.add(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void task(UUID nodeId, IgniteUuid sesId, String taskName, long startTime, long duration,
        int affPartId) {
        nodesIds.add(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void job(UUID nodeId, IgniteUuid sesId, long queuedTime, long startTime, long duration,
        boolean timedOut) {
        nodesIds.add(nodeId);
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        ArrayNode nodes = MAPPER.createArrayNode();

        nodesIds.forEach(uuid -> {
            ObjectNode node = MAPPER.createObjectNode();

            node.put("id", uuid.toString());

            nodes.add(node);
        });

        ArrayNode caches = MAPPER.createArrayNode();

        cachesIds.forEach((id, name) -> {
            ObjectNode cache = MAPPER.createObjectNode();

            cache.put("id", id);
            cache.put("name", name);

            caches.add(cache);
        });

        res.set("nodes", nodes);
        res.set("caches", caches);

        return U.map("clusterInfo", res);
    }
}
