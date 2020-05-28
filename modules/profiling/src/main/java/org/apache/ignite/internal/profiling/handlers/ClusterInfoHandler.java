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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.profiling.util.Utils.MAPPER;

/**
 * Builds JSON with nodes and started caches information.
 *
 * Example:
 * <pre>
 * {
 *      "profilingStartTime" : $startTime,
 *      "nodes" : {
*           $nodeId : {
 *              "name": $name,
 *              "verson": $verson,
 *              "startTime" : $startTime
 *          }
 *      }
 *      "caches": {
 *          $cacheId : {
 *              "startTime" : $startTime,
 *              "cacheName" : $cacheName,
 *              "groupName" : $groupName,
 *              "userCache" : $userCacheFlag
 *          }
 *      }
 * }
 * </pre>
 */
public class ClusterInfoHandler implements IgniteProfilingHandler {
    /** Result JSON. */
    private final ObjectNode res = MAPPER.createObjectNode();

    /** Nodes JSON object. */
    private final ObjectNode nodes = MAPPER.createObjectNode();

    /** Caches JSON object. */
    private final ObjectNode caches = MAPPER.createObjectNode();

    /** */
    public ClusterInfoHandler() {
        res.put("profilingStartTime", Long.MAX_VALUE);
        res.set("nodes", nodes);
        res.set("caches", caches);
    }

    /** {@inheritDoc} */
    @Override public void profilingStart(UUID nodeId, String igniteInstanceName, String igniteVersion, long startTime) {
        res.put("profilingStartTime", Math.min(startTime, res.get("profilingStartTime").longValue()));

        ObjectNode node = MAPPER.createObjectNode();

        node.put("name", igniteInstanceName);
        node.put("verson", igniteVersion);
        node.put("startTime", startTime);

        nodes.set(nodeId.toString(), node);
    }

    @Override public void cacheStart(int cacheId, long startTime, String cacheName, String groupName,
        boolean userCache) {
        ObjectNode node = MAPPER.createObjectNode();

        node.put("startTime", startTime);
        node.put("cacheName", cacheName);
        node.put("groupName", groupName);
        node.put("userCache", userCache);

        caches.set(String.valueOf(cacheId), node);
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        return U.map("clusterInfo", res);
    }
}
