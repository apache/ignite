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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.profiling.ProfilingLogParser.currentNodeId;
import static org.apache.ignite.internal.profiling.util.Utils.MAPPER;

/**
 * Builds JSON with topology information.
 *
 * Example:
 * <pre>
 * {
 *      "profilingStartTime" : $startTime,
 *      "profilingFinishTime" : $finishTime,
 *      "nodesInfo" : { $nodeId : {"name": $name, "verson": verson, "startTime" : $startTime} }
 * }
 * </pre>
 */
public class TopologyInfoParser implements IgniteLogParser {
    /** Result JSON. */
    private final ObjectNode res = MAPPER.createObjectNode();

    /** Result JSON. */
    private final ObjectNode nodesInfo = MAPPER.createObjectNode();

    /** */
    public TopologyInfoParser() {
        res.put("profilingStartTime", 0);
        res.put("profilingFinishTime", 0);
        res.set("nodesInfo", nodesInfo);
    }

    /** {@inheritDoc} */
    @Override public void profilingStart(UUID nodeId, String igniteInstanceName, String igniteVersion, long startTime) {
        res.put("profilingStartTime", Math.min(startTime, res.get("profilingStartTime").longValue()));
        res.put("profilingFinishTime", Math.max(startTime, res.get("profilingFinishTime").longValue()));

        if (!currentNodeId().equals(nodeId))
            throw new RuntimeException("Unknown node id found [nodeId=" + nodeId + ']');

        if (nodesInfo.get(nodeId.toString()) != null)
            throw new RuntimeException("Duplicate node id found [nodeId=" + nodeId + ']');

        ObjectNode node = MAPPER.createObjectNode();

        node.put("name", igniteInstanceName);
        node.put("verson", igniteVersion);
        node.put("startTime", startTime);

        nodesInfo.set(nodeId.toString(), node);
    }

    /** {@inheritDoc} */
    @Override public Map<String, JsonNode> results() {
        return U.map("topology", res);
    }
}
