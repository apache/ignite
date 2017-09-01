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

package org.apache.ignite.internal.managers.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Class is needed for map UUID to consistent id and vice versa.
 */
public class ConsistentIdMapper {
    /** Discovery manager. */
    private final GridDiscoveryManager discoveryManager;

    /**
     * Create an instance of mapper.
     *
     * @param discoveryManager Discovery manager.
     */
    public ConsistentIdMapper(GridDiscoveryManager discoveryManager) {
        this.discoveryManager = discoveryManager;
    }

    /**
     * Map UUID to consistent id.
     *
     * @param nodeId UUID of node.
     * @return Consistent id of node.
     */
    public Object mapToConsistentId(UUID nodeId) {
        ClusterNode node = discoveryManager.node(nodeId);
        if (node == null)
            throw new IllegalStateException("Unable to find node by UUID " + nodeId);

        return node.consistentId();
    }

    /**
     * Map consistent id to UUID.
     *
     * @param consistentId Consistent id of node.
     * @return UUID of node.
     */
    @Nullable public UUID mapToUUID(Object consistentId) {
        for (ClusterNode node : discoveryManager.allNodes())
            if (node.consistentId().equals(consistentId))
                return node.id();

        return null;
    }

    /**
     * Map primary -> backup node UUIDs to consistent ids.
     *
     * @param txNodes Primary -> backup UUID nodes.
     * @return Primary -> backup consistent id nodes.
     */
    public Map<Object, Collection<Object>> mapToConsistentIds(@Nullable Map<UUID, Collection<UUID>> txNodes) {
        if (txNodes == null)
            return null;

        Map<Object, Collection<Object>> consistentMap = new HashMap<>(txNodes.keySet().size());
        for (UUID node : txNodes.keySet()) {
            Collection<UUID> backupNodes = txNodes.get(node);

            Collection<Object> consistentIdsBackups = new ArrayList<>(backupNodes.size());
            for (UUID backup : backupNodes)
                consistentIdsBackups.add(mapToConsistentId(backup));

            consistentMap.put(mapToConsistentId(node), consistentIdsBackups);
        }

        return consistentMap;
    }

}
