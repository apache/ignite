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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Class is needed for map UUID to consistent id and vice versa.
 */
public class ConsistentIdMapper {
    /** Discovery manager. */
    private final GridDiscoveryManager discoveryMgr;

    /**
     * Create an instance of mapper.
     *
     * @param discoveryMgr Discovery manager.
     */
    public ConsistentIdMapper(GridDiscoveryManager discoveryMgr) {
        this.discoveryMgr = discoveryMgr;
    }

    /**
     * Maps UUID to compact ID for given baseline topology.
     *
     * @param topVer Topology version.
     * @param nodeId UUID of node.
     * @param baselineTop Baseline topology.
     * @return Compact ID of node for given baseline topology.
     */
    public short mapToCompactId(AffinityTopologyVersion topVer, UUID nodeId, BaselineTopology baselineTop) {
        ClusterNode node = discoveryMgr.node(topVer, nodeId);

        if (node == null)
            throw new IllegalStateException("Unable to find node by UUID [nodeId=" + nodeId + ", topVer=" + topVer + ']');

        return baselineTop.consistentIdMapping().get(node.consistentId());
    }

    /**
     * Map primary -> backup node compact ID accordingly to baseline topology..
     *
     * @param txNodes Primary -> backup UUID nodes.
     * @return Primary -> backup compact ID nodes.
     */
    public Map<Short, Collection<Short>> mapToCompactIds(
        AffinityTopologyVersion topVer,
        @Nullable Map<UUID, Collection<UUID>> txNodes,
        BaselineTopology baselineTop
    ) {
        if (txNodes == null)
            return null;

        Map<Short, Collection<Short>> consistentMap = U.newHashMap(txNodes.size());

        for (Map.Entry<UUID, Collection<UUID>> e : txNodes.entrySet()) {
            UUID node = e.getKey();

            Collection<UUID> backupNodes = e.getValue();

            Collection<Short> backups = new ArrayList<>(backupNodes.size());

            for (UUID backup : backupNodes)
                backups.add(mapToCompactId(topVer, backup, baselineTop));

            consistentMap.put(mapToCompactId(topVer, node, baselineTop), backups);
        }

        return consistentMap;
    }
}
