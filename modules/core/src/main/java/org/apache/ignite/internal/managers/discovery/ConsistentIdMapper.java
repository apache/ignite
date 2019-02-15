/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.managers.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
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
     * @return Compact ID of node for given baseline topology.
     */
    public short mapToCompactId(AffinityTopologyVersion topVer, UUID nodeId) {
        Map<UUID, Short> m = discoveryMgr.consistentId(topVer);

        if (m == null)
            throw new IllegalStateException("Unable to find consistent id map [topVer" + topVer + ']');

        Short constId = m.get(nodeId);

        if (constId == null)
            throw new IllegalStateException("Unable to find consistentId by UUID [nodeId=" + nodeId + ", topVer=" + topVer + ']');

        return constId;
    }

    /**
     * Maps UUID to compact ID for given baseline topology.
     *
     * @param topVer Topology version.
     * @param nodeConstId UUID of node.
     * @return Compact ID of node for given baseline topology.
     */
    public UUID mapToUuid(AffinityTopologyVersion topVer, short nodeConstId) {
        Map<Short, UUID> map = discoveryMgr.nodeIdMap(topVer);

        if (map == null)
            return null;

        UUID constId = map.get(nodeConstId);

        if (constId == null)
            throw new IllegalStateException("Unable to find UUID by constId [nodeId=" + nodeConstId + ", topVer=" + topVer + ']');

        return constId;
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

        Map<UUID, Short> m = discoveryMgr.consistentId(topVer);

        int bltNodes = m.size();

        Map<Short, Collection<Short>> consistentMap = U.newHashMap(txNodes.size());

        int nodeCnt = 0;

        for (Map.Entry<UUID, Collection<UUID>> e : txNodes.entrySet()) {
            UUID node = e.getKey();

            if (!m.containsKey(node)) // not in blt
                continue;

            Collection<UUID> backupNodes = e.getValue();

            Collection<Short> backups = new ArrayList<>(backupNodes.size());

            for (UUID backup : backupNodes) {
                if (m.containsKey(backup)) {
                    nodeCnt++;

                    backups.add(mapToCompactId(topVer, backup));
                }
            }

            // Optimization for short store full nodes set.
            if (backups.size() == nodeCnt && nodeCnt == (bltNodes - 1))
                backups = Collections.singletonList(Short.MAX_VALUE);

            consistentMap.put(mapToCompactId(topVer, node), backups);
        }

        return consistentMap;
    }
}
