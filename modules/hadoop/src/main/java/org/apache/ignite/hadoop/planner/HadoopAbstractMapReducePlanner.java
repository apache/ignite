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

package org.apache.ignite.hadoop.planner;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.hadoop.HadoopMapReducePlanner;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopMapReducePlanGroup;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopMapReducePlanTopology;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Base class for map-reduce planners.
 */
public abstract class HadoopAbstractMapReducePlanner implements HadoopMapReducePlanner {
    /** Injected grid. */
    @IgniteInstanceResource
    protected Ignite ignite;

    /** Logger. */
    @SuppressWarnings("UnusedDeclaration")
    @LoggerResource
    protected IgniteLogger log;

    /**
     * Create plan topology.
     *
     * @param nodes Topology nodes.
     * @return Plan topology.
     */
    protected static HadoopMapReducePlanTopology topology(Collection<ClusterNode> nodes) {
        Map<String, HadoopMapReducePlanGroup> macsMap = new HashMap<>(nodes.size());

        Map<UUID, HadoopMapReducePlanGroup> idToGrp = new HashMap<>(nodes.size());
        Map<String, HadoopMapReducePlanGroup> hostToGrp = new HashMap<>(nodes.size());

        for (ClusterNode node : nodes) {
            String macs = node.attribute(ATTR_MACS);

            HadoopMapReducePlanGroup grp = macsMap.get(macs);

            if (grp == null) {
                grp = new HadoopMapReducePlanGroup(node, macs);

                macsMap.put(macs, grp);
            }
            else
                grp.add(node);

            idToGrp.put(node.id(), grp);

            for (String host : node.addresses()) {
                HadoopMapReducePlanGroup hostGrp = hostToGrp.get(host);

                if (hostGrp == null)
                    hostToGrp.put(host, grp);
                else
                    assert hostGrp == grp;
            }
        }

        return new HadoopMapReducePlanTopology(new ArrayList<>(macsMap.values()), idToGrp, hostToGrp);
    }


    /**
     * Groups nodes by host names.
     *
     * @param top Topology to group.
     * @return Map.
     */
    protected static Map<String, Collection<UUID>> groupByHost(Collection<ClusterNode> top) {
        Map<String, Collection<UUID>> grouped = U.newHashMap(top.size());

        for (ClusterNode node : top) {
            for (String host : node.hostNames()) {
                Collection<UUID> nodeIds = grouped.get(host);

                if (nodeIds == null) {
                    // Expecting 1-2 nodes per host.
                    nodeIds = new ArrayList<>(2);

                    grouped.put(host, nodeIds);
                }

                nodeIds.add(node.id());
            }
        }

        return grouped;
    }
}
