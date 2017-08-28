package org.apache.ignite.internal.processors.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.services.ServiceTopology;

/**
 * Use this class to create {@link ServiceTopology} implementations
 */
public class GridServiceTopologyFactory {
    /**
     * @param node Node in the topology
     * @param cnt Number of service instances deployed on the node
     * @return a {@link ServiceTopology} instance of the type most appropriate for the specified parameters.
     */
    public static ServiceTopology get(ClusterNode node, int cnt) {
        A.notNull(node, "node");

        return new SingleNodeServiceTopology(node.id(), cnt);
    }

    /**
     * @param nodes Nodes in the topology
     * @param cnt Number of service instances deployed on each node
     * @return a {@link ServiceTopology} instance of the type most appropriate for the specified parameters.
     */
    public static ServiceTopology get(Iterable<ClusterNode> nodes, int cnt) {
        A.notNull(nodes, "nodes");

        if (!nodes.iterator().hasNext())
            return new EmptyServiceTopology();

        Collection<UUID> nodeIds = new ArrayList<>();

        for (ClusterNode n : nodes)
            nodeIds.add(n.id());

        return nodeIds.size() > 1 ?
            new HomomorphicServiceTopology(nodeIds, cnt) :
            new SingleNodeServiceTopology(nodeIds.iterator().next(), cnt);
    }

    /**
     * @param nodeCntMap Node ID -> number of service instances map. Attention: the method will remove zero assignments
     * from this map!
     * @return a {@link ServiceTopology} instance of the type most appropriate for the specified parameters.
     */
    public static ServiceTopology get(Map<UUID, Integer> nodeCntMap) {
        A.notNull(nodeCntMap, "nodeCntMap");

        int prevCnt = 0;
        boolean allSame = true;

        Iterator<Map.Entry<UUID, Integer>> it = nodeCntMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<UUID, Integer> e = it.next();
            Integer cnt = e.getValue();
            if (cnt == null || cnt == 0)
                it.remove();
            else {
                if (prevCnt != 0 && prevCnt != cnt)
                    allSame = false;

                prevCnt = cnt;
            }
        }

        if (nodeCntMap.size() == 0)
            return new EmptyServiceTopology();

        if (nodeCntMap.size() > 1)
            return allSame ?
                new HomomorphicServiceTopology(nodeCntMap.keySet(), prevCnt) :
                new PolymorphicServiceTopology(nodeCntMap);

        Map.Entry<UUID, Integer> e = nodeCntMap.entrySet().iterator().next();

        return new SingleNodeServiceTopology(e.getKey(), e.getValue());
    }
}
