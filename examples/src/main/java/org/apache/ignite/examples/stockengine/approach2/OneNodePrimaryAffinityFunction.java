package org.apache.ignite.examples.stockengine.approach2;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class OneNodePrimaryAffinityFunction extends RendezvousAffinityFunction {
    @Override public List<ClusterNode> assignPartition(
            int part, List<ClusterNode> nodes, int backups, @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache
    ) {
        //We have guarantee that nodes is always the same on all nodes.
        //Look at org.apache.ignite.cache.affinity.AffinityFunctionContext#currentTopologySnapshot
        return nodes;
    }
}
