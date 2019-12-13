/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping.DEDUPLICATED;

/**
 *
 */
public class MappingServiceImpl implements MappingService {
    private final GridKernalContext ctx;

    public MappingServiceImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    @Override public NodesMapping local() {
        return new NodesMapping(Collections.singletonList(ctx.discovery().localNode().id()), null, DEDUPLICATED);
    }

    @Override public NodesMapping random(AffinityTopologyVersion topVer) {
        List<ClusterNode> nodes = ctx.discovery().discoCache(topVer).serverNodes();

        return new NodesMapping(Commons.transform(nodes, ClusterNode::id), null, DEDUPLICATED);
    }

    @Override public NodesMapping distributed(int cacheId, AffinityTopologyVersion topVer) {
        GridCacheContext<?,?> cctx = ctx.cache().context().cacheContext(cacheId);

        return cctx.isReplicated() ? replicatedLocation(cctx, topVer) : partitionedLocation(cctx, topVer);
    }

    private NodesMapping partitionedLocation(GridCacheContext<?,?> cctx, AffinityTopologyVersion topVer) {
        byte flags = NodesMapping.HAS_PARTITIONED_CACHES;

        List<List<ClusterNode>> assignments = cctx.affinity().assignments(topVer);
        List<List<UUID>> res;

        if (cctx.config().getWriteSynchronizationMode() == CacheWriteSynchronizationMode.PRIMARY_SYNC) {
            res = new ArrayList<>(assignments.size());

            for (List<ClusterNode> partNodes : assignments)
                res.add(F.isEmpty(partNodes) ? Collections.emptyList() : Collections.singletonList(F.first(partNodes).id()));
        }
        else if (!cctx.topology().rebalanceFinished(topVer)) {
            res = new ArrayList<>(assignments.size());

            flags |= NodesMapping.HAS_MOVING_PARTITIONS;

            for (int part = 0; part < assignments.size(); part++) {
                List<ClusterNode> partNodes = assignments.get(part);
                List<UUID> partIds = new ArrayList<>(partNodes.size());

                for (ClusterNode node : partNodes) {
                    if (cctx.topology().partitionState(node.id(), part) == GridDhtPartitionState.OWNING)
                        partIds.add(node.id());
                }

                res.add(partIds);
            }
        }
        else
            res = Commons.transform(assignments, nodes -> Commons.transform(nodes, ClusterNode::id));

        return new NodesMapping(null, res, flags);
    }

    private NodesMapping replicatedLocation(GridCacheContext<?,?> cctx, AffinityTopologyVersion topVer) {
        byte flags = NodesMapping.HAS_REPLICATED_CACHES;

        if (cctx.config().getNodeFilter() != null)
            flags |= NodesMapping.PARTIALLY_REPLICATED;

        GridDhtPartitionTopology topology = cctx.topology();

        List<ClusterNode> nodes = cctx.discovery().discoCache(topVer).cacheGroupAffinityNodes(cctx.cacheId());
        List<UUID> res;

        if (!topology.rebalanceFinished(topVer)) {
            flags |= NodesMapping.PARTIALLY_REPLICATED;

            res = new ArrayList<>(nodes.size());

            int parts = topology.partitions();

            for (ClusterNode node : nodes) {
                if (isOwner(node.id(), topology, parts))
                    res.add(node.id());
            }
        }
        else
            res = Commons.transform(nodes, ClusterNode::id);

        return new NodesMapping(res, null, flags);
    }

    private boolean isOwner(UUID nodeId, GridDhtPartitionTopology topology, int parts) {
        for (int p = 0; p < parts; p++) {
            if (topology.partitionState(nodeId, p) != GridDhtPartitionState.OWNING)
                return false;
        }
        return true;
    }
}
