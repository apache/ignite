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
import java.util.function.ToIntFunction;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.query.calcite.metadata.DistributionRegistry;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentLocation;
import org.apache.ignite.internal.processors.query.calcite.metadata.Location;
import org.apache.ignite.internal.processors.query.calcite.metadata.LocationRegistry;
import org.apache.ignite.internal.processors.query.calcite.schema.RowType;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.DestinationFunctionFactory;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class RegistryImpl implements DistributionRegistry, LocationRegistry {
    private final GridKernalContext ctx;

    public RegistryImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    @Override public DistributionTrait distribution(int cacheId, RowType rowType) {
        if (ctx.cache().context().cacheContext(cacheId).isReplicated())
            return IgniteDistributions.broadcast();

        Object key = ctx.cache().context().affinity().affinity(cacheId).similarAffinityKey();
        ToIntFunction<Object> partFun = ctx.cache().context().cacheContext(cacheId).affinity()::partition;

        return IgniteDistributions.hash(rowType.distributionKeys(), new AffinityFactory(partFun, key));
    }

    @Override public Location single(AffinityTopologyVersion topVer) {
        return new Location(Collections.singletonList(ctx.discovery().localNode()), null, (byte) 0);
    }

    @Override public Location random(AffinityTopologyVersion topVer) {
        return new Location(ctx.discovery().discoCache(topVer).serverNodes(), null, (byte) 0);
    }

    @Override public Location distributed(int cacheId, AffinityTopologyVersion topVer) {
        GridCacheContext cctx = ctx.cache().context().cacheContext(cacheId);

        return cctx.isReplicated() ? replicatedLocation(cctx, topVer) : partitionedLocation(cctx, topVer);
    }

    private Location partitionedLocation(GridCacheContext cctx, AffinityTopologyVersion topVer) {
        byte flags = Location.HAS_PARTITIONED_CACHES;

        List<List<ClusterNode>> assignments = cctx.affinity().assignments(topVer);

        if (cctx.config().getWriteSynchronizationMode() == CacheWriteSynchronizationMode.PRIMARY_SYNC) {
            List<List<ClusterNode>> assignments0 = new ArrayList<>(assignments.size());

            for (List<ClusterNode> partNodes : assignments)
                assignments0.add(F.isEmpty(partNodes) ? Collections.emptyList() : Collections.singletonList(F.first(partNodes)));

            assignments = assignments0;
        }
        else if (!cctx.topology().rebalanceFinished(topVer)) {
            flags |= Location.HAS_MOVING_PARTITIONS;

            List<List<ClusterNode>> assignments0 = new ArrayList<>(assignments.size());

            for (int part = 0; part < assignments.size(); part++) {
                List<ClusterNode> partNodes = assignments0.get(part), partNodes0 = new ArrayList<>(partNodes.size());

                for (ClusterNode partNode : partNodes) {
                    if (cctx.topology().partitionState(partNode.id(), part) == GridDhtPartitionState.OWNING)
                        partNodes0.add(partNode);
                }

                assignments0.add(partNodes0);
            }

            assignments = assignments0;
        }

        return new Location(null, assignments, flags);
    }

    private Location replicatedLocation(GridCacheContext cctx, AffinityTopologyVersion topVer) {
        byte flags = Location.HAS_REPLICATED_CACHES;

        if (cctx.config().getNodeFilter() != null)
            flags |= Location.PARTIALLY_REPLICATED;

        List<ClusterNode> nodes = cctx.discovery().discoCache(topVer).cacheGroupAffinityNodes(cctx.cacheId());

        if (!cctx.topology().rebalanceFinished(topVer)) {
            flags |= Location.PARTIALLY_REPLICATED;

            List<ClusterNode> nodes0 = new ArrayList<>(nodes.size());

            int parts = cctx.topology().partitions();

            parent:
            for (ClusterNode node : nodes) {
                for (int part = 0; part < parts; part++) {
                    if (cctx.topology().partitionState(node.id(), part) != GridDhtPartitionState.OWNING)
                        continue parent;
                }

                nodes0.add(node);
            }

            nodes = nodes0;
        }

        return new Location(nodes, null, flags);
    }

    private static class AffinityFactory implements DestinationFunctionFactory {
        private final ToIntFunction<Object> partFun;
        private final Object key;

        AffinityFactory(ToIntFunction<Object> partFun, Object key) {
            this.partFun = partFun;
            this.key = key;
        }

        @Override public DestinationFunction create(FragmentLocation targetLocation, ImmutableIntList keys) {
            assert keys.size() == 1 && targetLocation.location != null;

            return create(targetLocation.location, partFun, keys.getInt(0));
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            return key.equals(((AffinityFactory) o).key);
        }

        @Override public int hashCode() {
            return key.hashCode();
        }

        private static DestinationFunction create(Location location, ToIntFunction<Object> partFun, int affField) {
            return row -> location.nodes(partFun.applyAsInt(((Object[]) row)[affField]));
        }
    }
}
