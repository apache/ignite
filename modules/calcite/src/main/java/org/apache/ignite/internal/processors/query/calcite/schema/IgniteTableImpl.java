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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Ignite table implementation.
 */
public class IgniteTableImpl extends AbstractTable implements IgniteTable {
    /** */
    public static final String PK_INDEX_NAME = "PK";

    /** */
    public static final String PK_ALIAS_INDEX_NAME = "PK_ALIAS";

    /** */
    private final TableDescriptor desc;

    /** */
    private final Statistic statistic;

    /** */
    private final List<RelCollation> collations;

    /** */
    private final Map<String, IgniteIndex> indexes = new ConcurrentHashMap<>();

    /**
     *
     * @param desc Table descriptor.
     * @param collation Table collation.
     */
    public IgniteTableImpl(TableDescriptor desc, RelCollation collation) {
        this.desc = desc;
        collations = toCollations(collation);
        statistic = new StatisticsImpl();
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return desc.apply(typeFactory);
    }

    /** {@inheritDoc} */
    @Override public Statistic getStatistic() {
        return statistic;
    }


    /** {@inheritDoc} */
    @Override public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(RelOptTable.ToRelContext ctx, RelOptTable relOptTbl) {
        RelOptCluster cluster = ctx.getCluster();

        return toRel(cluster, relOptTbl, PK_INDEX_NAME);
    }

    /** {@inheritDoc} */
    @Override public IgniteTableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl, String idxName) {
        RelTraitSet traitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replace(distribution());

        IgniteIndex idx = getIndex(idxName);

        if (idx == null)
            return null;

        traitSet = traitSet.replace(idx.collation());

        return new IgniteTableScan(cluster, traitSet, relOptTbl, idxName, null);
    }

    /** {@inheritDoc} */
    @Override public NodesMapping mapping(PlanningContext ctx) {
        GridCacheContext<?, ?> cctx = desc.cacheContext();

        assert cctx != null;

        if (!cctx.gate().enterIfNotStopped())
            throw U.convertException(new CacheStoppedException(cctx.name()));

        try {
            if (cctx.isReplicated())
                return replicatedMapping(cctx, ctx.topologyVersion());

            return partitionedMapping(cctx, ctx.topologyVersion());
        }
        finally {
            cctx.gate().leave();
        }
    }


    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /** {@inheritDoc} */
    @Override public List<RelCollation> collations() {
        return collations;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Override public Map<String, IgniteIndex> indexes() {
        return indexes;
    }

    /** {@inheritDoc} */
    @Override public void addIndex(IgniteIndex idxTbl) {
        indexes.put(idxTbl.name(), idxTbl);
    }

    /** {@inheritDoc} */
    @Override public IgniteIndex getIndex(String idxName) {
        return indexes.get(idxName);
    }

    /** {@inheritDoc} */
    @Override public void removeIndex(String idxName) {
        indexes.remove(idxName);
    }

    /** {@inheritDoc} */
    @Override public Enumerable<Object[]> scan(DataContext dataCtx, List<RexNode> filters, int[] projects) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <C> C unwrap(Class<C> aCls) {
        if (aCls.isInstance(desc))
            return aCls.cast(desc);

        return super.unwrap(aCls);
    }

    /** */
    private List<RelCollation> toCollations(RelCollation collation) {
        return collation == null || collation.getFieldCollations().isEmpty() ? emptyList() : singletonList(collation);
    }

    /** */
    private NodesMapping partitionedMapping(@NotNull GridCacheContext<?,?> cctx, @NotNull AffinityTopologyVersion topVer) {
        byte flags = NodesMapping.HAS_PARTITIONED_CACHES;

        List<List<ClusterNode>> assignments = cctx.affinity().assignments(topVer);
        List<List<UUID>> res;

        if (cctx.config().getWriteSynchronizationMode() == CacheWriteSynchronizationMode.PRIMARY_SYNC) {
            res = new ArrayList<>(assignments.size());

            for (List<ClusterNode> partNodes : assignments)
                res.add(F.isEmpty(partNodes) ? emptyList() : singletonList(F.first(partNodes).id()));
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

    /** */
    private NodesMapping replicatedMapping(@NotNull GridCacheContext<?,?> cctx, @NotNull AffinityTopologyVersion topVer) {
        byte flags = NodesMapping.HAS_REPLICATED_CACHES;

        if (cctx.config().getNodeFilter() != null)
            flags |= NodesMapping.PARTIALLY_REPLICATED;

        GridDhtPartitionTopology top = cctx.topology();

        List<ClusterNode> nodes = cctx.discovery().discoCache(topVer).cacheGroupAffinityNodes(cctx.cacheId());
        List<UUID> res;

        if (!top.rebalanceFinished(topVer)) {
            flags |= NodesMapping.PARTIALLY_REPLICATED;

            res = new ArrayList<>(nodes.size());

            int parts = top.partitions();

            for (ClusterNode node : nodes) {
                if (isOwner(node.id(), top, parts))
                    res.add(node.id());
            }
        }
        else
            res = Commons.transform(nodes, ClusterNode::id);

        return new NodesMapping(res, null, flags);
    }

    /** */
    private boolean isOwner(UUID nodeId, GridDhtPartitionTopology top, int parts) {
        for (int p = 0; p < parts; p++) {
            if (top.partitionState(nodeId, p) != GridDhtPartitionState.OWNING)
                return false;
        }
        return true;
    }

    /** */
    private class StatisticsImpl implements Statistic {
        /** {@inheritDoc} */
        @Override public Double getRowCount() {
            return 1000d;
        }

        /** {@inheritDoc} */
        @Override public boolean isKey(ImmutableBitSet cols) {
            return false; // TODO
        }

        /** {@inheritDoc} */
        @Override public List<ImmutableBitSet> getKeys() {
            return null; // TODO
        }

        /** {@inheritDoc} */
        @Override public List<RelReferentialConstraint> getReferentialConstraints() {
            return ImmutableList.of();
        }

        /** {@inheritDoc} */
        @Override public List<RelCollation> getCollations() {
            return collations();
        }

        /** {@inheritDoc} */
        @Override public IgniteDistribution getDistribution() {
            return distribution();
        }
    }
}
