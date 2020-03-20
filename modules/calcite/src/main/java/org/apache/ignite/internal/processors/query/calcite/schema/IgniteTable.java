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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TableScan;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/** */
public class IgniteTable extends AbstractTable implements TranslatableTable, ScannableTable, DistributedTable, SortedTable {
    /** */
    private final String name;

    /** */
    private final TableDescriptor desc;

    /** */
    private final Statistic statistic;

    /** */
    private final List<RelCollation> collations;

    /** */
    private final Map<String, IgniteTable> indexes = new ConcurrentHashMap<>();

    /**
     * @param name Table full name.
     */
    public IgniteTable(String name, TableDescriptor desc, List<RelCollation> collations) {
        this.name = name;
        this.desc = desc;
        this.collations = collations;

        statistic = new StatisticsImpl();
    }

    /**
     * @return Table name.
     */
    public String name() {
        return name;
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
    @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();

        return toRel(cluster, relOptTable);
    }

    /**
     * Converts table into relational expression.
     *
     * @param cluster Custer.
     * @param relOptTable Table.
     * @return Table relational expression.
     */
    public IgniteTableScan toRel(RelOptCluster cluster, RelOptTable relOptTable) {
        RelTraitSet traitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, this::collations)
            .replaceIf(DistributionTraitDef.INSTANCE, this::distribution);

        return new IgniteTableScan(cluster, traitSet, relOptTable);
    }

    /**
     * @return Indexes for the current table.
     */
    public Map<String, IgniteTable> indexes() {
        return indexes;
    }

    /**
     * Adds index to table.
     * @param idxName Index name.
     * @param collation Index collation.
     */
    public void addIndex(String idxName, RelCollation collation) {
        IgniteTable idx = new IgniteTable(idxName, desc, Collections.singletonList(collation));

        indexes.put(idxName, idx);
    }

    /**
     * @return Column descriptors.
     */
    public ColumnDescriptor[] columnDescriptors() {
        return desc.columnDescriptors();
    }

    /**
     * @return Map of column descriptors.
     */
    public Map<String, ColumnDescriptor> columnDescriptorsMap() {
        return desc.columnDescriptorsMap();
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
    @Override public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(new TableScan((ExecutionContext) root, desc));
    }

    /** {@inheritDoc} */
    @Override public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(desc))
            return aClass.cast(desc);

        return super.unwrap(aClass);
    }

    /** */
    private NodesMapping partitionedMapping(@NotNull GridCacheContext<?,?> cctx, @NotNull AffinityTopologyVersion topVer) {
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

    /** */
    private NodesMapping replicatedMapping(@NotNull GridCacheContext<?,?> cctx, @NotNull AffinityTopologyVersion topVer) {
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

    /** */
    private boolean isOwner(UUID nodeId, GridDhtPartitionTopology topology, int parts) {
        for (int p = 0; p < parts; p++) {
            if (topology.partitionState(nodeId, p) != GridDhtPartitionState.OWNING)
                return false;
        }
        return true;
    }

    /** */
    public void removeIndex(String idxName) {
        indexes.remove(idxName);
    }

    /** */
    private class StatisticsImpl implements Statistic {
        /** {@inheritDoc} */
        @Override public Double getRowCount() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isKey(ImmutableBitSet columns) {
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
