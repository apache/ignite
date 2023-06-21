/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.QueryProperties;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;

/** */
public abstract class LogicalScanConverterRule<T extends ProjectableFilterableTableScan> extends AbstractIgniteConverterRule<T> {
    /** Instance. */
    public static final LogicalScanConverterRule<IgniteLogicalIndexScan> INDEX_SCAN =
        new LogicalScanConverterRule<IgniteLogicalIndexScan>(IgniteLogicalIndexScan.class, "LogicalIndexScanConverterRule") {
            /** {@inheritDoc} */
            @Override protected PhysicalNode convert(
                RelOptPlanner planner,
                RelMetadataQuery mq,
                IgniteLogicalIndexScan rel
            ) {
                RelOptCluster cluster = rel.getCluster();
                IgniteTable table = rel.getTable().unwrap(IgniteTable.class);
                IgniteIndex idx = table.getIndex(rel.indexName());

                if (table.isIndexRebuildInProgress()) {
                    cluster.getPlanner().prune(rel);

                    return null;
                }

                RelDistribution distribution;
                QueryProperties qryProps = planner.getContext().unwrap(QueryProperties.class);
                if (qryProps != null && qryProps.isLocal())
                    distribution = IgniteDistributions.single();
                else
                    distribution = table.distribution();

                RelCollation collation = idx.collation();

                if (rel.projects() != null || rel.requiredColumns() != null) {
                    Mappings.TargetMapping mapping = createMapping(
                        rel.projects(),
                        rel.requiredColumns(),
                        table.getRowType(cluster.getTypeFactory()).getFieldCount()
                    );

                    distribution = distribution.apply(mapping);
                    collation = collation.apply(mapping);
                }

                RelTraitSet traits = rel.getCluster().traitSetOf(IgniteConvention.INSTANCE)
                    .replace(RewindabilityTrait.REWINDABLE)
                    .replace(distribution)
                    .replace(collation);

                Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(rel.condition());

                if (!F.isEmpty(rel.projects())) {
                    corrIds = new HashSet<>(corrIds);
                    corrIds.addAll(RexUtils.extractCorrelationIds(rel.projects()));
                }

                if (!corrIds.isEmpty())
                    traits = traits.replace(CorrelationTrait.correlations(corrIds));

                return new IgniteIndexScan(
                    cluster,
                    traits,
                    rel.getTable(),
                    rel.indexName(),
                    rel.projects(),
                    rel.condition(),
                    rel.searchBounds(),
                    rel.requiredColumns(),
                    idx.collation()
                );
            }
        };

    /** Instance. */
    public static final LogicalScanConverterRule<IgniteLogicalTableScan> TABLE_SCAN =
        new LogicalScanConverterRule<IgniteLogicalTableScan>(IgniteLogicalTableScan.class, "LogicalTableScanConverterRule") {
            /** {@inheritDoc} */
            @Override protected PhysicalNode convert(
                RelOptPlanner planner,
                RelMetadataQuery mq,
                IgniteLogicalTableScan rel
            ) {
                RelOptCluster cluster = rel.getCluster();
                IgniteTable table = rel.getTable().unwrap(IgniteTable.class);

                RelDistribution distribution;
                QueryProperties qryProps = planner.getContext().unwrap(QueryProperties.class);
                if (qryProps != null && qryProps.isLocal())
                    distribution = IgniteDistributions.single();
                else
                    distribution = table.distribution();

                if (rel.requiredColumns() != null) {
                    Mappings.TargetMapping mapping = createMapping(
                        rel.projects(),
                        rel.requiredColumns(),
                        table.getRowType(cluster.getTypeFactory()).getFieldCount()
                    );

                    distribution = distribution.apply(mapping);
                }

                RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
                    .replace(RewindabilityTrait.REWINDABLE)
                    .replace(distribution);

                Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(rel.condition());

                if (!F.isEmpty(rel.projects())) {
                    corrIds = new HashSet<>(corrIds);
                    corrIds.addAll(RexUtils.extractCorrelationIds(rel.projects()));
                }

                if (!corrIds.isEmpty())
                    traits = traits.replace(CorrelationTrait.correlations(corrIds));

                return new IgniteTableScan(rel.getCluster(), traits,
                    rel.getTable(), rel.projects(), rel.condition(), rel.requiredColumns());
            }
        };

    /** */
    private LogicalScanConverterRule(Class<T> clazz, String descPrefix) {
        super(clazz, descPrefix);
    }

    /** */
    public static Mappings.TargetMapping createMapping(
        List<RexNode> projects,
        ImmutableBitSet requiredColumns,
        int tableRowSize
    ) {
        if (projects != null) {
            Mapping trimmingMapping = requiredColumns != null
                ? Mappings.invert(Mappings.source(requiredColumns.asList(), tableRowSize))
                : Mappings.createIdentity(tableRowSize);

            Map<Integer, Integer> mappingMap = new HashMap<>();

            for (int i = 0; i < projects.size(); i++) {
                RexNode rex = projects.get(i);
                if (!(rex instanceof RexLocalRef))
                    continue;

                RexLocalRef ref = (RexLocalRef)rex;

                mappingMap.put(trimmingMapping.getSource(ref.getIndex()), i);
            }

            return Mappings.target(
                mappingMap,
                tableRowSize,
                projects.size()
            );
        }

        if (requiredColumns != null)
            return Mappings.target(requiredColumns.asList(), tableRowSize);

        return Mappings.createIdentity(tableRowSize);
    }
}
