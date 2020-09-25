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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.FilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;

/** */
public abstract class ProjectableScanRule<T extends FilterableTableScan> extends RelOptRule {
    /** Instance. */
    public static final ProjectableScanRule<IgniteIndexScan> FILTER_WITH_PROJECTIONS_INTO_INDEX_SCAN =
        new ProjectableScanRule<IgniteIndexScan>(LogicalProject.class, IgniteIndexScan.class,
            "PushFilterAndProjectionsIntoIndexScanRule") {
            /** {@inheritDoc} */
            @Override protected IgniteIndexScan createNode(
                RelOptCluster cluster,
                IgniteIndexScan scan,
                ArrayList<RexNode> projections,
                ImmutableBitSet requiredColumns
            ) {
                return new IgniteIndexScan(cluster, scan.getTraitSet(), scan.getTable(), scan.indexName(),
                    projections, requiredColumns);
            }
        };

    /** Instance. */
    public static final ProjectableScanRule<IgniteTableScan> FILTER_WITH_PROJECTIONS_INTO_TABLE_SCAN =
        new ProjectableScanRule<IgniteTableScan>(LogicalProject.class, IgniteTableScan.class,
            "PushFilterAndProjectionsIntoTableScanRule") {
            /** {@inheritDoc} */
            @Override protected IgniteTableScan createNode(
                RelOptCluster cluster,
                IgniteTableScan scan,
                ArrayList<RexNode> projections,
                ImmutableBitSet requiredColumns
            ) {
                return new IgniteTableScan(cluster, scan.getTraitSet(), scan.getTable(), null, projections,
                    requiredColumns);
            }
        };

    /** */
    protected abstract T createNode(RelOptCluster cluster, T scan, ArrayList<RexNode> projections, ImmutableBitSet requiredColumns);

    /**
     * Constructor.
     *
     * @param projectionClazz Projection class of relational expression to match.
     * @param tableClass Ignite scan class.
     * @param desc Description, or null to guess description
     */
    private ProjectableScanRule(Class<? extends RelNode> projectionClazz, Class<T> tableClass, String desc) {
        super(operand(projectionClazz,
            operand(tableClass, none())),
            RelFactories.LOGICAL_BUILDER,
            desc);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalProject proj = call.rel(0);
        T scan = call.rel(1);

        RelOptCluster cluster = scan.getCluster();

        List<RexNode> projections = proj.getProjects();

        final ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();

        final int[] cnt = {0}; // fix it

        for (RexNode node : projections) {
            node.accept(new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef ref) {
                    requiredColumns.set(ref.getIndex());

                    cnt[0]++;

                    return ref;
                }
            });
        }

        ArrayList<RexNode> proj0 = new ArrayList<>(projections.size());

        ImmutableBitSet requiredColumns0 = requiredColumns.build();

        Mappings.TargetMapping mapping = mappings0(cnt[0], requiredColumns0);

        for (RexNode node : projections) {
            RexNode n = node.accept(new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef ref) {
                    return new RexInputRef(mapping.getTarget(ref.getIndex()), ref.getType());
                }
            });
            proj0.add(n);
        }

        call.transformTo(createNode(cluster, scan, proj0, requiredColumns0));
    }

    /** */
    private Mappings.TargetMapping mappings0(int columnCnt, ImmutableBitSet requiredColumns) {
        Mapping m = Mappings.create(MappingType.FUNCTION, columnCnt, requiredColumns.cardinality());

        int realPos = 0;

        for (int i = 0; i < columnCnt; ++i) {
            if (requiredColumns.get(i))
                m.set(i, realPos++);
        }

        return m;
    }
}
