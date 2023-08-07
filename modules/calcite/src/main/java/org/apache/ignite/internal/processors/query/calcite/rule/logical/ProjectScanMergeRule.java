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

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

/** */
@Value.Enclosing
public abstract class ProjectScanMergeRule<T extends ProjectableFilterableTableScan>
    extends RelRule<ProjectScanMergeRule.Config> {
    /** Instance. */
    public static final RelOptRule INDEX_SCAN = Config.INDEX_SCAN.toRule();

    /** Instance. */
    public static final RelOptRule TABLE_SCAN = Config.TABLE_SCAN.toRule();

    /** Instance. */
    public static final RelOptRule TABLE_SCAN_SKIP_CORRELATED = Config.TABLE_SCAN_SKIP_CORRELATED.toRule();

    /** */
    protected abstract @Nullable T createNode(
        RelOptCluster cluster,
        T scan,
        RelTraitSet traits,
        List<RexNode> projections,
        RexNode cond,
        ImmutableBitSet requiredColumns
    );

    /**
     * Constructor.
     *
     * @param config Project scan merge rule config,
     */
    private ProjectScanMergeRule(Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalProject relProject = call.rel(0);
        T scan = call.rel(1);

        RelOptCluster cluster = scan.getCluster();
        List<RexNode> projects = relProject.getProjects();
        RexNode cond = scan.condition();
        ImmutableBitSet requiredColumns = scan.requiredColumns();
        List<RexNode> scanProjects = scan.projects();

        // Set default traits, real traits will be calculated for physical node.
        RelTraitSet traits = cluster.traitSet();

        IgniteTable tbl = scan.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = Commons.typeFactory(cluster);

        if (requiredColumns == null) {
            assert scanProjects == null;

            ImmutableBitSet.Builder builder = ImmutableBitSet.builder();

            new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef ref) {
                    builder.set(ref.getIndex());
                    return ref;
                }
            }.apply(projects);

            new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef inputRef) {
                    builder.set(inputRef.getIndex());
                    return inputRef;
                }
            }.apply(cond);

            requiredColumns = builder.build();

            Mappings.TargetMapping targetMapping = Commons.mapping(requiredColumns,
                tbl.getRowType(typeFactory).getFieldCount());

            projects = new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef ref) {
                    return new RexLocalRef(targetMapping.getTarget(ref.getIndex()), ref.getType());
                }
            }.apply(projects);

            cond = new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef ref) {
                    return new RexLocalRef(targetMapping.getTarget(ref.getIndex()), ref.getType());
                }
            }.apply(cond);
        }
        else
            projects = RexUtils.replaceInputRefs(projects);

        if (scanProjects != null) {
            // Merge projects.
            projects = new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef ref) {
                    return scanProjects.get(ref.getIndex());
                }
            }.apply(projects);
        }

        if (RexUtils.isIdentity(projects, tbl.getRowType(typeFactory, requiredColumns), true))
            projects = null;

        T res = createNode(cluster, scan, traits, projects, cond, requiredColumns);

        if (res == null)
            return;

        call.transformTo(res);

        if (!RexUtils.hasCorrelation(relProject.getProjects()))
            cluster.getPlanner().prune(relProject);
    }

    /** */
    private static class ProjectTableScanMergeRule extends ProjectScanMergeRule<IgniteLogicalTableScan> {
        /**
         * Constructor.
         *
         * @param config Project scan merge rule config,
         */
        private ProjectTableScanMergeRule(ProjectScanMergeRule.Config config) {
            super(config);
        }

        /** {@inheritDoc} */
        @Override protected IgniteLogicalTableScan createNode(
            RelOptCluster cluster,
            IgniteLogicalTableScan scan,
            RelTraitSet traits,
            List<RexNode> projections,
            RexNode cond,
            ImmutableBitSet requiredColumns
        ) {
            return IgniteLogicalTableScan.create(
                cluster,
                traits,
                scan.getTable(),
                scan.getHints(),
                projections,
                cond,
                requiredColumns
            );
        }
    }

    /** */
    private static class ProjectIndexScanMergeRule extends ProjectScanMergeRule<IgniteLogicalIndexScan> {
        /**
         * Constructor.
         *
         * @param config Project scan merge rule config,
         */
        private ProjectIndexScanMergeRule(ProjectScanMergeRule.Config config) {
            super(config);
        }

        /** {@inheritDoc} */
        @Override protected @Nullable IgniteLogicalIndexScan createNode(
            RelOptCluster cluster,
            IgniteLogicalIndexScan scan,
            RelTraitSet traits,
            List<RexNode> projections,
            RexNode cond,
            ImmutableBitSet requiredColumns
        ) {
            if (scan.getTable().unwrap(IgniteTable.class).isIndexRebuildInProgress()) {
                cluster.getPlanner().prune(scan);

                return null;
            }

            return IgniteLogicalIndexScan.create(
                cluster,
                traits,
                scan.getTable(),
                scan.indexName(),
                projections,
                cond, requiredColumns
            );
        }
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable(singleton = false)
    public interface Config extends RuleFactoryConfig<Config> {
        /** */
        Config DEFAULT = ImmutableProjectScanMergeRule.Config.builder()
            .withRuleFactory(ProjectTableScanMergeRule::new)
            .build();

        /** */
        Config TABLE_SCAN = DEFAULT.withScanRuleConfig(
            IgniteLogicalTableScan.class, "ProjectTableScanMergeRule", false);

        /** */
        Config TABLE_SCAN_SKIP_CORRELATED = DEFAULT.withScanRuleConfig(
            IgniteLogicalTableScan.class, "ProjectTableScanMergeSkipCorrelatedRule", true);

        /** */
        Config INDEX_SCAN = DEFAULT
            .withRuleFactory(ProjectIndexScanMergeRule::new)
            .withScanRuleConfig(IgniteLogicalIndexScan.class, "ProjectIndexScanMergeRule", false);

        /** */
        default Config withScanRuleConfig(
            Class<? extends ProjectableFilterableTableScan> scanCls,
            String desc,
            boolean skipCorrelated
        ) {
            return withDescription(desc)
                .withOperandSupplier(b -> b.operand(LogicalProject.class)
                    .predicate(p -> !skipCorrelated || !RexUtils.hasCorrelation(p.getProjects()))
                    .oneInput(b1 -> b1.operand(scanCls).noInputs()))
                .as(Config.class);
        }
    }
}
