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
package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Rule that pushes filter into the scan. This might be useful for index range scans.
 */
@Value.Enclosing
public abstract class FilterScanMergeRule<T extends ProjectableFilterableTableScan>
    extends RelRule<FilterScanMergeRule.Config> {
    /** Instance. */
    public static final RelOptRule INDEX_SCAN = Config.INDEX_SCAN.toRule();

    /** Instance. */
    public static final RelOptRule TABLE_SCAN = Config.TABLE_SCAN.toRule();

    /** Instance. */
    public static final RelOptRule TABLE_SCAN_SKIP_CORRELATED = Config.TABLE_SCAN_SKIP_CORRELATED.toRule();

    /**
     * Constructor.
     *
     * @param config Filter scan merge rule config.
     */
    private FilterScanMergeRule(Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        T scan = call.rel(1);

        RelOptCluster cluster = scan.getCluster();
        RexBuilder builder = RexUtils.builder(cluster);

        RexNode condition = filter.getCondition();

        if (scan.projects() != null) {
            RexShuttle shuttle = new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef ref) {
                    return scan.projects().get(ref.getIndex());
                }
            };

            condition = shuttle.apply(condition);
        }

        if (scan.condition() != null)
            condition = RexUtil.composeConjunction(builder, F.asList(scan.condition(), condition));

        // We need to replace RexInputRef with RexLocalRef because TableScan doesn't have inputs.
        // TODO SEARCH support
        //condition = RexUtils.replaceInputRefs(RexUtil.expandSearch(builder, null, condition));
        condition = RexUtils.replaceInputRefs(condition);

        // Set default traits, real traits will be calculated for physical node.
        RelTraitSet trait = cluster.traitSet();

        RelNode res = createNode(cluster, scan, trait, condition);

        if (res == null)
            return;

        call.transformTo(res);
    }

    /** */
    protected abstract @Nullable T createNode(RelOptCluster cluster, T scan, RelTraitSet traits, RexNode cond);

    /** */
    private static class FilterIndexScanMergeRule extends FilterScanMergeRule<IgniteLogicalIndexScan> {
        /** */
        private FilterIndexScanMergeRule(FilterScanMergeRule.Config cfg) {
            super(cfg);
        }

        /** {@inheritDoc} */
        @Override protected @Nullable IgniteLogicalIndexScan createNode(
            RelOptCluster cluster,
            IgniteLogicalIndexScan scan,
            RelTraitSet traits,
            RexNode cond
        ) {
            if (scan.getTable().unwrap(IgniteTable.class).isIndexRebuildInProgress()) {
                cluster.getPlanner().prune(scan);

                return null;
            }

            return IgniteLogicalIndexScan.create(cluster, traits, scan.getTable(), scan.indexName(),
                scan.projects(), cond, scan.requiredColumns());
        }
    }

    /** */
    private static class FilterTableScanMergeRule extends FilterScanMergeRule<IgniteLogicalTableScan> {
        /** */
        private FilterTableScanMergeRule(FilterScanMergeRule.Config cfg) {
            super(cfg);
        }

        /** {@inheritDoc} */
        @Override protected IgniteLogicalTableScan createNode(
            RelOptCluster cluster,
            IgniteLogicalTableScan scan,
            RelTraitSet traits,
            RexNode cond
        ) {
            return IgniteLogicalTableScan.create(cluster, traits, scan.getTable(), scan.projects(),
                cond, scan.requiredColumns());
        }
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable(singleton = false)
    public interface Config extends RuleFactoryConfig<Config> {
        /** */
        Config DEFAULT = ImmutableFilterScanMergeRule.Config.builder()
            .withRuleFactory(FilterTableScanMergeRule::new)
            .build();

        /** */
        Config TABLE_SCAN = DEFAULT
            .withScanRuleConfig(IgniteLogicalTableScan.class, "FilterTableScanMergeRule", false);

        /** */
        Config TABLE_SCAN_SKIP_CORRELATED = DEFAULT
            .withScanRuleConfig(IgniteLogicalTableScan.class, "FilterTableScanMergeSkipCorrelatedRule", true);

        /** */
        Config INDEX_SCAN = DEFAULT
            .withRuleFactory(FilterIndexScanMergeRule::new)
            .withScanRuleConfig(IgniteLogicalIndexScan.class, "FilterIndexScanMergeRule", false);

        /** */
        default Config withScanRuleConfig(
            Class<? extends ProjectableFilterableTableScan> scanCls,
            String desc,
            boolean skipCorrelated
        ) {
            return withDescription(desc)
                .withOperandSupplier(b -> b.operand(LogicalFilter.class)
                    .predicate(p -> !skipCorrelated || !RexUtils.hasCorrelation(p.getCondition()))
                    .oneInput(b1 -> b1.operand(scanCls).noInputs()))
                .as(Config.class);
        }
    }
}
