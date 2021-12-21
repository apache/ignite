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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Rule that pushes filter into the scan. This might be useful for index range scans.
 */
public abstract class FilterScanMergeRule<T extends ProjectableFilterableTableScan>
    extends RelRule<FilterScanMergeRule.Config> {
    /** Instance. */
    public static final FilterScanMergeRule<IgniteLogicalIndexScan> INDEX_SCAN =
        new FilterIndexScanMergeRule(Config.INDEX_SCAN);

    /** Instance. */
    public static final FilterScanMergeRule<IgniteLogicalTableScan> TABLE_SCAN =
        new FilterTableScanMergeRule(Config.TABLE_SCAN);

    /** Instance. */
    public static final FilterScanMergeRule<IgniteLogicalTableScan> TABLE_SCAN_SKIP_CORRELATED =
        new FilterTableScanMergeRule(Config.TABLE_SCAN_SKIP_CORRELATED);

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
        condition = RexUtils.replaceInputRefs(RexUtil.expandSearch(builder, null, condition));

        // Set default traits, real traits will be calculated for physical node.
        RelTraitSet trait = cluster.traitSet();

        RelNode res = createNode(cluster, scan, trait, condition);

        call.transformTo(res);
    }

    /** */
    protected abstract T createNode(RelOptCluster cluster, T scan, RelTraitSet traits, RexNode cond);

    /** */
    private static class FilterIndexScanMergeRule extends FilterScanMergeRule<IgniteLogicalIndexScan> {
        /** */
        private FilterIndexScanMergeRule(FilterScanMergeRule.Config cfg) {
            super(cfg);
        }

        /** {@inheritDoc} */
        @Override protected IgniteLogicalIndexScan createNode(
            RelOptCluster cluster,
            IgniteLogicalIndexScan scan,
            RelTraitSet traits,
            RexNode cond
        ) {
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
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = EMPTY.withRelBuilderFactory(RelFactories.LOGICAL_BUILDER).as(Config.class);

        /** */
        Config TABLE_SCAN = DEFAULT
            .withScanRuleConfig(IgniteLogicalTableScan.class, "FilterTableScanMergeRule", false);

        /** */
        Config TABLE_SCAN_SKIP_CORRELATED = DEFAULT
            .withScanRuleConfig(IgniteLogicalTableScan.class, "FilterTableScanMergeSkipCorrelatedRule", true);

        /** */
        Config INDEX_SCAN = DEFAULT
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
