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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

/**
 * Rule that pushes filter into the scan. This might be useful for index range scans.
 */
public abstract class FilterScanMergeRule<T extends ProjectableFilterableTableScan> extends RelOptRule {
    /** Instance. */
    public static final FilterScanMergeRule<IgniteLogicalIndexScan> INDEX_SCAN =
        new FilterScanMergeRule<IgniteLogicalIndexScan>(LogicalFilter.class, IgniteLogicalIndexScan.class, "FilterIndexScanMergeRule") {
            /** {@inheritDoc} */
            @Override protected IgniteLogicalIndexScan createNode(
                RelOptCluster cluster,
                IgniteLogicalIndexScan scan,
                RelTraitSet traits,
                RexNode cond) {
                Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(cond);

                if (!nullOrEmpty(corrIds))
                    traits = traits.replace(CorrelationTrait.correlations(corrIds));

                return IgniteLogicalIndexScan.create(cluster, traits, scan.getTable(), scan.indexName(),
                    scan.projects(), cond, scan.requiredColumns());
            }
        };

    /** Instance. */
    public static final FilterScanMergeRule<IgniteLogicalTableScan> TABLE_SCAN =
        new FilterScanMergeRule<IgniteLogicalTableScan>(LogicalFilter.class, IgniteLogicalTableScan.class, "FilterTableScanMergeRule") {
            /** {@inheritDoc} */
            @Override protected IgniteLogicalTableScan createNode(
                RelOptCluster cluster,
                IgniteLogicalTableScan scan,
                RelTraitSet traits,
                RexNode cond) {
                Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(cond);

                if (!nullOrEmpty(corrIds))
                    traits = traits.replace(CorrelationTrait.correlations(corrIds));

                return IgniteLogicalTableScan.create(cluster, traits, scan.getTable(), scan.projects(),
                    cond, scan.requiredColumns());
            }
        };

    /**
     * Constructor.
     *
     * @param clazz Class of relational expression to match.
     * @param desc Description, or null to guess description.
     */
    private FilterScanMergeRule(Class<? extends RelNode> clazz, Class<T> tableClass, String desc) {
        super(operand(clazz,
            operand(tableClass, none())),
            RelFactories.LOGICAL_BUILDER,
            desc);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        T scan = call.rel(1);

        RelOptCluster cluster = scan.getCluster();
        RexBuilder builder = RexUtils.builder(cluster);

        RexNode condition = filter.getCondition();
        RexNode remaining = null;

        if (scan.condition() != null)
            condition = RexUtil.composeConjunction(builder, asList(scan.condition(), condition));

        if (scan.projects() != null) {
            IgniteTypeFactory typeFactory = Commons.typeFactory(scan);

            IgniteTable tbl = scan.getTable().unwrap(IgniteTable.class);

            RelDataType cols = tbl.getRowType(typeFactory, scan.requiredColumns());

            Mappings.TargetMapping permutation = RexUtils.permutation(scan.projects(), cols, true);

            List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);

            List<RexNode> condition0 = new ArrayList<>(conjunctions.size());
            List<RexNode> remaining0 = new ArrayList<>(conjunctions.size());

            RexShuttle shuttle = new RexShuttle() {
                @Override public RexNode visitInputRef(RexInputRef ref) {
                    int targetRef = permutation.getTargetOpt(ref.getIndex());
                    if (targetRef == -1)
                        throw new ControlFlowException();
                    return new RexInputRef(targetRef, ref.getType());
                }
            };

            for (RexNode cond0 : conjunctions) {
                try {
                    condition0.add(shuttle.apply(cond0));
                }
                catch (ControlFlowException e) {
                    remaining0.add(cond0);
                }
            }

            condition = RexUtil.composeConjunction(builder, condition0, false);
            remaining = RexUtil.composeConjunction(builder, remaining0, true);
        }

        // We need to replace RexInputRef with RexLocalRef because TableScan doesn't have inputs.
        // TODO SEARCH support
        condition = RexUtils.replaceInputRefs(RexUtil.expandSearch(builder, null, condition));

        RelTraitSet trait = scan.getTraitSet();
        CorrelationTrait filterCorr = TraitUtils.correlation(filter);

        if (filterCorr.correlated())
            trait = trait.replace(filterCorr);

        RelNode res = createNode(cluster, scan, trait, condition);

        if (remaining != null) {
            res = relBuilderFactory.create(cluster, null)
                .push(res)
                .filter(remaining)
                .build();
        }

        call.transformTo(res);
    }

    /** */
    protected abstract T createNode(RelOptCluster cluster, T scan, RelTraitSet traits, RexNode cond);
}
