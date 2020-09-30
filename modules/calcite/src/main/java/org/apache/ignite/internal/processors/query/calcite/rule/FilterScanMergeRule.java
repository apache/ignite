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
package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.builder;
import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.simplifier;

/**
 * Rule that pushes filter into the scan. This might be useful for index range scans.
 */
public abstract class FilterScanMergeRule<T extends ProjectableFilterableTableScan> extends RelOptRule {
    /** Instance. */
    public static final FilterScanMergeRule<IgniteIndexScan> INDEX_SCAN =
        new FilterScanMergeRule<IgniteIndexScan>(LogicalFilter.class, IgniteIndexScan.class, "FilterIndexScanMergeRule") {
            /** {@inheritDoc} */
            @Override protected IgniteIndexScan createNode(
                RelOptCluster cluster,
                IgniteIndexScan scan,
                List<RexNode> projects,
                RexNode cond,
                ImmutableBitSet requiredColunms
            ) {
                return new IgniteIndexScan(cluster, scan.getTraitSet(), scan.getTable(), scan.indexName(),
                    projects, cond, requiredColunms);
            }
        };

    /** Instance. */
    public static final FilterScanMergeRule<IgniteTableScan> TABLE_SCAN =
        new FilterScanMergeRule<IgniteTableScan>(LogicalFilter.class, IgniteTableScan.class, "FilterTableScanMergeRule") {
            /** {@inheritDoc} */
            @Override protected IgniteTableScan createNode(
                RelOptCluster cluster,
                IgniteTableScan scan,
                List<RexNode> projects,
                RexNode cond,
                ImmutableBitSet requiredColunms
            ) {
                return new IgniteTableScan(cluster, scan.getTraitSet(), scan.getTable(), projects, cond, requiredColunms);
            }
        };

    /**
     * Constructor.
     *
     * @param clazz Class of relational expression to match.
     * @param desc Description, or null to guess description
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
        List<RexNode> projects = scan.projects();

        RelMetadataQuery mq = call.getMetadataQuery();

        RexNode condition = filter.getCondition();

        RexSimplify simplifier = simplifier(cluster);

        // Let's remove from the condition common with the scan filter parts.
        condition = simplifier
            .withPredicates(mq.getPulledUpPredicates(scan))
            .simplifyUnknownAsFalse(condition);

        // We need to replace RexInputRef with RexLocalRef because TableScan doesn't have inputs.
        condition = condition.accept(new InputRefReplacer());

        // Combine the condition with the scan filter.
        condition = RexUtil.composeConjunction(builder(cluster), F.asList(condition, scan.condition()));

        // Final simplification. We need several phases because simplifier sometimes
        // (see RexSimplify.simplifyGenericNode) leaves UNKNOWN nodes that can be
        // eliminated on next simplify attempt. We limit attempts count not to break
        // planning performance on complex condition.
        Set<RexNode> nodes = new HashSet<>();
        while (nodes.add(condition) && nodes.size() < 3)
            condition = simplifier.simplifyUnknownAsFalse(condition);

        Holder params = projectTransform(filter, scan);

        condition = params.conditions == null ? condition : params.conditions;

        projects = params.projects == null ? projects : params.projects;

        ImmutableBitSet requiredColunms = params.requiredColunms;

        call.transformTo(createNode(cluster, scan, projects, condition, requiredColunms));
    }

    /** */
    protected abstract T createNode(RelOptCluster cluster, T scan, List<RexNode> project, RexNode cond,
                                    ImmutableBitSet requiredColunms);

    /** Visitor for replacing input refs to local refs. We need it for proper plan serialization. */
    private static class InputRefReplacer extends RexShuttle {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            return new RexLocalRef(inputRef.getIndex(), inputRef.getType());
        }
    }

    /** Collects only participating members and applies appropriate mappings.
     *
     * @param filter Initial filter.
     * @param scan TableScan implementation.
     */
    private Holder projectTransform(LogicalFilter filter, T scan) {
        List<RexNode> projects = scan.projects();

        if (projects != null) {
            RexNode condition = filter.getCondition();

            final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();

            RexShuttle shuttle = new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef ref) {
                    builder.set(ref.getIndex());
                    return ref;
                }
            };

            shuttle.apply(projects);

            ImmutableBitSet requiredColunms = builder.build();

            IgniteTypeFactory typeFactory = Commons.context(scan).typeFactory();
            IgniteTable tbl = scan.getTable().unwrap(IgniteTable.class);

            Mappings.TargetMapping targetMapping = Mappings.create(MappingType.PARTIAL_FUNCTION,
                tbl.getRowType(typeFactory).getFieldCount(), requiredColunms.cardinality());

            for (Ord<Integer> ord : Ord.zip(requiredColunms))
                targetMapping.set(ord.e, ord.i);

            shuttle = new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef inputRef) {
                    return new RexLocalRef(targetMapping.getTarget(inputRef.getIndex()), inputRef.getType());
                }
            };

            projects = shuttle.apply(projects);

            if (condition != null)
                condition = shuttle.apply(condition);

            return new Holder(projects, condition, requiredColunms);
        }
        else
            return Holder.EMPTY;
    }

    /** Transformation params holder. */
    private static class Holder {
        /** */
        private static final Holder EMPTY = new Holder(null, null, null);

        /** Projects. */
        private final List<RexNode> projects;

        /** Filters. */
        private final RexNode conditions;

        /** Participating columns. */
        private final ImmutableBitSet requiredColunms;

        /** Constructor. */
        private Holder(List<RexNode> projects, RexNode conditions, ImmutableBitSet requiredColunms) {
            this.projects = projects;
            this.conditions = conditions;
            this.requiredColunms = requiredColunms;
        }
    }
}
