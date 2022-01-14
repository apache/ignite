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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.Nullable;

/**
 * Converts OR to UNION ALL.
 */
public abstract class LogicalOrToUnionRule extends RelRule<LogicalOrToUnionRule.Config> {
    /** Rule instance to replace table scans with condition. */
    public static final RelOptRule SCAN_INSTANCE = new LogicalOrToUnionRule(Config.SCAN) {
        @Override protected RexNode getCondition(RelOptRuleCall call) {
            final IgniteLogicalTableScan rel = call.rel(0);

            return rel.condition();
        }

        @Override protected RelNode getInput(RelOptRuleCall call) {
            return call.rel(0);
        }

        @Override protected void buildInput(RelBuilder relBldr, RelNode input, RexNode condition) {
            IgniteLogicalTableScan scan = (IgniteLogicalTableScan)input;

            // Set default traits, real traits will be calculated for physical node.
            RelTraitSet trait = scan.getCluster().traitSet();

            relBldr.push(IgniteLogicalTableScan.create(
                scan.getCluster(),
                trait,
                scan.getTable(),
                scan.projects(),
                condition,
                scan.requiredColumns()
            ));
        }
    };

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private LogicalOrToUnionRule(Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        final RelOptCluster cluster = call.rel(0).getCluster();

        List<RexNode> operands = getOrOperands(cluster.getRexBuilder(), getCondition(call));

        if (operands == null)
            return;

        if (!idxCollationCheck(call, operands))
            return;

        RelNode input = getInput(call);

        RelNode rel0 = createUnionAll(cluster, input, operands.get(0), operands.get(1));
        RelNode rel1 = createUnionAll(cluster, input, operands.get(1), operands.get(0));

        call.transformTo(rel0, ImmutableMap.of(rel1, rel0));
    }

    /** */
    protected abstract RexNode getCondition(RelOptRuleCall call);

    /**
     * Returns common required columns from scan.
     */
    protected ImmutableBitSet getRequiredColumns(RelOptRuleCall call, int fldCount) {
        final IgniteLogicalTableScan scan = call.rel(0);

        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();

        Mappings.TargetMapping mapping = Commons.inverseMapping(scan.requiredColumns(), fldCount);

        new RexShuttle() {
            @Override public RexNode visitLocalRef(RexLocalRef inputRef) {
                builder.set(mapping.getSourceOpt(inputRef.getIndex()));
                return inputRef;
            }
        }.apply(scan.condition());

        builder.addAll(scan.requiredColumns());

        return builder.build();
    }

    /** */
    protected abstract RelNode getInput(RelOptRuleCall call);

    /**
     * @param call Set of appropriate RelNode.
     * @param operands Operands fron OR expression.
     *
     * Compares intersection (currently begining position) of condition and index fields.
     * This rule need to be triggered only if appropriate indexes will be found otherwise it`s not applicable.
     */
    private boolean idxCollationCheck(RelOptRuleCall call, List<RexNode> operands) {
        final IgniteLogicalTableScan scan = call.rel(0);

        IgniteTable tbl = scan.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = Commons.typeFactory(scan.getCluster());
        int fieldCount = tbl.getRowType(typeFactory).getFieldCount();

        ImmutableBitSet commonReqCols = getRequiredColumns(call, fieldCount);

        boolean idxPreMatch = false;

        Map<String, RelCollation> cache = new HashMap<>();

        for (RexNode op : operands) {
            ImmutableBitSet.Builder builder = ImmutableBitSet.builder();

            new RexShuttle() {
                @Override public RexNode visitLocalRef(RexLocalRef inputRef) {
                    builder.set(inputRef.getIndex());
                    return inputRef;
                }
            }.apply(op);

            ImmutableBitSet localReqCols = builder.build();

            idxPreMatch = false;

            for (IgniteIndex idx : tbl.indexes().values()) {
                final RelCollation col0 = idx.collation();

                RelCollation collProj =
                    cache.computeIfAbsent(idx.name(), k -> col0.apply(Commons.mapping(commonReqCols, fieldCount)));

                if (collProj.getFieldCollations().isEmpty())
                    continue;

                int firstIdxColPos = collProj.getKeys().get(0);

                if (localReqCols.get(firstIdxColPos)) {
                    idxPreMatch = true;
                    break;
                }
            }

            if (!idxPreMatch)
                break;
        }

        return idxPreMatch;
    }

    /** */
    private static @Nullable List<RexNode> getOrOperands(RexBuilder rexBuilder, RexNode condition) {
        RexNode dnf = RexUtil.toDnf(rexBuilder, condition);

        if (!dnf.isA(SqlKind.OR))
            return null;

        List<RexNode> operands = RelOptUtil.disjunctions(dnf);

        if (operands.size() != 2 || RexUtil.find(SqlKind.IS_NULL).anyContain(operands))
            return null;

        return operands;
    }

    /** */
    protected abstract void buildInput(RelBuilder relBldr, RelNode input, RexNode condition);

    /**
     * Creates 'UnionAll' for conditions.
     *
     * @param cluster The cluster UnionAll expression will belongs to.
     * @param input Input.
     * @param op1 First filter condition.
     * @param op2 Second filter condition.
     * @return UnionAll expression.
     */
    private RelNode createUnionAll(RelOptCluster cluster, RelNode input, RexNode op1, RexNode op2) {
        RelBuilder relBldr = relBuilderFactory.create(cluster, null);

        buildInput(relBldr, input, op1);
        buildInput(relBldr, input, relBldr.and(op2, relBldr.or(relBldr.isNull(op1), relBldr.not(op1))));

        return relBldr
            .union(true)
            .build();
    }

    /** */
    private static boolean preMatch(IgniteLogicalTableScan scan) {
        return scan.condition() != null &&
            // _key_PK not interesting here, but it`s depend on current PK implementation, in future PK can be removed
            // and this condition will become incorrect.
            scan.getTable().unwrap(IgniteTable.class).indexes().size() >= 2;
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public interface Config extends RelRule.Config {
        /** */
        Config DEFAULT = RelRule.Config.EMPTY
            .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
            .as(Config.class);

        /** */
        Config SCAN = DEFAULT
            .withDescription("ScanLogicalOrToUnionRule")
            .withOperandSupplier(o -> o.operand(IgniteLogicalTableScan.class)
                .predicate(LogicalOrToUnionRule::preMatch)
                .noInputs()
            )
            .as(Config.class);
    }
}
