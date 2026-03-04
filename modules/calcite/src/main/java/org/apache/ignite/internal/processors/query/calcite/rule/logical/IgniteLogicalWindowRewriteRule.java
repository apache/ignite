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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.immutables.value.Value;

/**
 * Rule that rewrites LogicalWindow to LogicalAggregate LogicalJoin LogicalProject.
 * This approach is valid only for unbounded frame.
 */
@Value.Enclosing
public class IgniteLogicalWindowRewriteRule extends RelRule<IgniteLogicalWindowRewriteRule.Config> {
    /** Rule instance. */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private IgniteLogicalWindowRewriteRule(Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalWindow win = call.rel(0);

        if (win.groups.size() > 1) {
            RelNode input = win.getInput();
            RelDataTypeFactory typeFactory = win.getCluster().getTypeFactory();

            for (LogicalWindow.Group grp : win.groups) {
                RelDataType joinRowType = buildWindowRowType(typeFactory, input, grp);

                LogicalWindow single = LogicalWindow.create(
                    input.getTraitSet(),
                    input,
                    win.getConstants(),
                    joinRowType,
                    List.of(grp)
                );

                input = single;
            }

            call.transformTo(input);

            return;
        }

        LogicalWindow.Group grp = win.groups.get(0);

        validateSupported(grp);

        RelNode input = win.getInput();
        RexBuilder rexBuilder = win.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = win.getCluster().getTypeFactory();
        RelNode aggInput = appendConstants(input, win.getConstants());

        ImmutableBitSet grpSet = grp.keys;

        List<AggregateCall> aggCalls = new ArrayList<>(grp.aggCalls.size());

        for (Window.RexWinAggCall winAggCall : grp.aggCalls) {
            aggCalls.add(toAggregateCall(winAggCall, typeFactory, grpSet.cardinality()));
        }

        RelNode agg = LogicalAggregate.create(
            aggInput,
            grpSet,
            null,
            aggCalls
        );

        RexNode condition = buildPartitionJoinCondition(rexBuilder, typeFactory, input, agg, grpSet);

        RelNode join = LogicalJoin.create(
            input,
            agg,
            Collections.emptyList(),
            condition,
            Collections.emptySet(),
            JoinRelType.INNER
        );

        RelNode project = LogicalProject.create(
            join,
            List.of(),
            buildProjection(join, win, grp),
            win.getRowType().getFieldNames(),
            ImmutableSet.of()
        );

        call.transformTo(project);
    }

    /**
     * Appends LogicalWindow constants to input as additional projection columns.
     *
     * @param input Input relation.
     * @param constants Window constants.
     * @return Input relation augmented with constants.
     */
    private static RelNode appendConstants(RelNode input, List<RexLiteral> constants) {
        if (constants.isEmpty())
            return input;

        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        int inputFieldCnt = input.getRowType().getFieldCount();

        List<RexNode> projects = new ArrayList<>(inputFieldCnt + constants.size());
        List<String> names = new ArrayList<>(input.getRowType().getFieldNames());

        for (int i = 0; i < inputFieldCnt; i++)
            projects.add(rexBuilder.makeInputRef(input, i));

        projects.addAll(constants);

        for (int i = 0; i < constants.size(); i++)
            names.add("_w_const$" + i);

        return LogicalProject.create(input, List.of(), projects, names, ImmutableSet.of());
    }

    /**
     * Builds a row type for a window with aggregate calls.
     *
     * @param typeFactory Type factory.
     * @param input Input relation.
     * @param grp Window group.
     * @return A row type combining the input fields and windowed aggregate results.
     */
    private static RelDataType buildWindowRowType(
        RelDataTypeFactory typeFactory,
        RelNode input,
        LogicalWindow.Group grp
    ) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();

        builder.addAll(input.getRowType().getFieldList());

        for (int i = 0; i < grp.aggCalls.size(); i++) {
            Window.RexWinAggCall winAggCall = grp.aggCalls.get(i);

            String name = "agg$" + i;

            RelDataType type = winAggCall.getType();

            builder.add(name, type);
        }

        return builder.build();
    }

    /**
     * Builds a join condition between input and aggregate results using partition keys.
     * Returns TRUE for an empty partition set (cross join).
     *
     * @param rexBuilder Rex builder.
     * @param typeFactory Type factory.
     * @param input Input relation.
     * @param agg Aggregate relation.
     * @param groupSet Partition keys.
     * @return Join a condition expression.
     */
    private static RexNode buildPartitionJoinCondition(
        RexBuilder rexBuilder,
        RelDataTypeFactory typeFactory,
        RelNode input,
        RelNode agg,
        ImmutableBitSet groupSet
    ) {
        if (groupSet.isEmpty())
            return rexBuilder.makeLiteral(true);

        int inputFieldCnt = input.getRowType().getFieldCount();
        List<Integer> keys = groupSet.asList();

        RelDataType joinRowType = typeFactory.builder()
            .addAll(input.getRowType().getFieldList())
            .addAll(agg.getRowType().getFieldList())
            .build();

        List<RexNode> conditions = new ArrayList<>(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            int keyIdx = keys.get(i);

            RexNode left = rexBuilder.makeInputRef(joinRowType, keyIdx);
            RexNode right = rexBuilder.makeInputRef(joinRowType, inputFieldCnt + i);

            conditions.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, left, right));
        }

        return RexUtil.composeConjunction(rexBuilder, conditions);
    }

    /**
     * Validates that the window definition is supported by this rule:
     * only unbounded frames are allowed, and ORDER BY is supported only with a full unbounded frame.
     *
     * @param group Window group to validate.
     */
    private void validateSupported(LogicalWindow.Group group) {
        boolean hasOrderBy = !group.orderKeys.getKeys().isEmpty();
        boolean fullFrame = isUnbounded(group.lowerBound, group.upperBound);

        if (hasOrderBy && !fullFrame) {
            throw new IgniteSQLException("ORDER BY with bounded frame is not supported yet.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        if (!fullFrame) {
            throw new IgniteSQLException("Window frame bounds are not supported yet.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }
    }

    /**
     * Checks whether the window frame is fully unbounded (UNBOUNDED PRECEDING ... UNBOUNDED FOLLOWING).
     *
     * @param lower Lower frame bound.
     * @param upper Upper frame bound.
     * @return {@code true} if the frame is unbounded.
     */
    private static boolean isUnbounded(RexWindowBound lower, RexWindowBound upper) {
        if (lower == null && upper == null)
            return true;

        if (lower == null || upper == null)
            return false;

        return lower.isUnbounded() && lower.isPreceding()
            && upper.isUnbounded() && upper.isFollowing();
    }

    /**
     * Converts a window aggregate call to a regular AggregateCall,
     * inferring the result type from the aggregate function.
     *
     * @param winAggCall Window aggregate call.
     * @param typeFactory Type factory.
     * @param grpKeyCnt Partition key count.
     * @return AggregateCall for LogicalAggregate.
     */
    private static AggregateCall toAggregateCall(
        Window.RexWinAggCall winAggCall,
        RelDataTypeFactory typeFactory,
        int grpKeyCnt
    ) {
        List<Integer> argList = new ArrayList<>(winAggCall.getOperands().size());

        for (RexNode operand : winAggCall.getOperands()) {
            if (operand instanceof RexInputRef) {
                RexInputRef ref = (RexInputRef)operand;
                argList.add(ref.getIndex());
            }
            else {
                throw new IgniteSQLException("Window aggregate arguments must be input references.",
                    IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }
        }

        List<RelDataType> operandTypes = winAggCall.getOperands().stream()
            .map(RexNode::getType)
            .collect(Collectors.toList());

        SqlAggFunction agg = (SqlAggFunction)winAggCall.getOperator();

        SqlOperatorBinding binding = new Aggregate.AggCallBinding(typeFactory, agg, operandTypes, grpKeyCnt, false);

        RelDataType inferredType = agg.inferReturnType(binding);

        return AggregateCall.create(
            agg,
            winAggCall.distinct,
            false,
            winAggCall.ignoreNulls,
            argList,
            -1,
            RelCollations.EMPTY,
            inferredType,
            null
        );
    }

    /**
     * Builds projection expressions for the final Project node:
     * all input fields followed by aggregate results, with casts if needed.
     *
     * @param join Join node.
     * @param win Original window node.
     * @param grp Window group.
     * @return List of projection expressions.
     */
    private static List<RexNode> buildProjection(RelNode join, LogicalWindow win, LogicalWindow.Group grp) {
        RexBuilder rexBuilder = win.getCluster().getRexBuilder();
        int inputFieldCnt = win.getInput().getRowType().getFieldCount();

        int grpKeyCnt = grp.keys.cardinality();
        int aggFieldCnt = grp.aggCalls.size();

        List<RexNode> projects = new ArrayList<>(inputFieldCnt + aggFieldCnt);

        for (int i = 0; i < inputFieldCnt; i++)
            projects.add(rexBuilder.makeInputRef(join, i));

        for (int i = 0; i < aggFieldCnt; i++) {
            RexNode ref = rexBuilder.makeInputRef(join, inputFieldCnt + grpKeyCnt + i);

            RelDataType targetType = windowAggType(win, i);

            if (!targetType.equals(ref.getType()))
                ref = rexBuilder.makeCast(targetType, ref);

            projects.add(ref);
        }

        return projects;
    }

    /**
     * Returns the expected type of the window aggregate from the window row type.
     *
     * @param win Window node.
     * @param aggIdx Aggregate index.
     * @return Aggregate result type.
     */
    private static RelDataType windowAggType(LogicalWindow win, int aggIdx) {
        int inputFieldCnt = win.getInput().getRowType().getFieldCount();
        return win.getRowType().getFieldList().get(inputFieldCnt + aggIdx).getType();
    }

    /** Rule configuration. */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    @Value.Immutable
    public interface Config extends RuleFactoryConfig<Config> {
        /** Default configuration. */
        Config DEFAULT = ImmutableIgniteLogicalWindowRewriteRule.Config.builder()
            .withRuleFactory(IgniteLogicalWindowRewriteRule::new)
            .withDescription("IgniteLogicalWindowRewriteRule: rewrites LogicalWindow to LogicalAggregate LogicalJoin LogicalProject")
            .withOperandSupplier(b -> b.operand(LogicalWindow.class).anyInputs())
            .build();

        /**
         * Returns the rule factory for this configuration.
         *
         * @return Rule factory.
         */
        @Override @Value.Default
        default java.util.function.Function<Config, RelOptRule> ruleFactory() {
            return IgniteLogicalWindowRewriteRule::new;
        }
    }
}
