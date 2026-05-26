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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.internal.util.collection.BitSetIntSet;
import org.immutables.value.Value;

/**
 * A rule to process window rel with constants.
 * If window has agg call, which uses constants, then split window to:
 * - project with constants;
 * - window without constants;
 * - project removing constants.
 * Overwise just replace input refs in window bounds with constant values,
 */
@Value.Enclosing
public class WindowConstantsRule extends RelRule<WindowConstantsRule.Config> implements TransformationRule {
    /**  */
    public static final WindowConstantsRule INSTANCE = new WindowConstantsRule(WindowConstantsRule.Config.DEFAULT);

    /**  */
    private WindowConstantsRule(Config cfg) {
        super(cfg);
    }

    /**  */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalWindow window = call.rel(0);

        assert !window.constants.isEmpty();

        RelNode input = window.getInput();

        int[] constRefs = collectConstantsFromAggCalls(window);
        if (constRefs.length == 0) {
            // Agg calls does not use constants, so only group bounds should be processed.
            RelNode newWindow = buildWindowWithoutConstants(window, input, input, constRefs);
            call.transformTo(newWindow);
        }
        else {
            // Project constants, used in agg calls, replace agg call constant operands with new input refs,
            // and remove constants from result.
            RelNode projectWithConstants = buildProjectWithConstants(input, window.constants, constRefs);
            RelNode newWindow = buildWindowWithoutConstants(window, input, projectWithConstants, constRefs);
            RelNode projectWithoutConstants = buildProjectExcludeConstants(window, input, newWindow, constRefs);

            assert window.getRowType().equalsSansFieldNames(projectWithoutConstants.getRowType());
            call.transformTo(projectWithoutConstants);
        }
    }

    /** Creates projection with window constants. */
    private RelNode buildProjectWithConstants(RelNode input, List<RexLiteral> consts, int[] constRefs) {
        int inputFldCnt = input.getRowType().getFieldCount();
        RelBuilder relBldr = relBuilderFactory.create(input.getCluster(), null);
        relBldr.push(input);
        for (int idx : constRefs)
            relBldr.projectPlus(consts.get(idx - inputFldCnt));
        return relBldr.build();
    }

    /** Creates new window without constants. */
    private RelNode buildWindowWithoutConstants(LogicalWindow window, RelNode input, RelNode newInput, int[] constRefs) {
        int windowFldCnt = window.getRowType().getFieldCount();
        int inputFldCnt = input.getRowType().getFieldCount();

        List<RelDataTypeField> newInputFlds = newInput.getRowType().getFieldList();
        List<RelDataTypeField> windowFlds = window.getRowType().getFieldList();

        assert inputFldCnt <= newInputFlds.size();

        RelDataTypeFactory typeFactory = window.getCluster().getTypeFactory();
        RelDataTypeFactory.Builder builder = typeFactory.builder();

        for (int i = 0; i < inputFldCnt; i++) {
            // Add fields from original input, passed through window rel.
            builder.add(windowFlds.get(i));
        }
        for (int i = inputFldCnt; i < newInputFlds.size(); i++) {
            // Add constants from new input.
            builder.add(newInputFlds.get(i));
        }
        for (int i = inputFldCnt; i < windowFlds.size(); i++) {
            // Add fields, provided by window.
            builder.add(windowFlds.get(i));
        }

        RelDataType type = builder.build();
        assert type.getFieldCount() == newInputFlds.size() + (windowFldCnt - inputFldCnt);

        RexShuttle constToInputRefTransform = new ConstantRefToInputRefTransformation(inputFldCnt, constRefs, newInputFlds);
        RexShuttle constToValTransform = new ConstantRefToConstantValueTransformation(inputFldCnt, window.getConstants());

        // Replace input refs to constants:
        // - to new input ref in rex agg call;
        // - to actual constant value in group bounds, thus it lets reuse calculated frame bound for the same peer.
        // Also replaces origial agg call ordinal with sequential index within group.
        ImmutableList.Builder<Group> newGrps = ImmutableList.builder();
        for (Group grp : window.groups) {
            Group newGrp = new Group(
                grp.keys,
                grp.isRows,
                grp.lowerBound.accept(constToValTransform),
                grp.upperBound.accept(constToValTransform),
                grp.exclude,
                grp.orderKeys,
                traverseAggregateCalls(grp.aggCalls, constToInputRefTransform)
            );
            newGrps.add(newGrp);
        }

        // Agg calls in the original window allready reference fields by index,
        // do not need to remap it.
        return LogicalWindow.create(window.getTraitSet(), newInput, ImmutableList.of(), type, newGrps.build());
    }

    /** Creates projection without window constants. */
    private RelNode buildProjectExcludeConstants(LogicalWindow window, RelNode input, RelNode newWindow, int[] constRefs) {
        int inputFldCnt = input.getRowType().getFieldCount();
        int windowFldCnt = window.getRowType().getFieldCount();
        int constantCnt = constRefs.length;

        RexBuilder rexBuilder = window.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>(windowFldCnt);
        for (int i = 0; i < inputFldCnt; i++)
            projects.add(rexBuilder.makeInputRef(newWindow, i));
        for (int i = inputFldCnt; i < windowFldCnt; i++)
            projects.add(rexBuilder.makeInputRef(newWindow, i + constantCnt));

        RelBuilder relBldr = relBuilderFactory.create(newWindow.getCluster(), null);
        relBldr.push(newWindow);
        relBldr.project(projects);
        return relBldr.build();
    }

    /** Collects constants, used in aggregates call. Result is sorted array of used constants indicies. */
    private int[] collectConstantsFromAggCalls(LogicalWindow window) {
        int inputFldCnt = window.getInput().getRowType().getFieldCount();
        int constantCnt = window.constants.size();

        ConstantRefCollector collector = new ConstantRefCollector(inputFldCnt, constantCnt);
        window.groups.forEach(grp ->
            grp.aggCalls.forEach(aggCall ->
                aggCall.accept(collector)));

        // BitSetIntSet returns sorted array of contained values due to nature of bit set.
        return collector.used.toIntArray();
    }

    /** Replaces {@link RexWinAggCall} operands constant using provided transformation. */
    private ImmutableList<RexWinAggCall> traverseAggregateCalls(List<RexWinAggCall> aggCalls, RexShuttle visitor) {
        ImmutableList.Builder<RexWinAggCall> builder = ImmutableList.builderWithExpectedSize(aggCalls.size());
        for (RexWinAggCall call : aggCalls) {
            List<RexNode> newOperands = call.getOperands().stream()
                .map(arg -> arg.accept(visitor))
                .collect(Collectors.toList());
            RexWinAggCall newCall = new RexWinAggCall(
                (SqlAggFunction)call.getOperator(),
                call.getType(),
                newOperands,
                call.ordinal,
                call.distinct,
                call.ignoreNulls
            );
            builder.add(newCall);
        }

        return builder.build();
    }

    /** Collects constants, used in visited {@link RexNode}. */
    private static final class ConstantRefCollector extends RexShuttle {

        /** */
        private final int inputFldCnt;

        /** */
        private final BitSetIntSet used;

        /** */
        ConstantRefCollector(int inputFldCnt, int constantCnt) {
            this.inputFldCnt = inputFldCnt;
            used = new BitSetIntSet(constantCnt);
        }

        /** {@inheritDoc} */
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            int idx = inputRef.getIndex();
            if (idx >= inputFldCnt)
                used.add(idx);
            return super.visitInputRef(inputRef);
        }
    }

    /** Replaces constant references to new input fields references. */
    private static final class ConstantRefToInputRefTransformation extends RexShuttle {
        /** */
        private final int inputFldCnt;

        /** */
        private final int[] constRefs;

        /** */
        private final List<RelDataTypeField> newInputFlds;

        /** */
        ConstantRefToInputRefTransformation(int inputFldCnt, int[] constRefs, List<RelDataTypeField> newInputFlds) {
            this.inputFldCnt = inputFldCnt;
            this.constRefs = constRefs;
            this.newInputFlds = newInputFlds;
        }

        /** {@inheritDoc} */
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            int idx = inputRef.getIndex();
            if (idx >= inputFldCnt) {
                int refIdx = Arrays.binarySearch(constRefs, idx) + inputFldCnt;
                assert refIdx >= 0 && refIdx < newInputFlds.size();
                return RexInputRef.of(refIdx, newInputFlds);
            }

            return super.visitInputRef(inputRef);
        }
    }

    /** Replaces constant references to actual const value. */
    private static final class ConstantRefToConstantValueTransformation extends RexShuttle {
        /** */
        private final int inputFldCnt;

        /** */
        private final List<RexLiteral> consts;

        /** */
        ConstantRefToConstantValueTransformation(int inputFldCnt, List<RexLiteral> consts) {
            this.inputFldCnt = inputFldCnt;
            this.consts = consts;
        }

        /** {@inheritDoc} */
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            int idx = inputRef.getIndex();
            if (idx >= inputFldCnt)
                return consts.get(idx - inputFldCnt);
            return super.visitInputRef(inputRef);
        }
    }


    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        /**  */
        WindowConstantsRule.Config DEFAULT = ImmutableWindowConstantsRule.Config.of()
            .withOperandSupplier(b -> b.operand(LogicalWindow.class)
                .predicate(it -> !it.constants.isEmpty())
                .anyInputs());

        /** {@inheritDoc} */
        @Override default WindowConstantsRule toRule() {
            return INSTANCE;
        }
    }
}
