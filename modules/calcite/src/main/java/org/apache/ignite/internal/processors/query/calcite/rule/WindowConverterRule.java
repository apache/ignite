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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.window.WindowFunctions;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteWindow;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class WindowConverterRule extends AbstractIgniteConverterRule<LogicalWindow> {
    /** */
    public static final RelOptRule INSTANCE = new WindowConverterRule();

    /** */
    private WindowConverterRule() {
        super(LogicalWindow.class, "WindowConverterRule");
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalWindow window) {
        RelOptCluster cluster = window.getCluster();

        RelNode result = window.getInput();

        int inputFldCnt = result.getRowType().getFieldCount();
        RexShuttle restoreConstant = new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef inputRef) {
                int idx = inputRef.getIndex();
                if (idx < inputFldCnt)
                    return inputRef;
                else {
                    // Index above input field count reffers to window constant
                    idx -= inputFldCnt;
                    return window.constants.get(idx);
                }
            }
        };

        for (int grpIdx = 0; grpIdx < window.groups.size(); grpIdx++) {
            Window.Group grp = window.groups.get(grpIdx);

            RelCollation collation = mergeCollations(
                TraitUtils.createCollation(grp.keys.asList()),
                grp.collation()
            );

            RelTraitSet inTraits = cluster
                .traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(collation);

            RelTraitSet outTraits = cluster
                .traitSetOf(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(collation);

            result = convert(result, inTraits);

            // Add fields added by current group.
            // see org.apache.calcite.rel.logical.LogicalWindow#create
            String grpFieldPrefix = "w" + grpIdx + "$";
            List<RelDataTypeField> fieldsAddedByCurGrp = U.arrayList(window.getRowType().getFieldList(),
                it -> it.getName().startsWith(grpFieldPrefix));
            List<RelDataTypeField> grpFields = new ArrayList<>(result.getRowType().getFieldList());
            grpFields.addAll(fieldsAddedByCurGrp);

            RelRecordType rowType = new RelRecordType(grpFields);

            IgniteWindow.Group newGrp = convertGroup(grp, restoreConstant);

            result = new IgniteWindow(
                window.getCluster(),
                window.getTraitSet().merge(outTraits),
                result,
                rowType,
                newGrp,
                WindowFunctions.streamable(newGrp)
            );
        }

        return (PhysicalNode)result;
    }

    /**
     * Replaces:
     * - original agg call ordinal with sequential index within group;
     * - input ref to window constant with actual rex literal.
     */
    private static IgniteWindow.Group convertGroup(Window.Group grp, RexShuttle restoreConstant) {
        List<Window.RexWinAggCall> newAggCalls = new ArrayList<>(grp.aggCalls.size());
        ImmutableList<Window.RexWinAggCall> calls = grp.aggCalls;
        for (int i = 0; i < calls.size(); i++) {
            Window.RexWinAggCall aggCall = calls.get(i);

            Window.RexWinAggCall newCall = new Window.RexWinAggCall(
                (SqlAggFunction)aggCall.op,
                aggCall.type,
                aggCall.operands.stream()
                        .map(it -> it.accept(restoreConstant))
                        .collect(Collectors.toList()),
                i,
                aggCall.distinct,
                aggCall.ignoreNulls
            );
            newAggCalls.add(newCall);
        }

        return new IgniteWindow.Group(
            grp.keys,
            grp.isRows,
            grp.lowerBound.accept(restoreConstant),
            grp.upperBound.accept(restoreConstant),
            grp.exclude,
            grp.orderKeys,
            newAggCalls
        );
    }

    /**
     * Merges provided collation is sinle one.
     *
     * @param collation0 First collation
     * @param collation1 Second collation
     * @return New collation
     */
    public static RelCollation mergeCollations(RelCollation collation0, RelCollation collation1) {
        ImmutableBitSet keys = ImmutableBitSet.of(collation0.getKeys());
        List<RelFieldCollation> fields = U.arrayList(collation0.getFieldCollations());
        for (RelFieldCollation it : collation1.getFieldCollations()) {
            if (!keys.get(it.getFieldIndex()))
                fields.add(it);
        }
        return RelCollations.of(fields);
    }
}
