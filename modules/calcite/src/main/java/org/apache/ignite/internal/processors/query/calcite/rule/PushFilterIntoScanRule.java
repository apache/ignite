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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Rule that pushes filter into the scan. This might be useful for index range scans.
 */
public class PushFilterIntoScanRule extends RelOptRule {

    public static final PushFilterIntoScanRule FILTER_INTO_SCAN =
        new PushFilterIntoScanRule(LogicalFilter.class, "IgniteFilterIntoScanRule");

    private PushFilterIntoScanRule(Class<? extends RelNode> clazz, String desc) {
        super(operand(clazz,
            operand(IgniteTableScan.class, none())),
            RelFactories.LOGICAL_BUILDER,
            desc);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        IgniteTableScan scan = call.rel(1);

        RexNode cond = filter.getCondition();

        // We need to replace RexInputRef with RexLocalRef because TableScan doesn't have inputs.
        RexVisitor<RexNode> inputRefReplacer = new InputRefReplacer();
        cond = cond.accept(inputRefReplacer);

        RexNode cond0 = scan.condition();

        if (cond0 != null) {
            ImmutableList<RexNode> nodes = RexUtil.flattenAnd(ImmutableList.of(cond0));
            if (nodes.contains(cond))
                return;

            RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
            cond = RexUtil.composeConjunction(rexBuilder, F.concat(true, cond, nodes));
        }

        call.transformTo(
            new IgniteTableScan(scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.indexName(), cond));
    }

    /** Visitor for replacing input refs to local refs. We need it for proper plan serialization. */
    private class InputRefReplacer extends RexShuttle {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            int idx = inputRef.getIndex();
            return new RexLocalRef(idx, inputRef.getType());
        }
    }
}
