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
import org.apache.calcite.rex.RexUtil;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.builder;
import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.simplifier;

/**
 * Rule that pushes filter into the scan. This might be useful for index range scans.
 */
public class PushFilterIntoScanRule extends RelOptRule {
    /** Instance. */
    public static final PushFilterIntoScanRule FILTER_INTO_SCAN =
        new PushFilterIntoScanRule(LogicalFilter.class, "IgniteFilterIntoScanRule");

    /**
     * Constructor.
     *
     * @param clazz Class of relational expression to match.
     * @param desc Description, or null to guess description
     */
    private PushFilterIntoScanRule(Class<? extends RelNode> clazz, String desc) {
        super(operand(clazz,
            operand(IgniteIndexScan.class, none())),
            RelFactories.LOGICAL_BUILDER,
            desc);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        IgniteIndexScan scan = call.rel(1);

        RelOptCluster cluster = scan.getCluster();
        RelMetadataQuery mq = call.getMetadataQuery();

        RexNode cond = filter.getCondition();

        // Let's remove from the condition common with the scan filter parts.
        cond = simplifier(cluster)
            .withPredicates(mq.getPulledUpPredicates(scan))
            .simplifyUnknownAsFalse(cond);

        // We need to replace RexInputRef with RexLocalRef because TableScan doesn't have inputs.
        cond = cond.accept(new InputRefReplacer());

        // Combine the condition with the scan filter.
        cond = RexUtil.composeConjunction(builder(cluster), F.asList(cond, scan.condition()));

        call.transformTo(
            new IgniteIndexScan(cluster, scan.getTraitSet(), scan.getTable(), scan.indexName(), cond));
    }

    /** Visitor for replacing input refs to local refs. We need it for proper plan serialization. */
    private static class InputRefReplacer extends RexShuttle {
        @Override public RexNode visitInputRef(RexInputRef inputRef) {
            return new RexLocalRef(inputRef.getIndex(), inputRef.getType());
        }
    }
}
