/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexLiteral;
import org.immutables.value.Value;

/**
 * Rule that eliminates table modify node if it doesn't have any source rows.
 */
@Value.Enclosing
public class PruneTableModifyRule extends RelRule<PruneTableModifyRule.Config> implements SubstitutionRule {
    /** */
    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    /**
     * Constructor.
     *
     * @param config Rule configuration.
     */
    private PruneTableModifyRule(PruneTableModifyRule.Config config) {
        super(config);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        TableModify singleRel = call.rel(0);

        // TODO https://issues.apache.org/jira/browse/IGNITE-23512: Default Calcite RexBuilder ignores field type and extract type from
        //  the given value. E.g. for zero value RexBuilder creates INT literal. Use simple way create `singleValue` after fixing the issue.
        // RelNode singleValue = call.builder().values(singleRel.getRowType(), 0L).build();
        RexLiteral zeroLiteral =
            singleRel.getCluster().getRexBuilder().makeLiteral(0L, singleRel.getRowType().getFieldList().get(0).getType());
        RelNode singleVal = call.builder().values(List.of(List.of(zeroLiteral)), singleRel.getRowType()).build();

        singleVal = singleVal.copy(singleRel.getCluster().traitSet(), Collections.emptyList());
        call.transformTo(singleVal);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RuleFactoryConfig<Config> {
        /** */
        Config DEFAULT = ImmutablePruneTableModifyRule.Config.builder()
            .withDescription("PruneTableModify")
            .withRuleFactory(PruneTableModifyRule::new)
            .withOperandSupplier(b0 ->
                b0.operand(TableModify.class).oneInput(b1 ->
                    b1.operand(Values.class).predicate(Values::isEmpty).noInputs()))
            .build();
    }
}
