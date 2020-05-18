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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Ignite Project converter.
 */
public class ProjectTraitsPropagationRule extends RelOptRule {
    /** */
    public static final RelOptRule INSTANCE = new ProjectTraitsPropagationRule();

    /**
     * Creates a converter.
     */
    public ProjectTraitsPropagationRule() {
        super(RuleUtils.traitPropagationOperand(IgniteProject.class));
    }

    @Override public void onMatch(RelOptRuleCall call) {
        IgniteProject rel = call.rel(0);
        RelNode input = call.rel(1);

        RelOptCluster cluster = rel.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        RelTraitSet traits = rel.getTraitSet()
            .replace(IgniteDistributions.project(mq, input, rel.getProjects()));

        if (call.getPlanner().getRelTraitDefs().contains(RelCollationTraitDef.INSTANCE)) {
            List<RelCollation> projCollations = RelMdCollation.project(mq, input, rel.getProjects());

            if (!F.isEmpty(projCollations))
                traits = traits.replace(projCollations.get(0));
        }

        RuleUtils.transformTo(call,
            new IgniteProject(cluster, traits, input, rel.getProjects(), rel.getRowType()));
    }
}
