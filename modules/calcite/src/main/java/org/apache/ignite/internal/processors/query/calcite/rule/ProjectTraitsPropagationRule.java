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
import java.util.HashSet;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;

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
        super(operand(IgniteProject.class, operand(RelSubset.class, any())));
    }

    @Override public void onMatch(RelOptRuleCall call) {
        IgniteProject proto = call.rel(0);
        RelSubset inputSubset = call.rel(1);

        RelOptCluster cluster = proto.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        List<RelNode> subsetNodes = inputSubset.getRelList();
        HashSet<RelTraitSet> processed = new HashSet<>(subsetNodes.size());

        List<RelNode> newRels = new ArrayList<>(subsetNodes.size());

        for (RelNode subsetNode : subsetNodes) {
            RelTraitSet inputTraits = subsetNode.getTraitSet();

            if (!processed.add(inputTraits))
                continue;

            RelNode input = convert(inputSubset, inputTraits);
            RelTraitSet traits = proto.getTraitSet()
                .replace(IgniteMdDistribution.project(mq, input, proto.getProjects()));

            newRels.add(new IgniteProject(cluster, traits, input, proto.getProjects(), proto.getRowType()));
        }

        RuleUtils.transformTo(call, newRels);
    }
}
