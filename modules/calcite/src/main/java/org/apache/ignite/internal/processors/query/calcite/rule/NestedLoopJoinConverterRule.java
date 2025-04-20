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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdCumulativeCost;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;

/**
 * Ignite Join converter.
 */
public class NestedLoopJoinConverterRule extends AbstractIgniteJoinConverterRule {
    /** */
    public static final RelOptRule INSTANCE = new NestedLoopJoinConverterRule();

    /**
     * Creates a converter.
     */
    public NestedLoopJoinConverterRule() {
        super("NestedLoopJoinConverter", HintDefinition.NL_JOIN);
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalJoin rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet outTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet leftInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelTraitSet rightInTraits = cluster.traitSetOf(IgniteConvention.INSTANCE);
        RelNode left = convert(rel.getLeft(), leftInTraits);
        RelNode right = convert(rel.getRight(), rightInTraits);

        IgniteNestedLoopJoin res = new IgniteNestedLoopJoin(cluster, outTraits, left, right, rel.getCondition(), rel.getVariablesSet(), rel.getJoinType());

//        if ((res.getRight() instanceof IgniteExchange) && res.getRight().getInput(0) instanceof TableScan
//            && RelOptUtil.toString(res.getRight().getInput(0)).contains("table=[[PUBLIC, REGION]]"))
//            IgniteMdCumulativeCost.printCost(res, res.computeSelfCost(planner, mq), mq);

//        System.err.println("TEST | " + RelOptUtil.toString(res));

        return res;
    }
}
