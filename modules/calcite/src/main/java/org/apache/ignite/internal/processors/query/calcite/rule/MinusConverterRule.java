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
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteMapMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteReduceMinus;
import org.apache.ignite.internal.processors.query.calcite.rel.set.IgniteSingleMinus;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;

/**
 * MINUS (EXCEPT) operation converter rule.
 */
public class MinusConverterRule {
    /**
     *
     */
    public static final RelOptRule SINGLE = new SingleMinusConverterRule();

    /**
     *
     */
    public static final RelOptRule MAP_REDUCE = new MapReduceMinusConverterRule();

    /**
     *
     */
    private MinusConverterRule() {
        // No-op.
    }

    /**
     *
     */
    private static class SingleMinusConverterRule extends AbstractIgniteConverterRule<LogicalMinus> {
        /**
         *
         */
        SingleMinusConverterRule() {
            super(LogicalMinus.class, "SingleMinusConverterRule");
        }

        /** {@inheritDoc} */
        @Override
        protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalMinus setOp) {
            RelOptCluster cluster = setOp.getCluster();
            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(IgniteDistributions.single());
            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(IgniteDistributions.single());
            List<RelNode> inputs = Util.transform(setOp.getInputs(), rel -> convert(rel, inTrait));

            return new IgniteSingleMinus(cluster, outTrait, inputs, setOp.all);
        }
    }

    /**
     *
     */
    private static class MapReduceMinusConverterRule extends AbstractIgniteConverterRule<LogicalMinus> {
        /**
         *
         */
        MapReduceMinusConverterRule() {
            super(LogicalMinus.class, "MapReduceMinusConverterRule");
        }

        /** {@inheritDoc} */
        @Override
        protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalMinus setOp) {
            RelOptCluster cluster = setOp.getCluster();
            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
            List<RelNode> inputs = Util.transform(setOp.getInputs(), rel -> convert(rel, inTrait));

            RelNode map = new IgniteMapMinus(cluster, outTrait, inputs, setOp.all);

            return new IgniteReduceMinus(
                    cluster,
                    outTrait.replace(IgniteDistributions.single()),
                    convert(map, inTrait.replace(IgniteDistributions.single())),
                    setOp.all,
                    cluster.getTypeFactory().leastRestrictive(Util.transform(inputs, RelNode::getRowType))
            );
        }
    }
}
