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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.agg.IgniteSingleHashAggregate;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.HintUtils;

/**
 *
 */
public class HashAggregateConverterRule {
    /** */
    public static final RelOptRule SINGLE = new HashSingleAggregateConverterRule();

    /** */
    public static final RelOptRule MAP_REDUCE = new HashMapReduceAggregateConverterRule();

    /** */
    private HashAggregateConverterRule() {
        // No-op.
    }

    /** */
    private static class HashSingleAggregateConverterRule extends AbstractIgniteConverterRule<LogicalAggregate> {
        /** */
        HashSingleAggregateConverterRule() {
            super(LogicalAggregate.class, "HashSingleAggregateConverterRule");
        }

        /** {@inheritDoc} */
        @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq,
            LogicalAggregate agg) {
            if (HintUtils.isExpandDistinctAggregate(agg))
                return null;

            RelOptCluster cluster = agg.getCluster();
            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(IgniteDistributions.single());
            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE).replace(IgniteDistributions.single());
            RelNode input = convert(agg.getInput(), inTrait);

            return new IgniteSingleHashAggregate(
                cluster,
                outTrait,
                input,
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList()
            );
        }
    }

    /** */
    private static class HashMapReduceAggregateConverterRule extends AbstractIgniteConverterRule<LogicalAggregate> {
        /** */
        HashMapReduceAggregateConverterRule() {
            super(LogicalAggregate.class, "HashMapReduceAggregateConverterRule");
        }

        /** {@inheritDoc} */
        @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq,
            LogicalAggregate agg) {
            if (HintUtils.isExpandDistinctAggregate(agg))
                return null;

            RelOptCluster cluster = agg.getCluster();
            RelTraitSet inTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
            RelTraitSet outTrait = cluster.traitSetOf(IgniteConvention.INSTANCE);
            RelNode input = convert(agg.getInput(), inTrait);

            RelNode map = new IgniteMapHashAggregate(
                cluster,
                outTrait,
                input,
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList()
            );

            return new IgniteReduceHashAggregate(
                cluster,
                outTrait.replace(IgniteDistributions.single()),
                convert(map, inTrait.replace(IgniteDistributions.single())),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList(),
                agg.getRowType()
            );
        }
    }
}
