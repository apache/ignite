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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.Accumulator;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.random;

/**
 *
 */
public class IgniteMapAggregate extends Aggregate implements IgniteRel {
    public IgniteMapAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    }

    /** */
    public IgniteMapAggregate(RelInput input) {
        super(Commons.changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new IgniteMapAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override protected RelDataType deriveRowType() {
        return rowType(getCluster().getTypeFactory());
    }

    /**
     * @return Map aggregate relational node distribution calculated on the basis of its input and groupingSets.
     * <b>Note</b> that the method returns {@code null} in case the given input cannot be processed in map-reduce
     * style by an aggregate.
     */
    public static IgniteDistribution distribution(IgniteDistribution inputDistr, ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {

        switch (inputDistr.getType()) {
            case SINGLETON:
            case BROADCAST_DISTRIBUTED:
                return inputDistr;

            case RANDOM_DISTRIBUTED:
            case HASH_DISTRIBUTED:
                return random(); // its OK to just erase distribution here

            default:
                throw new AssertionError("Unexpected distribution type.");
        }
    }

    /** */
    public static RelDataType rowType(RelDataTypeFactory typeFactory) {
        assert typeFactory instanceof IgniteTypeFactory;

        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

        builder.add("GROUP_ID", typeFactory.createJavaType(byte.class));
        builder.add("GROUP_KEY", typeFactory.createJavaType(GroupKey.class));
        builder.add("AGG_DATA", typeFactory.createArrayType(typeFactory.createJavaType(Accumulator.class), -1));

        return builder.build();
    }
}
