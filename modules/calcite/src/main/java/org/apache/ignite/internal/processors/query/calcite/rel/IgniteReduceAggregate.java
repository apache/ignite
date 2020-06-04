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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

/**
 *
 */
public class IgniteReduceAggregate extends SingleRel implements IgniteRel {
    /** */
    private final ImmutableBitSet groupSet;

    /** */
    private final List<ImmutableBitSet> groupSets;

    /** */
    private final List<AggregateCall> aggCalls;

    /** */
    public IgniteReduceAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls, RelDataType rowType) {
        super(cluster, traits, input);

        assert rowType != null;
        assert RelOptUtil.areRowTypesEqual(input.getRowType(),
            IgniteMapAggregate.rowType(getCluster().getTypeFactory()), true);
        this.groupSet = groupSet;
        if (groupSets == null)
            groupSets = ImmutableList.of(groupSet);
        this.groupSets = groupSets;
        this.aggCalls = aggCalls;
        this.rowType = rowType;
    }

    /** */
    public IgniteReduceAggregate(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInput(),
            input.getBitSet("group"),
            input.getBitSetList("groups"),
            input.getAggregateCalls("aggs"),
            input.getRowType("rowType"));
    }

    /** {@inheritDoc} */
    @Override protected RelDataType deriveRowType() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteReduceAggregate(getCluster(), traitSet, sole(inputs), groupSet, groupSets, aggCalls, rowType);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw)
            .itemIf("rowType", rowType, pw.getDetailLevel() == SqlExplainLevel.ALL_ATTRIBUTES)
            .item("group", groupSet)
            .itemIf("groups", groupSets, Group.induce(groupSet, groupSets) != Group.SIMPLE)
            .itemIf("aggs", aggCalls, pw.nest());

        if (!pw.nest()) {
            for (Ord<AggregateCall> ord : Ord.zip(aggCalls))
                pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
        }

        return pw;
    }

    public ImmutableBitSet groupSet() {
        return groupSet;
    }

    public List<ImmutableBitSet> groupSets() {
        return groupSets;
    }

    public List<AggregateCall> aggregateCalls() {
        return aggCalls;
    }
}
