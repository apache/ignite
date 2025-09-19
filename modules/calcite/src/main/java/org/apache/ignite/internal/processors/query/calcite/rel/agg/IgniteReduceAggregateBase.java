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

package org.apache.ignite.internal.processors.query.calcite.rel.agg;

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public abstract class IgniteReduceAggregateBase extends SingleRel implements TraitsAwareIgniteRel {
    /** */
    protected final ImmutableBitSet groupSet;

    /** */
    protected final List<ImmutableBitSet> groupSets;

    /** */
    protected final List<AggregateCall> aggCalls;

    /** */
    protected IgniteReduceAggregateBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        RelDataType rowType
    ) {
        super(cluster, traits, input);

        assert rowType != null;
        this.groupSet = groupSet;
        if (groupSets == null)
            groupSets = ImmutableList.of(groupSet);
        this.groupSets = groupSets;
        this.aggCalls = aggCalls;
        this.rowType = rowType;
    }

    /** */
    protected IgniteReduceAggregateBase(RelInput input) {
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

    /** */
    public ImmutableBitSet getGroupSet() {
        return groupSet;
    }

    /** */
    public List<ImmutableBitSet> getGroupSets() {
        return groupSets;
    }

    /** */
    public List<AggregateCall> getAggregateCalls() {
        return aggCalls;
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughDistribution(RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits) {
        if (TraitUtils.distribution(nodeTraits) == IgniteDistributions.single())
            return Pair.of(nodeTraits, Commons.transform(inTraits, t -> t.replace(IgniteDistributions.single())));

        return null;
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inputTraits
    ) {
        RelTraitSet in = inputTraits.get(0);

        return ImmutableList.of(
            Pair.of(
                nodeTraits.replace(IgniteDistributions.single()),
                ImmutableList.of(in.replace(IgniteDistributions.single()))
            )
        );
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCorrelation(
        RelTraitSet nodeTraits,
        List<RelTraitSet> inTraits
    ) {
        return ImmutableList.of(Pair.of(nodeTraits.replace(TraitUtils.correlation(inTraits.get(0))),
            inTraits));
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        // Reduce aggregate doesn't change result's row count until we don't use
        // cluster parallelism at the model (devide source rows by nodes for partitioned data).
        return mq.getRowCount(getInput());
    }
}
