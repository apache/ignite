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

import java.util.Collections;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Uncollect} in {@link IgniteConvention Ignite calling convention}.
 */
public class IgniteUncollect extends Uncollect implements IgniteRel {
    /**
     * Creates an Uncollect relational operator.
     */
    public IgniteUncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, boolean withOrdinality) {
        super(cluster, traitSet, child, withOrdinality, Collections.emptyList());
    }

    /** */
    public IgniteUncollect(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /**
     * Creates an IgniteUncollect.
     *
     * <p>Each field of the input relational expression must be an array or multiset.
     *
     * @param traitSet Trait set
     * @param input    Input relational expression
     * @param withOrdinality Whether output should contain an ORDINALITY column
     */
    public static IgniteUncollect create(RelTraitSet traitSet, RelNode input, boolean withOrdinality) {
        RelOptCluster cluster = input.getCluster();

        return new IgniteUncollect(cluster, traitSet, input, withOrdinality);
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, RelNode input) {
        return new IgniteUncollect(getCluster(), traitSet, input, withOrdinality);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteUncollect(cluster, getTraitSet(), sole(inputs), withOrdinality);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        int fieldCnt = getInput().getRowType().getFieldCount();

        // Assume every field contains collection of 4 elements (to simplify calculation).
        long rowsMultiplier = fieldCnt > 15 ? Integer.MAX_VALUE : (1L << (fieldCnt * 2));

        return rowsMultiplier * mq.getRowCount(getInput());
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        if (required.getConvention() != IgniteConvention.INSTANCE)
            return null;

        if (TraitUtils.collation(required) != RelCollations.EMPTY)
            return null;

        if (TraitUtils.distribution(required).getType() == RelDistribution.Type.HASH_DISTRIBUTED)
            return null;

        return Pair.of(required, ImmutableList.of(required));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        if (childTraits.getConvention() != IgniteConvention.INSTANCE)
            return null;

        RelTraitSet traits = childTraits.replace(RelCollations.EMPTY);

        if (TraitUtils.distribution(traits).getType() == RelDistribution.Type.HASH_DISTRIBUTED)
            traits = traits.replace(IgniteDistributions.random());

        return Pair.of(traits, ImmutableList.of(childTraits));
    }
}
