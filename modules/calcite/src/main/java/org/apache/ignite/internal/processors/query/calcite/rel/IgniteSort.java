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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.fixTraits;

/**
 * Ignite sort operator.
 */
public class IgniteSort extends Sort implements IgniteRel {
    /**
     * Constructor.
     *
     * @param cluster Cluster.
     * @param traits Trait set.
     * @param child Input node.
     * @param collation Collation.
     * @param offset Offset.
     * @param fetch Limit.
     */
    public IgniteSort(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation,
        RexNode offset,
        RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    public IgniteSort(RelInput input) {
        super(Commons.changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public Sort copy(
        RelTraitSet traitSet,
        RelNode newInput,
        RelCollation newCollation,
        RexNode offset,
        RexNode fetch) {
        return new IgniteSort(getCluster(), traitSet, newInput,newCollation, offset, fetch);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        required = fixTraits(required);

        RelCollation collation = Commons.collation(required);

        if (!collation().satisfies(collation))
            return null;

        return Pair.of(required.replace(collation()), ImmutableList.of(required.replace(RelCollations.EMPTY)));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        childTraits = fixTraits(childTraits);

        return Pair.of(childTraits.replace(collation()), ImmutableList.of(childTraits.replace(RelCollations.EMPTY)));
    }
}
