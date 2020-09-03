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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitsAwareIgniteRel;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.hash;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * Relational expression that computes a set of
 * 'select expressions' from its input relational expression.
 */
public class IgniteProject extends Project implements TraitsAwareIgniteRel {
    /**
     * Creates a Project.
     *
     * @param cluster  Cluster that this relational expression belongs to
     * @param traits   Traits of this relational expression
     * @param input    Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType  Output row type
     */
    public IgniteProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    public IgniteProject(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
    }

    /** {@inheritDoc} */
    @Override public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new IgniteProject(getCluster(), traitSet, input, projects, rowType);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // The node is rewindable if its input is rewindable.

        RelTraitSet in = inputTraits.get(0);
        RewindabilityTrait rewindability = TraitUtils.rewindability(nodeTraits);

        return ImmutableList.of(Pair.of(nodeTraits, ImmutableList.of(in.replace(rewindability))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // All distribution types except hash distribution are propagated as is.
        // In case of hash distribution we need to project distribution keys.
        // In case one of distribution keys is erased by projection result distribution
        // becomes default single since we cannot calculate required input distribution.

        RelTraitSet in = inputTraits.get(0);
        IgniteDistribution distribution = TraitUtils.distribution(nodeTraits);

        if (distribution.getType() != HASH_DISTRIBUTED)
            return ImmutableList.of(Pair.of(nodeTraits, ImmutableList.of(in.replace(distribution))));

        Mappings.TargetMapping mapping = getPartialMapping(
            input.getRowType().getFieldCount(), getProjects());

        ImmutableIntList keys = distribution.getKeys();
        List<Integer> srcKeys = new ArrayList<>(keys.size());

        for (int key : keys) {
            int src = mapping.getSourceOpt(key);

            if (src == -1)
                break;

            srcKeys.add(src);
        }

        if (srcKeys.size() == keys.size()) {
            return ImmutableList.of(Pair.of(nodeTraits, ImmutableList.of(in.replace(hash(srcKeys, distribution.function())))));
        }

        return ImmutableList.of(Pair.of(nodeTraits.replace(single()), ImmutableList.of(in.replace(single()))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // The code below projects required collation. In case we cannot calculate required source collation
        // (e.g. one of required sorted fields is result of a function call), input and output collations are erased.

        RelTraitSet in = inputTraits.get(0);

        List<RelFieldCollation> fieldCollations = TraitUtils.collation(nodeTraits).getFieldCollations();

        if (fieldCollations.isEmpty())
            return ImmutableList.of(Pair.of(nodeTraits, ImmutableList.of(in.replace(RelCollations.EMPTY))));

        Map<Integer, Integer> targets = new HashMap<>();
        for (Ord<RexNode> project : Ord.zip(getProjects())) {
            if (project.e instanceof RexInputRef)
                targets.putIfAbsent(project.i, ((RexInputRef)project.e).getIndex());
        }

        List<RelFieldCollation> inFieldCollations = new ArrayList<>();
        for (RelFieldCollation inFieldCollation : fieldCollations) {
            Integer newIndex = targets.get(inFieldCollation.getFieldIndex());
            if (newIndex == null)
                break;
            else
                inFieldCollations.add(inFieldCollation.withFieldIndex(newIndex));
        }

        if (inFieldCollations.size() == fieldCollations.size())
            return ImmutableList.of(Pair.of(nodeTraits, ImmutableList.of(in.replace(RelCollations.of(inFieldCollations)))));

        return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY), ImmutableList.of(in.replace(RelCollations.EMPTY))));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // The node is rewindable if its input is rewindable.

        RelTraitSet in = inputTraits.get(0);
        RewindabilityTrait rewindability = TraitUtils.rewindability(in);

        return ImmutableList.of(Pair.of(nodeTraits.replace(rewindability), ImmutableList.of(in)));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // All distribution types except hash distribution are propagated as is.
        // In case of hash distribution we need to project distribution keys.
        // In case one of distribution keys is erased by projection result distribution
        // becomes random since we cannot determine where data is without erased key.

        RelTraitSet in = inputTraits.get(0);
        IgniteDistribution distribution = TraitUtils.distribution(in);

        if (distribution.getType() == HASH_DISTRIBUTED) {
            Mappings.TargetMapping mapping = Project.getPartialMapping(
                input.getRowType().getFieldCount(), getProjects());

            return ImmutableList.of(Pair.of(nodeTraits.replace(distribution.apply(mapping)), ImmutableList.of(in)));
        }

        return ImmutableList.of(Pair.of(nodeTraits.replace(distribution), ImmutableList.of(in)));
    }

    /** {@inheritDoc} */
    @Override public List<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(RelTraitSet nodeTraits, List<RelTraitSet> inputTraits) {
        // The code below projects input collation.

        RelTraitSet in = inputTraits.get(0);
        RelCollation collation = TraitUtils.collation(in);

        if (collation.getFieldCollations().isEmpty())
            return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.EMPTY), ImmutableList.of(in)));

        Map<Integer, Integer> targets = new HashMap<>();
        for (Ord<RexNode> project : Ord.zip(getProjects())) {
            if (project.e instanceof RexInputRef)
                targets.putIfAbsent(((RexInputRef)project.e).getIndex(), project.i);
        }

        List<RelFieldCollation> outFieldCollations = new ArrayList<>();
        for (RelFieldCollation inFieldCollation : collation.getFieldCollations()) {
            Integer newIndex = targets.get(inFieldCollation.getFieldIndex());
            if (newIndex != null)
                outFieldCollations.add(inFieldCollation.withFieldIndex(newIndex));
        }

        return ImmutableList.of(Pair.of(nodeTraits.replace(RelCollations.of(outFieldCollations)), ImmutableList.of(in)));
    }
}
