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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

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
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughRewindability(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, in = pair.right.get(0);
            RewindabilityTrait rewindability = TraitUtils.rewindability(out);

            traits0.add(Pair.of(out, ImmutableList.of(in.replace(rewindability))));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughDistribution(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, in = pair.right.get(0);
            IgniteDistribution distribution = TraitUtils.distribution(out);

            if (distribution.getType() == HASH_DISTRIBUTED) {
                Mappings.TargetMapping mapping = getPartialMapping(
                    input.getRowType().getFieldCount(), getProjects());

                List<Integer> keys = new ArrayList<>(distribution.getKeys().size());

                boolean mapped = true;
                for (int key : distribution.getKeys()) {
                    int src = mapping.getSourceOpt(key);

                    if (src == -1) {
                        mapped = false;

                        break;
                    }

                    keys.add(src);
                }

                if (mapped)
                    traits0.add(Pair.of(out, ImmutableList.of(in.replace(hash(keys, distribution.function())))));
                else
                    traits0.add(Pair.of(out.replace(single()), ImmutableList.of(in.replace(single()))));
            }
            else
                traits0.add(Pair.of(out, ImmutableList.of(in.replace(distribution))));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> passThroughCollation(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, in = pair.right.get(0);
            RelCollation collation = TraitUtils.collation(out);

            if (collation.getFieldCollations().isEmpty()) {
                traits0.add(Pair.of(out, ImmutableList.of(in.replace(RelCollations.EMPTY))));

                continue;
            }

            Map<Integer, Integer> targets = new HashMap<>();
            for (Ord<RexNode> project : Ord.zip(getProjects())) {
                if (project.e instanceof RexInputRef)
                    targets.putIfAbsent(project.i, ((RexInputRef)project.e).getIndex());
            }

            List<RelFieldCollation> inFieldCollations = new ArrayList<>();
            for (RelFieldCollation inFieldCollation : collation.getFieldCollations()) {
                Integer newIndex = targets.get(inFieldCollation.getFieldIndex());
                if (newIndex != null)
                    inFieldCollations.add(inFieldCollation.withFieldIndex(newIndex));
                else {
                    out = out.replace(RelCollations.EMPTY);
                    inFieldCollations = Collections.emptyList();

                    break;
                }
            }

            traits0.add(Pair.of(out, ImmutableList.of(in.replace(RelCollations.of(inFieldCollations)))));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveRewindability(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, in = pair.right.get(0);
            RewindabilityTrait rewindability = TraitUtils.rewindability(in);

            traits0.add(Pair.of(out.replace(rewindability), ImmutableList.of(in)));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveDistribution(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, in = pair.right.get(0);
            IgniteDistribution distribution = TraitUtils.distribution(in);

            if (distribution.getType() == HASH_DISTRIBUTED) {
                Mappings.TargetMapping mapping = Project.getPartialMapping(
                    input.getRowType().getFieldCount(), getProjects());

                traits0.add(Pair.of(out.replace(distribution.apply(mapping)), ImmutableList.of(in)));
            }
            else
                traits0.add(Pair.of(out.replace(distribution), ImmutableList.of(in)));
        }

        return traits0;
    }

    /** {@inheritDoc} */
    @Override public Collection<Pair<RelTraitSet, List<RelTraitSet>>> deriveCollation(
        Collection<Pair<RelTraitSet, List<RelTraitSet>>> traits) {
        HashSet<Pair<RelTraitSet, List<RelTraitSet>>> traits0 = U.newHashSet(traits.size());

        for (Pair<RelTraitSet, List<RelTraitSet>> pair : traits) {
            RelTraitSet out = pair.left, in = pair.right.get(0);
            RelCollation collation = TraitUtils.collation(in);

            if (collation.getFieldCollations().isEmpty()) {
                traits0.add(Pair.of(out.replace(RelCollations.EMPTY), ImmutableList.of(in)));

                continue;
            }

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

            traits0.add(Pair.of(out.replace(RelCollations.of(outFieldCollations)), ImmutableList.of(in)));
        }

        return traits0;
    }
}
