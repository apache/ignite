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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
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
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.fixTraits;

/**
 * Relational expression that computes a set of
 * 'select expressions' from its input relational expression.
 */
public class IgniteProject extends Project implements IgniteRel {
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
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        required = fixTraits(required);

        IgniteDistribution distr;
        RelCollation collation;

        if ((distr = inDistribution(TraitUtils.distribution(required))) == null)
            return passThroughTraits(required.replace(IgniteDistributions.single()));

        if ((collation = inCollation(TraitUtils.collation(required))) == null)
            return passThroughTraits(required.replace(RelCollations.EMPTY));

        return Pair.of(required, ImmutableList.of(required.replace(distr).replace(collation)));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        childTraits = fixTraits(childTraits);

        IgniteDistribution distr = outDistribution(TraitUtils.distribution(childTraits));
        RelCollation collation = outCollation(TraitUtils.collation(childTraits));

        return Pair.of(childTraits.replace(distr).replace(collation), ImmutableList.of(childTraits));
    }

    /** */
    private IgniteDistribution outDistribution(IgniteDistribution inDistr) {
        if (inDistr.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            Mappings.TargetMapping mapping = Project.getPartialMapping(
                input.getRowType().getFieldCount(), getProjects());

            return inDistr.apply(mapping);
        }

        return inDistr;
    }

    /** */
    private IgniteDistribution inDistribution(IgniteDistribution outDistr) {
        if (outDistr.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            Mappings.TargetMapping mapping = Project.getPartialMapping(
                input.getRowType().getFieldCount(), getProjects());

            List<Integer> inKeys = new ArrayList<>(outDistr.getKeys().size());

            for (int key : outDistr.getKeys()) {
                int src = mapping.getSourceOpt(key);
                if (src == -1)
                    return null;

                inKeys.add(src);
            }

            return IgniteDistributions.hash(inKeys);
        }

        return outDistr;
    }

    /** */
    private RelCollation outCollation(RelCollation inCollation) {
        if (inCollation.getFieldCollations().isEmpty())
            return RelCollations.EMPTY;

        Map<Integer, Integer> targets = new HashMap<>();
        for (Ord<RexNode> project : Ord.zip(getProjects())) {
            if (project.e instanceof RexInputRef)
                targets.putIfAbsent(((RexInputRef)project.e).getIndex(), project.i);
        }

        List<RelFieldCollation> outFieldCollations = new ArrayList<>();
        for (RelFieldCollation inFieldCollation : inCollation.getFieldCollations()) {
            Integer newIndex = targets.get(inFieldCollation.getFieldIndex());
            if (newIndex != null)
                outFieldCollations.add(inFieldCollation.withFieldIndex(newIndex));
        }

        return RelCollations.of(outFieldCollations);
    }

    /** */
    private RelCollation inCollation(RelCollation outCollation) {
        if (outCollation.getFieldCollations().isEmpty())
            return RelCollations.EMPTY;

        Map<Integer, Integer> targets = new HashMap<>();
        for (Ord<RexNode> project : Ord.zip(getProjects())) {
            if (project.e instanceof RexInputRef)
                targets.putIfAbsent(project.i, ((RexInputRef)project.e).getIndex());
        }

        List<RelFieldCollation> inFieldCollations = new ArrayList<>();
        for (RelFieldCollation inFieldCollation : outCollation.getFieldCollations()) {
            Integer newIndex = targets.get(inFieldCollation.getFieldIndex());
            if (newIndex == null)
                return null;

            inFieldCollations.add(inFieldCollation.withFieldIndex(newIndex));
        }

        return RelCollations.of(inFieldCollations);
    }
}
