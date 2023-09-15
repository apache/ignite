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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;

import static java.util.Objects.requireNonNull;

/** */
public class IgniteCollect extends Collect implements IgniteRel {
    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to.
     * @param traits Relation traits.
     * @param input Input relational expression.
     * @param rowType Row type.
     */
    public IgniteCollect(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        RelDataType rowType
    ) {
        super(cluster, traits, input, rowType);
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteCollect(RelInput input) {
        super(input.getCluster(), input.getTraitSet().replace(IgniteConvention.INSTANCE), input.getInput(),
            deriveRowType(input.getCluster().getTypeFactory(), input.getEnum("collectionType", SqlTypeName.class),
                requireNonNull(input.getString("field"), "field"), input.getInput().getRowType()));
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteCollect(cluster, getTraitSet(), sole(inputs), rowType);
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, RelNode input) {
        return new IgniteCollect(getCluster(), traitSet, input, rowType());
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("collectionType", getCollectionType());
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        if (required.getConvention() != IgniteConvention.INSTANCE)
            return null;

        if (TraitUtils.distribution(required) != IgniteDistributions.single())
            return null;

        if (TraitUtils.collation(required) != RelCollations.EMPTY)
            return null;

        return Pair.of(required, ImmutableList.of(required));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        if (childTraits.getConvention() != IgniteConvention.INSTANCE)
            return null;

        if (TraitUtils.distribution(childTraits) != IgniteDistributions.single())
            return null;

        return Pair.of(childTraits.replace(RelCollations.EMPTY), ImmutableList.of(childTraits));
    }
}
