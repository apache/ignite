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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.any;

/**
 *
 */
public class IgniteUnionAll extends Union implements IgniteRel {
    /** */
    public IgniteUnionAll(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
        super(cluster, traits, inputs, true);
    }

    /** */
    public IgniteUnionAll(RelInput input) {
        this(
            input.getCluster(),
            input.getTraitSet().replace(IgniteConvention.INSTANCE),
            input.getInputs());
    }

    /** {@inheritDoc} */
    @Override public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        assert all;

        return new IgniteUnionAll(getCluster(), traitSet, inputs);
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelNode passThrough(RelTraitSet required) {
        IgniteDistribution toDistr = TraitUtils.distribution(required);

        // Union erases collation and only distribution trait can be passed through.
        // So that, it's no use to pass ANY distribution.
        if (toDistr == any())
            return null;

        RelTraitSet traits = getCluster().traitSetOf(IgniteConvention.INSTANCE)
            .replace(toDistr);

        List<RelNode> inputs0 = Commons.transform(inputs,
            input -> RelOptRule.convert(input, traits));

        return copy(traits, inputs0);
    }

    /** {@inheritDoc} */
    @Override public List<RelNode> derive(List<List<RelTraitSet>> inputTraits) {
        RelOptCluster cluster = getCluster();

        Set<RelTraitSet> traits = inputTraits.stream()
            .flatMap(List::stream)
            .map(TraitUtils::distribution)
            .filter(d -> d != any())
            .map(distr -> cluster.traitSetOf(IgniteConvention.INSTANCE).replace(distr))
            .collect(Collectors.toSet());

        List<RelNode> res = new ArrayList<>(traits.size());
        for (RelTraitSet traits0 : traits) {
            List<RelNode> inputs0 = Commons.transform(inputs,
                input -> RelOptRule.convert(input, traits0));
            res.add(copy(traits0, inputs0));
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public DeriveMode getDeriveMode() {
        return DeriveMode.OMAKASE;
    }
}
