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

package org.apache.ignite.internal.processors.query.calcite.trait;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.fixTraits;
import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.inputTraits;

/** */
public class TraitsPropagationContext {
    /** */
    private final Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations;

    /**
     * Creates a context for up-to-bottom traits propagation.
     *
     * @param node Target node.
     * @param outTrait Desired relational node output trait.
     * @return Newly created context.
     */
    public static TraitsPropagationContext forPassingThrough(RelNode node, RelTraitSet outTrait) {
        ImmutableSet<Pair<RelTraitSet, List<RelTraitSet>>> variants = ImmutableSet.of(
            Pair.of(fixTraits(outTrait),
                Commons.transform(node.getInputs(), i -> fixTraits(i.getCluster().traitSet()))));

        return new TraitsPropagationContext(variants);

    }

    /**
     * Creates a context for bottom-up traits propagation.
     *
     * @param node Target node.
     * @param inTraits Input traits.
     * @return Newly created context.
     */
    public static TraitsPropagationContext forDerivation(RelNode node, List<List<RelTraitSet>> inTraits) {
        ImmutableSet.Builder<Pair<RelTraitSet, List<RelTraitSet>>> b = ImmutableSet.builder();

        RelTraitSet out = fixTraits(node.getCluster().traitSet());

        for (List<RelTraitSet> srcTraits : inputTraits(inTraits))
            b.add(Pair.of(out, Commons.transform(srcTraits, TraitUtils::fixTraits)));

        return new TraitsPropagationContext(b.build());
    }

    /** */
    private TraitsPropagationContext(Set<Pair<RelTraitSet, List<RelTraitSet>>> combinations) {
        this.combinations = combinations;
    }

    /**
     * Propagates traits in bottom-up or up-to-bottom manner using given traits propagator.
     */
    public TraitsPropagationContext propagate(TraitsPropagator processor) {
        ImmutableSet.Builder<Pair<RelTraitSet, List<RelTraitSet>>> b = ImmutableSet.builder();

        for (Pair<RelTraitSet, List<RelTraitSet>> variant : combinations)
            b.addAll(processor.propagate(variant.left, variant.right));

        return new TraitsPropagationContext(b.build());
    }

    /**
     * Creates nodes using given factory.
     */
    public List<RelNode> nodes(RelFactory nodesCreator) {
        ImmutableList.Builder<RelNode> b = ImmutableList.builder();

        for (Pair<RelTraitSet, List<RelTraitSet>> variant : combinations)
            b.add(nodesCreator.create(variant.left, variant.right));

        return b.build();
    }
}
