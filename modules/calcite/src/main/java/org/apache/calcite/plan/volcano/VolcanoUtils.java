/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.plan.volcano;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;

/**
 *
 */
public class VolcanoUtils {
    public static RelSubset subset(RelSubset subset, RelTraitSet traits) {
        return subset.set.getOrCreateSubset(subset.getCluster(), traits.simplify()); // TODO getSet?
    }

    public static VolcanoPlanner impatient(VolcanoPlanner planner) {
       // planner.impatient = true;

        return planner;
    }

    public static void deriveAllPossibleTraits(RelNode node, Map<RelNode, RelTraitSet> traitsAcc) {
        if (!(node instanceof RelSubset))
            traitsAcc.put(node, node.getTraitSet().replace(IgniteConvention.INSTANCE));

        RelSubset subSet = (RelSubset)node;
        RelSet set = subSet.set;


        for (RelSubset subset : set.subsets)
            traitsAcc.put(subset, subset.getTraitSet().replace(IgniteConvention.INSTANCE));

        for (RelNode peerNode : set.rels) {
            for (RelNode input : peerNode.getInputs()) {
                if (!traitsAcc.containsKey(input))
                    deriveAllPossibleTraits(input, traitsAcc);
            }
        }

    }

    public static boolean isPhysicalNode(RelNode node) {
        return node.getTraitSet().contains(IgniteConvention.INSTANCE);
    }

    public static void ensureRootConverters(RelOptPlanner planner) {
        if (!(planner instanceof VolcanoPlanner))
            return;

        VolcanoPlanner volcanoPlanner = (VolcanoPlanner)planner;
        RelSubset root = (RelSubset)planner.getRoot();

        final Set<RelSubset> subsets = new HashSet<>();
        for (RelNode rel : root.getRels()) {
            if (rel instanceof AbstractConverter) {
                subsets.add((RelSubset) ((AbstractConverter) rel).getInput());
            }
        }
        for (RelSubset subset : root.set.subsets) {
            final ImmutableList<RelTrait> difference =
                root.getTraitSet().difference(subset.getTraitSet());

            List<RelTrait> diffSet = new ArrayList<>(difference.size());

            for (RelTrait trait : difference) {
                RelTraitDef def = trait.getTraitDef();
                RelTrait rootTrait = root.getTraitSet().getTrait(def);

                if (!trait.satisfies(rootTrait))
                    diffSet.add(trait);
            }

            if (diffSet.size() == 1 && subsets.add(subset)) {
                volcanoPlanner.register(
                    new AbstractConverter(subset.getCluster(), subset,
                        diffSet.get(0).getTraitDef(), root.getTraitSet()),
                    root);
            }
        }

        ((VolcanoPlanner)planner).ensureRootConverters();
    }
}
