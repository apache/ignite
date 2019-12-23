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

import java.util.Map;
import org.apache.calcite.plan.RelOptPlanner;
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
        planner.impatient = true;

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
        if (planner instanceof VolcanoPlanner)
            ((VolcanoPlanner)planner).ensureRootConverters();
    }
}
