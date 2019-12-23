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
package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;

/**
 * TODO: Add class description.
 */
public class SortConverter extends IgniteConverter {

    public static final RelOptRule INSTANCE = new SortConverter();

    private SortConverter() {
        super(LogicalSort.class, "SortConverter");
    }

    @Override protected List<RelNode> convert0(RelNode rel) {
        LogicalSort sort = (LogicalSort) rel;

        RelNode input = sort.getInput();

        Map<RelNode, RelTraitSet> allChildrenTraits = new HashMap<>();

        VolcanoUtils.deriveAllPossibleTraits(input, allChildrenTraits);

        List<RelNode> converted = new ArrayList<>(allChildrenTraits.size());

        RelTraitSet newTraitSet = rel.getTraitSet().replace(IgniteConvention.INSTANCE);

        for (RelTraitSet childTraitSet : new HashSet<>(allChildrenTraits.values())) {
            childTraitSet = childTraitSet.replace(IgniteConvention.INSTANCE);

            if (!newTraitSet.satisfies(childTraitSet)) {
                RelNode convertedNode = new IgniteSort(sort.getCluster(), newTraitSet, convert(input, childTraitSet), sort.getCollation());

                converted.add(convertedNode);
            }

        }

        return converted;
    }
}
