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

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;

import static org.apache.ignite.internal.processors.query.calcite.trait.DistributionType.HASH;

/**
 *
 */
public class IgniteDistributions {
    private static final DistributionTrait BROADCAST = new DistributionTrait(DistributionType.BROADCAST, ImmutableIntList.of(), AllTargetsFactory.INSTANCE);
    private static final DistributionTrait SINGLE = new DistributionTrait(DistributionType.SINGLE, ImmutableIntList.of(), SingleTargetFactory.INSTANCE);
    private static final DistributionTrait RANDOM = new DistributionTrait(DistributionType.RANDOM, ImmutableIntList.of(), RandomTargetFactory.INSTANCE);
    private static final DistributionTrait ANY    = new DistributionTrait(DistributionType.ANY, ImmutableIntList.of(), NoOpFactory.INSTANCE);

    public static DistributionTrait any() {
        return ANY;
    }

    public static DistributionTrait random() {
        return RANDOM;
    }

    public static DistributionTrait single() {
        return SINGLE;
    }

    public static DistributionTrait broadcast() {
        return BROADCAST;
    }

    public static DistributionTrait hash(List<Integer> keys) {
        return new DistributionTrait(HASH, ImmutableIntList.copyOf(keys), HashFunctionFactory.INSTANCE);
    }

    public static DistributionTrait hash(List<Integer> keys, DestinationFunctionFactory factory) {
        return new DistributionTrait(HASH, ImmutableIntList.copyOf(keys), factory);
    }

    public static List<DistributionTrait> deriveDistributions(RelNode rel, RelMetadataQuery mq) {
        if (!(rel instanceof RelSubset)) {
            DistributionTrait dist = IgniteMdDistribution.distribution(rel, mq);

            return dist.type() == DistributionType.ANY ? Collections.emptyList() : Collections.singletonList(dist);
        }

        HashSet<DistributionTrait> res = new HashSet<>();

        for (RelNode relNode : ((RelSubset) rel).getRels())
            res.addAll(deriveDistributions(relNode, mq));

        return res.isEmpty() ? Collections.emptyList() : new ArrayList<>(res);
    }
}
