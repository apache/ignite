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

import java.util.List;
import org.apache.calcite.util.ImmutableIntList;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution.DistributionType.HASH;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution.DistributionType.RANDOM;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution.DistributionType.SINGLE;

/**
 *
 */
public class IgniteDistributions {
    /** */
    private static final IgniteDistributionTraitDef traitDef = IgniteDistributionTraitDef.INSTANCE;

    public static IgniteDistribution random(List<Integer> sources) {
        return traitDef.canonize(new IgniteDistributionImpl(RANDOM, ImmutableIntList.of(), ImmutableIntList.copyOf(sources)));
    }

    public static IgniteDistribution hash(List<Integer> keys, List<Integer> sources) {
        return traitDef.canonize(new IgniteDistributionImpl(HASH, ImmutableIntList.copyOf(keys), ImmutableIntList.copyOf(sources)));
    }

    public static IgniteDistribution single(List<Integer> sources) {
        return traitDef.canonize(new IgniteDistributionImpl(SINGLE, ImmutableIntList.of(), ImmutableIntList.copyOf(sources)));
    }
}
