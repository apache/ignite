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

import static org.apache.ignite.internal.processors.query.calcite.trait.DistributionTrait.DistributionType.HASH;

/**
 *
 */
public class IgniteDistributions {
    /** */
    private static final DistributionTraitDef traitDef = DistributionTraitDef.INSTANCE;
    private static final DistributionTrait SINGLE = traitDef.canonize(new DistributionTraitImpl(DistributionTrait.DistributionType.SINGLE, ImmutableIntList.of()));
    private static final DistributionTrait RANDOM = traitDef.canonize(new DistributionTraitImpl(DistributionTrait.DistributionType.RANDOM, ImmutableIntList.of()));
    private static final DistributionTrait ANY    = traitDef.canonize(new DistributionTraitImpl(DistributionTrait.DistributionType.ANY, ImmutableIntList.of()));

    public static DistributionTrait any() {
        return ANY;
    }

    public static DistributionTrait random() {
        return RANDOM;
    }

    public static DistributionTrait single() {
        return SINGLE;
    }

    public static DistributionTrait hash(List<Integer> keys) {
        return traitDef.canonize(new DistributionTraitImpl(HASH, ImmutableIntList.copyOf(keys)));
    }
}
