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
    private static final DistributionFunctionFactory NO_OP_FACTORY = (t,k) -> null;

    private static final DistributionTrait BROADCAST = new DistributionTraitImpl(DistributionTrait.DistributionType.BROADCAST, ImmutableIntList.of(), allTargetsFunction());
    private static final DistributionTrait SINGLE = new DistributionTraitImpl(DistributionTrait.DistributionType.SINGLE, ImmutableIntList.of(), singleTargetFunction());
    private static final DistributionTrait RANDOM = new DistributionTraitImpl(DistributionTrait.DistributionType.RANDOM, ImmutableIntList.of(), randomTargetFunction());
    private static final DistributionTrait ANY    = new DistributionTraitImpl(DistributionTrait.DistributionType.ANY, ImmutableIntList.of(), noOpFunction());

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

    public static DistributionTrait hash(List<Integer> keys, DistributionFunctionFactory factory) {
        return new DistributionTraitImpl(HASH, ImmutableIntList.copyOf(keys), factory);
    }

    public static DistributionFunctionFactory noOpFunction() {
        return NO_OP_FACTORY;
    }

    public static DistributionFunctionFactory singleTargetFunction() {
        return noOpFunction(); // TODO
    }

    public static DistributionFunctionFactory allTargetsFunction() {
        return noOpFunction(); // TODO
    }

    public static DistributionFunctionFactory randomTargetFunction() {
        return noOpFunction(); // TODO
    }
}
