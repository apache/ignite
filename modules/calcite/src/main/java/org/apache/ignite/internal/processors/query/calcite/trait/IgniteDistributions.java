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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToIntFunction;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.cluster.ClusterNode;

import static org.apache.ignite.internal.processors.query.calcite.trait.DistributionType.HASH;

/**
 *
 */
public class IgniteDistributions {
    private static final DestinationFunctionFactory NO_OP_FACTORY = (t,k) -> null;
    private static final DestinationFunctionFactory HASH_FACTORY = (t,k) -> {
        int[] fields = k.toIntArray();

        ToIntFunction<Object> hashFun = r -> {
            Object[] row = (Object[]) r;

            if (row == null)
                return 0;

            int hash = 1;

            for (int i : fields)
                hash = 31 * hash + (row[i] == null ? 0 : row[i].hashCode());

            return hash;
        };

        return r -> t.location.nodes(hashFun.applyAsInt(r));
    };


    private static final DistributionTrait BROADCAST = new DistributionTrait(DistributionType.BROADCAST, ImmutableIntList.of(), allTargetsFunction());
    private static final DistributionTrait SINGLE = new DistributionTrait(DistributionType.SINGLE, ImmutableIntList.of(), singleTargetFunction());
    private static final DistributionTrait RANDOM = new DistributionTrait(DistributionType.RANDOM, ImmutableIntList.of(), randomTargetFunction());
    private static final DistributionTrait ANY    = new DistributionTrait(DistributionType.ANY, ImmutableIntList.of(), noOpFunction());

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

    public static DistributionTrait hash(List<Integer> keys, DestinationFunctionFactory factory) {
        return new DistributionTrait(HASH, ImmutableIntList.copyOf(keys), factory);
    }

    public static DestinationFunctionFactory noOpFunction() {
        return NO_OP_FACTORY;
    }

    public static DestinationFunctionFactory singleTargetFunction() {
        return (t, k) -> {
            List<ClusterNode> nodes = t.location.nodes();

            return r -> nodes;
        };
    }

    public static DestinationFunctionFactory allTargetsFunction() {
        return (t, k) -> {
            List<ClusterNode> nodes = t.location.nodes();

            return r -> nodes;
        };
    }

    public static DestinationFunctionFactory randomTargetFunction() {
        return (t, k) -> {
            List<ClusterNode> nodes = t.location.nodes();

            return r -> Collections.singletonList(nodes.get(ThreadLocalRandom.current().nextInt(nodes.size())));
        };
    }

    public static DestinationFunctionFactory hashFunction() {
        return HASH_FACTORY;
    }
}
