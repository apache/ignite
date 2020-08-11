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

import java.util.Objects;

import com.google.common.collect.Ordering;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;

import static org.apache.calcite.rel.RelDistribution.Type.ANY;
import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.RANDOM_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.SINGLETON;

/**
 * Description of the physical distribution of a relational expression.
 */
public final class DistributionTrait implements IgniteDistribution {
    /** */
    private static final Ordering<Iterable<Integer>> ORDERING =
        Ordering.<Integer>natural().lexicographical();

    /** */
    private final DistributionFunction function;

    /** */
    private final ImmutableIntList keys;

    /**
     * @param function Distribution function.
     */
    DistributionTrait(DistributionFunction function) {
        assert function.type() != HASH_DISTRIBUTED;

        this.function = function;

        keys = ImmutableIntList.of();
    }

    /**
     * @param keys Distribution keys.
     * @param function Distribution function.
     */
    DistributionTrait(ImmutableIntList keys, DistributionFunction function) {
        this.keys = keys;
        this.function = function;
    }

    /** {@inheritDoc} */
    @Override public RelDistribution.Type getType() {
        return function.type();
    }

    /** {@inheritDoc} */
    @Override public DistributionFunction function() {
        return function;
    }

    /** {@inheritDoc} */
    @Override public ImmutableIntList getKeys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public void register(RelOptPlanner planner) {}

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o instanceof DistributionTrait) {
            DistributionTrait that = (DistributionTrait) o;

            return Objects.equals(function, that.function) && Objects.equals(keys, that.keys);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(function, keys);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return function.name() + (function.type() == HASH_DISTRIBUTED ? keys : "");
    }

    /** {@inheritDoc} */
    @Override public DistributionTraitDef getTraitDef() {
        return DistributionTraitDef.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public boolean satisfies(RelTrait trait) {
        if (trait == this)
            return true;

        if (!(trait instanceof DistributionTrait))
            return false;

        DistributionTrait other = (DistributionTrait) trait;

        if (other.getType() == ANY)
            return true;

        if (getType() == other.getType())
            return getType() != HASH_DISTRIBUTED
                || (Objects.equals(keys, other.keys)
                    && Objects.equals(function, other.function));

        if (other.getType() == RANDOM_DISTRIBUTED)
            return getType() == HASH_DISTRIBUTED;

        return other.getType() == SINGLETON && getType() == BROADCAST_DISTRIBUTED;
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution apply(Mappings.TargetMapping mapping) {
        if (getType() != HASH_DISTRIBUTED)
            return this;

        if (mapping.getTargetCount() < keys.size())
            IgniteDistributions.random();

        int[] map = new int[mapping.getSourceCount()];
        int[] res = new int[keys.size()];

        for (int i = 0; i < keys.size(); i++)
            map[keys.getInt(i)] = i + 1;

        for (int i = 0, found = 0; i < mapping.getTargetCount(); i++) {
            int source = mapping.getSourceOpt(i);

            if (source == -1)
                continue;

            int keyPos = map[source] - 1;

            if (keyPos == -1)
                continue;

            res[keyPos] = i;

            if (++found == keys.size())
                return IgniteDistributions.hash(ImmutableIntList.of(res), function);

            map[source] = 0;
        }

        return IgniteDistributions.random();
    }

    /** {@inheritDoc} */
    @Override public boolean isTop() {
        return getType() == Type.ANY;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(RelMultipleTrait o) {
        final IgniteDistribution distribution = (IgniteDistribution) o;

        if (getType() == distribution.getType() && getType() == Type.HASH_DISTRIBUTED) {
            int cmp = ORDERING.compare(getKeys(), distribution.getKeys());

            if (cmp == 0)
                cmp = function.name().compareTo(distribution.function().name());

            return cmp;
        }

        return getType().compareTo(distribution.getType());
    }
}
