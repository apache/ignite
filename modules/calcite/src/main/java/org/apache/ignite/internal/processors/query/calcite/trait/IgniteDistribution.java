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

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

/**
 *
 */
public interface IgniteDistribution extends RelTrait {
    public enum DistributionType {
        HASH("hash"),
        RANDOM("random"),
        BROADCAST("broadcast"),
        SINGLE("single"),
        ANY("any");

        /** */
        private final String description;

        /** */
        DistributionType(String description) {
            this.description = description;
        }

        /** */
        @Override public String toString() {
            return description;
        }
    }

    IgniteDistribution ANY = new IgniteDistributionImpl(DistributionType.ANY, ImmutableIntList.of());

    DistributionType type();

    @Override default RelTraitDef getTraitDef() {
        return IgniteDistributionTraitDef.INSTANCE;
    }

    @Override default boolean satisfies(RelTrait trait) {
        if (trait == this)
            return true;

        if (!(trait instanceof IgniteDistribution))
            return false;

        IgniteDistribution other = (IgniteDistribution) trait;

        if (!Util.startsWith(other.sources(), sources()))
            return false;

        if (other.type() == DistributionType.ANY)
            return true;

        if (type() == other.type())
            return type() != DistributionType.HASH || keys().equals(other.keys());

        return other.type() == DistributionType.RANDOM && type() == DistributionType.HASH;
    }

    /**
     * @return Hash distribution columns ordinals or empty list otherwise.
     */
    ImmutableIntList keys();

    /**
     * @return Sources (indexes of sorted by node order nodes list for particular topology).
     */
    ImmutableIntList sources();
}
