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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.util.ImmutableIntList;

/**
 *
 */
public final class DistributionTrait implements RelTrait, Serializable {
    private DistributionType type;
    private int[] keys;
    private DestinationFunctionFactory functionFactory;

    public DistributionTrait() {
    }

    public DistributionTrait(DistributionType type, int[] keys, DestinationFunctionFactory functionFactory) {
        this.type = type;
        this.keys = keys;
        this.functionFactory = functionFactory;
    }

    public DistributionType type() {
        return type;
    }

    public DestinationFunctionFactory destinationFunctionFactory() {
        return functionFactory;
    }

    public ImmutableIntList keys() {
        return ImmutableIntList.of(keys);
    }

    @Override public void register(RelOptPlanner planner) {}

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o instanceof DistributionTrait) {
            DistributionTrait that = (DistributionTrait) o;

            return type == that.type() && Arrays.equals(keys, that.keys);
        }

        return false;
    }

    @Override public int hashCode() {
        return Objects.hash(type, Arrays.hashCode(keys));
    }

    @Override public String toString() {
        return type + (type == DistributionType.HASH ? Arrays.toString(keys) : "");
    }

    @Override public RelTraitDef getTraitDef() {
        return DistributionTraitDef.INSTANCE;
    }

    @Override public boolean satisfies(RelTrait trait) {
        if (trait == this)
            return true;

        if (!(trait instanceof DistributionTrait))
            return false;

        DistributionTrait other = (DistributionTrait) trait;

        if (other.type() == DistributionType.ANY)
            return true;

        if (type() == other.type())
            return type() != DistributionType.HASH
                || (Arrays.equals(keys, other.keys)
                    && Objects.equals(functionFactory, other.functionFactory));

        return other.type() == DistributionType.RANDOM && type() == DistributionType.HASH;
    }
}
