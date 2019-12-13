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

import com.google.common.collect.Ordering;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;

import static org.apache.calcite.rel.RelDistribution.Type.ANY;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.RANDOM_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.RANGE_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.ROUND_ROBIN_DISTRIBUTED;

/**
 *
 */
public final class DistributionTrait implements IgniteDistribution, Serializable {
    private static final Ordering<Iterable<Integer>> ORDERING =
        Ordering.<Integer>natural().lexicographical();

    private RelDistribution.Type type;
    private ImmutableIntList keys;
    private DestinationFunctionFactory functionFactory;

    public DistributionTrait() {
    }

    public DistributionTrait(RelDistribution.Type type, ImmutableIntList keys, DestinationFunctionFactory functionFactory) {
        if (type == RANGE_DISTRIBUTED || type == ROUND_ROBIN_DISTRIBUTED)
            throw new IllegalArgumentException("Distribution type " + type + " is unsupported.");

        this.type = type;
        this.keys = keys;
        this.functionFactory = functionFactory;
    }

    @Override public RelDistribution.Type getType() {
        return type;
    }

    @Override public DestinationFunctionFactory destinationFunctionFactory() {
        return functionFactory;
    }

    @Override public ImmutableIntList getKeys() {
        return keys;
    }

    @Override public void register(RelOptPlanner planner) {}

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o instanceof DistributionTrait) {
            DistributionTrait that = (DistributionTrait) o;

            return type == that.type
                && Objects.equals(keys, that.keys)
                && Objects.equals(functionFactory.key(), that.functionFactory.key());
        }

        return false;
    }

    @Override public int hashCode() {
        return Objects.hash(type, keys);
    }

    @Override public String toString() {
        return type + (type == Type.HASH_DISTRIBUTED ? "[" + functionFactory.key() + "]" + keys : "");
    }

    @Override public DistributionTraitDef getTraitDef() {
        return DistributionTraitDef.INSTANCE;
    }

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
                    && Objects.equals(functionFactory, other.functionFactory));

        return other.getType() == RANDOM_DISTRIBUTED && getType() == HASH_DISTRIBUTED;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeObject(type);
        out.writeObject(keys.toIntArray());
        out.writeObject(functionFactory);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        type = (Type) in.readObject();
        keys = ImmutableIntList.of((int[])in.readObject());
        functionFactory = (DestinationFunctionFactory) in.readObject();
    }

    private Object readResolve() throws ObjectStreamException {
        return DistributionTraitDef.INSTANCE.canonize(this);
    }

    @Override public IgniteDistribution apply(Mappings.TargetMapping mapping) {
        if (keys.isEmpty())
            return this;

        assert type == HASH_DISTRIBUTED;

        List<Integer> newKeys = IgniteDistributions.projectDistributionKeys(mapping, keys);

        return newKeys.size() == keys.size() ? IgniteDistributions.hash(newKeys, functionFactory) :
            IgniteDistributions.random();
    }

    @Override public boolean isTop() {
        return type == Type.ANY;
    }

    @Override public int compareTo(RelMultipleTrait o) {
        // TODO is this method really needed??

        final IgniteDistribution distribution = (IgniteDistribution) o;

        if (type == distribution.getType()
            && (type == Type.HASH_DISTRIBUTED
            || type == Type.RANGE_DISTRIBUTED)) {
            int cmp = ORDERING.compare(getKeys(), distribution.getKeys());

            if (cmp == 0)
                cmp = Integer.compare(functionFactory.key().hashCode(), distribution.destinationFunctionFactory().key().hashCode());

            return cmp;
        }

        return type.compareTo(distribution.getType());
    }
}
